use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

use alloc_counter::AllocationGuard;
use analytics_contract::{
    AnalyticsManifest, RetentionTimestamp, StorageItem, StorageStreamRecord, StorageValue,
    TenantSelector,
};
use analytics_engine::TenantResolver;
use axum::{Json, Router, routing::post};
use bg_jobs::{
    JobStore,
    store::{CommitOperationId, CommitOutcome},
    test_support::FakeStore,
};

use crate::retention::schedule::retention_schedule_transaction;

#[tokio::test]
#[ignore = "separate-process retained configuration measurement; requires an explicit probe mode"]
async fn given_many_wide_policies_when_runtime_outlives_config_then_lookups_preserve_requests_without_duplicate_templates()
 {
    use crate::constants::{
        RETENTION_CONFIG_PROBE_MODE, RETENTION_CONFIG_PROBE_TABLES,
        RETENTION_CONFIG_PROBE_VALUE_BYTES, RETENTION_CONFIG_PROBE_VALUES,
    };
    let mode = std::env::var(RETENTION_CONFIG_PROBE_MODE).expect("explicit retention probe mode");
    assert!(
        matches!(mode.as_str(), "copied" | "narrow"),
        "unsupported retention probe mode"
    );
    let server = RetentionPolicyServer::serve_responses(vec![
        serde_json::json!({"Items": [{"period_ms": {"N": "100"}}]});
        RETENTION_CONFIG_PROBE_TABLES
    ])
    .await;
    let config = wide_retention_config(&server.endpoint);
    let mut manifest = manifest(TenantSelector::TableName);
    manifest.tables = (0..RETENTION_CONFIG_PROBE_TABLES)
        .map(|index| {
            let mut table = manifest.tables[0].clone();
            table.analytics_table_name = format!("table_{index}");
            table
        })
        .collect();
    let record = parse_retention_record();
    let guard = AllocationGuard::start(
        module_path!(),
        "retention-config-startup",
        file!(),
        line!(),
        None,
    );
    let runtime = if mode == "copied" {
        RetentionConfigProbeRuntime::Copied(
            CopiedRetentionConfigRuntime::from_config(&config).await,
        )
    } else {
        RetentionConfigProbeRuntime::Narrow(
            crate::retention::RetentionRuntime::from_config(&config)
                .await
                .unwrap()
                .unwrap(),
        )
    };
    let report = guard.finish();
    drop(config);
    let resident_bytes = retention_probe_resident_bytes();
    let redundant_template_bytes = match &runtime {
        RetentionConfigProbeRuntime::Copied(runtime) => {
            assert_eq!(runtime.config.sweep_interval_ms, 60000);
            assert_eq!(runtime.config.delete_batch_size, 500);
            assert_eq!(runtime.tables.len(), RETENTION_CONFIG_PROBE_TABLES);
            std::hint::black_box(&runtime.cache);
            let bytes = runtime.redundant_template_bytes();
            assert!(
                bytes
                    >= 2 * RETENTION_CONFIG_PROBE_TABLES
                        * RETENTION_CONFIG_PROBE_VALUES
                        * RETENTION_CONFIG_PROBE_VALUE_BYTES
            );
            bytes
        }
        RetentionConfigProbeRuntime::Narrow(runtime) => {
            assert_eq!(runtime.sweep_interval_ms(), 60000);
            assert_eq!(runtime.delete_batch_size(), 500);
            assert_eq!(runtime.table_names().count(), RETENTION_CONFIG_PROBE_TABLES);
            0
        }
    };
    println!(
        "retention-config | mode={mode} | tables={RETENTION_CONFIG_PROBE_TABLES} | \
         values={RETENTION_CONFIG_PROBE_VALUES} | \
         value_bytes={RETENTION_CONFIG_PROBE_VALUE_BYTES} | startup_allocations={} | \
         startup_allocated_bytes={} | post_config_drop_resident_bytes={resident_bytes} | \
         redundant_template_string_capacity={redundant_template_bytes}",
        report.allocation_count, report.allocated_bytes
    );
    for table in &manifest.tables {
        match &runtime {
            RetentionConfigProbeRuntime::Copied(runtime) => {
                let owner = runtime.tables.get(&table.analytics_table_name).unwrap();
                assert_eq!(
                    owner.config.analytics_table_name,
                    table.analytics_table_name
                );
                std::hint::black_box(&owner.policy_setup);
                assert_eq!(
                    owner.lookup.lookup_period_ms("tenant-one").await.unwrap(),
                    100
                );
            }
            RetentionConfigProbeRuntime::Narrow(runtime) => {
                assert_eq!(
                    runtime
                        .retention_for_record(
                            &manifest,
                            &table.analytics_table_name,
                            "tenant-one",
                            &record
                        )
                        .await,
                    Some(analytics_engine::IngestRetention {
                        period_ms: Some(100),
                        timestamp: RetentionTimestamp::IngestedAt,
                        missing_retention: false
                    })
                );
            }
        }
    }
    let expected = wide_retention_wire_request();
    {
        let requests = server.requests.lock().unwrap();
        assert_eq!(requests.len(), RETENTION_CONFIG_PROBE_TABLES);
        for request in requests.iter() {
            assert_eq!(request, &expected);
        }
    }
    std::hint::black_box(&runtime);
    server.stop_server().await;
}

// Test-only reconstruction of former heap ownership, using the actual current
// client. The narrow lookup already owns request/selector/client: copy only the
// discarded setup fields here, otherwise this baseline would retain four
// templates instead of three.
enum RetentionConfigProbeRuntime {
    Copied(CopiedRetentionConfigRuntime),
    Narrow(crate::retention::RetentionRuntime),
}

struct CopiedRetentionConfigRuntime {
    config: config::AnalyticsRetentionConfig,
    tables: std::collections::HashMap<String, CopiedRetentionConfigTable>,
    cache: tokio::sync::RwLock<std::collections::HashMap<(String, String), (u64, u128)>>,
}

struct CopiedRetentionConfigTable {
    config: config::AnalyticsRetentionTableConfig,
    lookup: analytics_storage::RetentionPolicyLookup,
    policy_setup: (
        config::TenantRetentionPolicySource,
        Option<String>,
        Option<String>,
        Option<config::RemoteCredentialsConfig>,
        u64,
    ),
}

impl CopiedRetentionConfigRuntime {
    async fn from_config(config: &config::AnalyticsRetentionConfig) -> Self {
        let mut tables = std::collections::HashMap::new();
        for table in &config.tables {
            let policy = &table.tenant_policy;
            let lookup = analytics_storage::RetentionPolicyLookup::from_config(policy)
                .await
                .unwrap();
            tables.insert(
                table.analytics_table_name.clone(),
                CopiedRetentionConfigTable {
                    config: table.clone(),
                    lookup,
                    policy_setup: (
                        policy.source,
                        policy.endpoint_url.clone(),
                        policy.region.clone(),
                        policy.credentials.clone(),
                        policy.cache_ttl_ms,
                    ),
                },
            );
        }
        Self {
            config: config.clone(),
            tables,
            cache: tokio::sync::RwLock::new(std::collections::HashMap::new()),
        }
    }

    fn redundant_template_bytes(&self) -> usize {
        self.config
            .tables
            .iter()
            .chain(self.tables.values().map(|table| &table.config))
            .map(|table| {
                let config::TenantRetentionPolicyRequest::QueryTable(request) =
                    &table.tenant_policy.request
                else {
                    panic!("query template required")
                };
                request
                    .expression_attribute_values
                    .iter()
                    .map(|(_, value)| {
                        let storage_types::AttributeValue::S(value) = value else {
                            panic!("string template required")
                        };
                        value.capacity()
                    })
                    .sum::<usize>()
            })
            .sum()
    }
}

fn wide_retention_config(endpoint: &str) -> config::AnalyticsRetentionConfig {
    let wire = wide_retention_wire_request();
    let mut values = wire["ExpressionAttributeValues"].clone();
    values[":tenant"] = serde_json::json!({"S": "${tenant_id}"});
    let policy = serde_json::from_value::<config::TenantRetentionPolicyConfig>(serde_json::json!({
        "source": "aux_storage", "endpoint_url": endpoint,
        "request": {"query_table": {"table_name": "policies", "key_condition_expression": "#tenant = :tenant",
            "filter_expression": wire["FilterExpression"], "expression_attribute_names": {"#tenant": "tenant", "#body": "body"},
            "expression_attribute_values": values, "limit": 1}},
        "duration_selector": {"attribute_path": "period_ms"}
    })).unwrap();
    config::AnalyticsRetentionConfig {
        enabled: true,
        tables: (0..crate::constants::RETENTION_CONFIG_PROBE_TABLES)
            .map(|index| config::AnalyticsRetentionTableConfig {
                analytics_table_name: format!("table_{index}"),
                default_period_ms: 900,
                strict: true,
                timestamp: config::RetentionTimestampConfig::IngestedAt,
                tenant_policy: policy.clone(),
            })
            .collect(),
        ..Default::default()
    }
}

fn wide_retention_wire_request() -> serde_json::Value {
    let mut values = serde_json::Map::new();
    values.insert(":tenant".to_owned(), serde_json::json!({"S": "tenant-one"}));
    let mut filters = Vec::new();
    for index in 0..crate::constants::RETENTION_CONFIG_PROBE_VALUES {
        values.insert(format!(":value{index}"), serde_json::json!({"S": "x".repeat(crate::constants::RETENTION_CONFIG_PROBE_VALUE_BYTES)}));
        filters.push(format!("contains(#body, :value{index})"));
    }
    serde_json::json!({"TableName": "policies", "KeyConditionExpression": "#tenant = :tenant",
        "FilterExpression": filters.join(" AND "), "ExpressionAttributeNames": {"#tenant": "tenant", "#body": "body"},
        "ExpressionAttributeValues": values, "Limit": 1})
}

fn retention_probe_resident_bytes() -> u64 {
    let output = std::process::Command::new("/bin/ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .unwrap();
    assert!(output.status.success());
    std::str::from_utf8(&output.stdout)
        .unwrap()
        .trim()
        .parse::<u64>()
        .unwrap()
        * 1024
}

#[tokio::test]
#[ignore = "equal-work allocation probe for the public cached retention path"]
async fn given_cached_tenant_policy_when_retention_is_resolved_repeatedly_then_results_and_request_count_are_preserved()
 {
    let server = RetentionPolicyServer::serve_responses(vec![
        serde_json::json!({"Item": {"period_ms": {"N": "100"}}}),
    ])
    .await;
    let config = parse_retention_config(
        "aux_storage",
        &server.endpoint,
        serde_json::json!([
            {"analytics_table_name": "users", "default_period_ms": 900, "strict": true,
             "timestamp": {"kind": "ingested_at"}, "tenant_policy": {"cache_ttl_ms": 600000}}
        ]),
    );
    let runtime = crate::retention::RetentionRuntime::from_config(&config)
        .await
        .unwrap()
        .unwrap();
    drop(config);
    let manifest = manifest(TenantSelector::TableName);
    let record = parse_retention_record();
    let expected = analytics_engine::IngestRetention {
        period_ms: Some(100),
        timestamp: RetentionTimestamp::IngestedAt,
        missing_retention: false,
    };
    assert_eq!(
        runtime
            .retention_for_record(&manifest, "users", "one", &record)
            .await,
        Some(expected.clone())
    );
    for sample in 0..3 {
        let started = std::time::Instant::now();
        let guard = AllocationGuard::start(
            module_path!(),
            "retention-cache-hit",
            file!(),
            line!(),
            None,
        );
        for _ in 0..crate::constants::RETENTION_CACHE_PROBE_REPETITIONS {
            let actual = runtime
                .retention_for_record(&manifest, "users", "one", &record)
                .await
                .unwrap();
            assert_eq!(actual, expected);
            std::hint::black_box(actual);
        }
        let elapsed = started.elapsed();
        let report = guard.finish();
        println!(
            "retention-cache-hit | sample={sample} | repetitions={} | allocations={} | \
             allocated_bytes={} | elapsed={elapsed:?}",
            crate::constants::RETENTION_CACHE_PROBE_REPETITIONS,
            report.allocation_count,
            report.allocated_bytes
        );
        alloc_counter::emit_report(&report).unwrap();
    }
    assert_eq!(
        *server.requests.lock().unwrap(),
        [serde_json::json!({"TableName": "policies", "Key": {"pk": {"S": "one"}}})]
    );
    server.stop_server().await;
}

#[tokio::test]
async fn given_two_tables_and_tenants_when_retention_cache_expires_then_each_provider_preserves_isolation_and_refreshes_policy()
 {
    for source in ["aux_storage", "dynamodb"] {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let _recorder_guard = metrics::set_default_local_recorder(&recorder);
        let server = RetentionPolicyServer::serve_responses(vec![
            serde_json::json!({"Item": {"period_ms": {"N": "100"}}}),
            serde_json::json!({"Item": {"period_ms": {"N": "200"}}}),
            serde_json::json!({"Item": {"period_ms": {"N": "400"}}}),
            serde_json::json!({"Item": {"period_ms": {"N": "300"}}}),
        ])
        .await;
        let config = parse_retention_config(
            source,
            &server.endpoint,
            serde_json::json!([
                {"analytics_table_name": "users", "default_period_ms": 900, "strict": true,
                 "timestamp": {"kind": "attribute", "attribute_path": "created_at_ms"},
                 "tenant_policy": {"cache_ttl_ms": 1000}},
                {"analytics_table_name": "orders", "default_period_ms": 700, "strict": false,
                 "timestamp": {"kind": "ingested_at"}, "tenant_policy": {"cache_ttl_ms": 1000}}
            ]),
        );
        let runtime = crate::retention::RetentionRuntime::from_config(&config)
            .await
            .unwrap()
            .unwrap();
        drop(config);
        let mut manifest = manifest(TenantSelector::TableName);
        let mut orders = manifest.tables[0].clone();
        orders.analytics_table_name = "orders".to_string();
        manifest.tables.push(orders);
        let record = parse_retention_record();
        let expected = analytics_engine::IngestRetention {
            period_ms: Some(100),
            timestamp: RetentionTimestamp::Attribute {
                attribute_path: "created_at_ms".to_string(),
            },
            missing_retention: false,
        };
        assert_eq!(
            runtime
                .retention_for_record(&manifest, "users", "one", &record)
                .await,
            Some(expected.clone())
        );
        assert_eq!(
            runtime
                .retention_for_record(&manifest, "users", "one", &record)
                .await,
            Some(expected)
        );
        let second = runtime
            .retention_for_record(&manifest, "users", "two", &record)
            .await
            .unwrap();
        assert_eq!(second.period_ms, Some(200));
        assert!(!second.missing_retention);
        assert_eq!(
            second.timestamp,
            RetentionTimestamp::Attribute {
                attribute_path: "created_at_ms".to_string()
            }
        );
        assert_eq!(
            runtime
                .retention_for_record(&manifest, "orders", "one", &record)
                .await,
            Some(analytics_engine::IngestRetention {
                period_ms: Some(400),
                timestamp: RetentionTimestamp::IngestedAt,
                missing_retention: false
            })
        );
        assert_eq!(server.requests.lock().unwrap().len(), 3);
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let refreshed = runtime
            .retention_for_record(&manifest, "users", "one", &record)
            .await
            .unwrap();
        assert_eq!(refreshed.period_ms, Some(300));
        assert_eq!(refreshed.timestamp, second.timestamp);
        assert!(!refreshed.missing_retention);
        assert_eq!(
            *server.requests.lock().unwrap(),
            [
                serde_json::json!({"TableName": "policies", "Key": {"pk": {"S": "one"}}}),
                serde_json::json!({"TableName": "policies", "Key": {"pk": {"S": "two"}}}),
                serde_json::json!({"TableName": "policies", "Key": {"pk": {"S": "one"}}}),
                serde_json::json!({"TableName": "policies", "Key": {"pk": {"S": "one"}}}),
            ]
        );
        let rendered = recorder.handle().render();
        for (metric, table, expected_count) in [
            ("analytics_retention_lookups_total", "users", "4"),
            ("analytics_retention_lookups_total", "orders", "1"),
            ("analytics_retention_lookup_latency_ms_count", "users", "4"),
            ("analytics_retention_lookup_latency_ms_count", "orders", "1"),
        ] {
            let sample = rendered
                .lines()
                .find(|line| {
                    line.starts_with(&format!("{metric}{{"))
                        && line.contains(&format!("table=\"{table}\""))
                        && line.contains(&format!("source=\"{source}\""))
                        && line.contains("outcome=\"success\"")
                })
                .unwrap_or_else(|| panic!("missing {metric}/{table}/{source}: {rendered}"));
            assert_eq!(sample.split_whitespace().last(), Some(expected_count));
        }
        server.stop_server().await;
    }
}

#[tokio::test]
async fn given_strict_and_fallback_tables_when_policy_is_invalid_or_missing_then_timestamp_and_fallback_are_preserved()
 {
    for source in ["aux_storage", "dynamodb"] {
        let server = RetentionPolicyServer::serve_responses(vec![
            serde_json::json!({"Item": {"period_ms": {"S": "invalid"}}}),
            serde_json::json!({}),
        ])
        .await;
        let config = parse_retention_config(
            source,
            &server.endpoint,
            serde_json::json!([
                {"analytics_table_name": "users", "default_period_ms": 900, "strict": true,
                 "timestamp": {"kind": "attribute", "attribute_path": "created_at_ms"}, "tenant_policy": {}},
                {"analytics_table_name": "orders", "default_period_ms": 700, "strict": false,
                 "timestamp": {"kind": "ingested_at"}, "tenant_policy": {}}
            ]),
        );
        let runtime = crate::retention::RetentionRuntime::from_config(&config)
            .await
            .unwrap()
            .unwrap();
        drop(config);
        let mut manifest = manifest(TenantSelector::TableName);
        let mut orders = manifest.tables[0].clone();
        orders.analytics_table_name = "orders".to_string();
        manifest.tables.push(orders);
        let record = parse_retention_record();
        assert_eq!(
            runtime
                .retention_for_record(&manifest, "users", "one", &record)
                .await,
            Some(analytics_engine::IngestRetention {
                period_ms: None,
                timestamp: RetentionTimestamp::Attribute {
                    attribute_path: "created_at_ms".to_string()
                },
                missing_retention: true
            })
        );
        assert_eq!(
            runtime
                .retention_for_record(&manifest, "orders", "one", &record)
                .await,
            Some(analytics_engine::IngestRetention {
                period_ms: Some(700),
                timestamp: RetentionTimestamp::IngestedAt,
                missing_retention: false
            })
        );
        assert_eq!(
            runtime
                .retention_for_record(&manifest, "unconfigured", "one", &record)
                .await,
            None
        );
        let mut deleted = record;
        deleted.new_image = None;
        assert_eq!(
            runtime
                .retention_for_record(&manifest, "users", "one", &deleted)
                .await,
            None
        );
        assert_eq!(server.requests.lock().unwrap().len(), 2);
        assert_eq!(runtime.delete_batch_size(), 500);
        assert_eq!(runtime.sweep_interval_ms(), 60000);
        server.stop_server().await;
    }
}

fn parse_retention_record() -> StorageStreamRecord {
    serde_json::from_value(serde_json::json!({"Keys": {"pk": {"S": "row-one"}}, "SequenceNumber": "1", "NewImage": {"created_at_ms": {"N": "1000"}}})).unwrap()
}

fn parse_retention_config(
    source: &str,
    endpoint: &str,
    mut tables: serde_json::Value,
) -> config::AnalyticsRetentionConfig {
    for table in tables.as_array_mut().unwrap() {
        let policy = table["tenant_policy"].as_object_mut().unwrap();
        let mut shared_policy = serde_json::json!({
            "source": source, "endpoint_url": endpoint, "region": "local-test-1",
            "credentials": {"static": {"access_key": "test-access", "secret_key": "test-secret"}},
            "request": {"get_item": {"table_name": "policies", "key": {"pk": {"S": "${tenant_id}"}}}},
            "duration_selector": {"attribute_path": "period_ms"}
        });
        policy.append(shared_policy.as_object_mut().unwrap());
    }
    serde_json::from_value(serde_json::json!({"enabled": true, "tables": tables})).unwrap()
}

struct RetentionPolicyServer {
    endpoint: String,
    requests: Arc<Mutex<Vec<serde_json::Value>>>,
    task: tokio::task::JoinHandle<()>,
}

impl RetentionPolicyServer {
    async fn serve_responses(responses: Vec<serde_json::Value>) -> Self {
        let responses = Arc::new(Mutex::new(VecDeque::from(responses)));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        let router = Router::new().route(
            "/",
            post(move |body: axum::body::Bytes| {
                let responses = Arc::clone(&responses);
                let captured = Arc::clone(&captured);
                async move {
                    let request = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
                    captured.lock().unwrap().push(request);
                    Json(
                        responses
                            .lock()
                            .unwrap()
                            .pop_front()
                            .expect("unexpected retention request"),
                    )
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            endpoint,
            requests,
            task,
        }
    }

    async fn stop_server(self) {
        self.task.abort();
        assert!(self.task.await.unwrap_err().is_cancelled());
    }
}

fn manifest(selector: TenantSelector) -> AnalyticsManifest {
    serde_json::from_value(serde_json::json!({
        "version": 1,
        "tables": [{
            "source_table_name": "source_users",
            "analytics_table_name": "users",
            "tenant_selector": selector,
            "row_identity": {"kind": "record_key"}
        }]
    }))
    .expect("valid retention manifest")
}

#[test]
fn given_partition_key_prefix_when_resolved_then_engine_tenant_mapping_is_used() {
    let item = StorageItem::from_iter([(
        "pk".to_string(),
        StorageValue::S("ns_acme#user-1".to_string()),
    )]);
    let tenant = TenantResolver::default()
        .resolve(
            &manifest(TenantSelector::PartitionKeyPrefix {
                attribute_name: "pk".to_string(),
            })
            .tables[0],
            "source_users",
            &item,
        )
        .expect("tenant resolution");
    assert_eq!(tenant.as_deref(), Some("t_acme"));
}

#[test]
fn given_attribute_regex_when_resolved_then_named_capture_is_used() {
    let item = StorageItem::from_iter([(
        "pk".to_string(),
        StorageValue::S("TENANT#acme#USER#1".to_string()),
    )]);
    let tenant = TenantResolver::default()
        .resolve(
            &manifest(TenantSelector::AttributeRegex {
                attribute_name: "pk".to_string(),
                regex: r"^TENANT#(?<tenant>[^#]+)#USER#(?<user>[^#]+)$".to_string(),
                capture: "tenant".to_string(),
            })
            .tables[0],
            "source_users",
            &item,
        )
        .expect("tenant resolution");
    assert_eq!(tenant.as_deref(), Some("acme"));
}

#[tokio::test]
async fn given_retention_schedule_transaction_when_replayed_then_schedule_is_committed_once() {
    let store = FakeStore::new();
    let capabilities = store.capabilities();
    let (key, transaction) = retention_schedule_transaction(capabilities, None, 10_000)
        .expect("retention schedule transaction");
    let operation_id = transaction.operation_id();

    assert_eq!(
        operation_id.as_bytes().len(),
        CommitOperationId::BYTE_LENGTH
    );
    assert_eq!(
        store.commit(&transaction).await.expect("first commit"),
        CommitOutcome::Committed
    );
    let first = store.get(key.clone()).await.expect("first schedule");
    assert!(first.is_some(), "first commit persists the schedule");
    let (_, competing) =
        retention_schedule_transaction(capabilities, None, 20_000).expect("fresh transaction");
    assert_eq!(
        store.commit(&competing).await.expect("competing commit"),
        CommitOutcome::Conflict
    );
    assert_eq!(
        store.commit(&transaction).await.expect("replay commit"),
        CommitOutcome::Committed
    );
    assert_eq!(store.get(key).await.expect("replayed schedule"), first);
}

#[test]
fn given_dynamic_source_table_when_tenant_resolved_then_actual_table_is_used() {
    let manifest = manifest(TenantSelector::TableName);
    let tenant = TenantResolver::default()
        .resolve(&manifest.tables[0], "tenant_actual", &StorageItem::new())
        .expect("tenant resolution");
    assert_eq!(tenant.as_deref(), Some("tenant_actual"));
}
