use std::collections::HashMap;

use analytics_contract::{AnalyticsManifest, StorageValue};
use config::{AnalyticsSourceConfig, AnalyticsSourceTableConfig, AnalyticsStreamType};

use crate::{PollerConfig, facade::storage_stream_record_from_facade, planning::table_plans};

#[test]
fn storage_facade_record_converts_to_contract_record() {
    let record = storage_types::StreamRecord {
        cursor: None,
        source_table_name: None,
        keys: HashMap::new(),
        sequence_number: "1".to_string(),
        old_image: None,
        new_image: Some(HashMap::from([(
            "profile".to_string(),
            storage_types::AttributeValue::M(HashMap::from([(
                "email".to_string(),
                storage_types::AttributeValue::S("a@example.com".to_string()),
            )])),
        )])),
    };

    let converted = storage_stream_record_from_facade(record);
    assert_eq!(
        converted
            .new_image
            .as_ref()
            .and_then(|item| item.get("profile")),
        Some(&StorageValue::M(HashMap::from([(
            "email".to_string(),
            StorageValue::S("a@example.com".to_string())
        )])))
    );
}

#[test]
fn poller_defaults_bound_each_ingest_batch_to_four_source_responses() {
    let config = PollerConfig::default();

    assert_eq!(config.poll_interval, std::time::Duration::from_millis(100));
    assert_eq!(
        config.request_timeout,
        std::time::Duration::from_millis(5_000)
    );
    assert_eq!(config.max_shards, 16);
    assert_eq!(config.max_responses_per_interval, 4);
    assert_eq!(config.max_records_per_response, 8_192);
    assert_eq!(config.response_budget_per_shard(), 1);
}

#[test]
fn poller_plan_spreads_response_budget_across_concurrent_shards() {
    let config = PollerConfig {
        poll_interval: std::time::Duration::from_millis(100),
        request_timeout: std::time::Duration::from_millis(5_000),
        max_shards: 3,
        max_responses_per_interval: 30,
        max_records_per_response: 1_000,
    };

    let requests = config.plan_requests(["a", "b", "c", "d"]);

    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].shard_id, "a");
    assert_eq!(requests[0].max_responses, 10);
    assert_eq!(requests[1].shard_id, "b");
    assert_eq!(requests[1].max_responses, 10);
    assert_eq!(requests[2].shard_id, "c");
    assert_eq!(requests[2].max_responses, 10);
}

#[test]
fn poller_config_keeps_source_request_timeout_separate_from_poll_interval() {
    let source = AnalyticsSourceConfig {
        poll_interval_ms: 100,
        poll_request_timeout_ms: 5_000,
        ..AnalyticsSourceConfig::default()
    };

    let config = PollerConfig::from_source_config(&source);

    assert_eq!(config.poll_interval, std::time::Duration::from_millis(100));
    assert_eq!(
        config.request_timeout,
        std::time::Duration::from_millis(5_000)
    );
}

#[test]
fn table_plans_map_source_tables_to_manifest_tables() {
    let manifest: AnalyticsManifest = serde_json::from_value(serde_json::json!({
        "version": 1,
        "tables": [{
            "source_table_name": "tenant_entities",
            "analytics_table_name": "users",
            "tenant_selector": { "kind": "none" },
            "row_identity": { "kind": "stream_keys" },
            "columns": []
        }]
    }))
    .expect("manifest");
    let source = AnalyticsSourceConfig {
        stream_type: Some(AnalyticsStreamType::AuxStorage),
        endpoint_url: Some("http://127.0.0.1:39124/storage".to_string()),
        tables: vec![AnalyticsSourceTableConfig {
            table_name: "tenant_entities".to_string(),
            stream_type: None,
            stream_identifier: None,
        }],
        ..AnalyticsSourceConfig::default()
    };

    let plans = table_plans(&source, &manifest).expect("plans");

    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].source_table_name, "tenant_entities");
    assert_eq!(plans[0].analytics_table_names, vec!["users"]);
    assert_eq!(plans[0].stream_type, AnalyticsStreamType::AuxStorage);
}

#[test]
fn table_plans_use_manifest_source_tables_when_config_omits_table_list() {
    let manifest: AnalyticsManifest = serde_json::from_value(serde_json::json!({
        "version": 1,
        "tables": [
            {
                "source_table_name": "sys",
                "analytics_table_name": "sys_tenants",
                "tenant_selector": { "kind": "none" },
                "row_identity": { "kind": "stream_keys" },
                "columns": []
            },
            {
                "source_table_name": "sys",
                "analytics_table_name": "sys_domains",
                "tenant_selector": { "kind": "none" },
                "row_identity": { "kind": "stream_keys" },
                "columns": []
            },
            {
                "source_table_name": "s00000",
                "analytics_table_name": "tenant_items",
                "tenant_selector": { "kind": "none" },
                "row_identity": { "kind": "stream_keys" },
                "columns": []
            }
        ]
    }))
    .expect("manifest");
    let source = AnalyticsSourceConfig {
        stream_type: Some(AnalyticsStreamType::AuxStorage),
        endpoint_url: Some("http://127.0.0.1:39124/storage".to_string()),
        tables: Vec::new(),
        ..AnalyticsSourceConfig::default()
    };

    let plans = table_plans(&source, &manifest).expect("plans");

    assert_eq!(plans.len(), 2);
    assert_eq!(plans[0].source_table_name, "sys");
    assert_eq!(
        plans[0].analytics_table_names,
        vec!["sys_tenants", "sys_domains"]
    );
    assert_eq!(plans[1].source_table_name, "s00000");
    assert_eq!(plans[1].analytics_table_names, vec!["tenant_items"]);
}
