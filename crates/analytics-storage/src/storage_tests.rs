use analytics_contract::{AnalyticsManifest, StorageItem, StorageValue};
use config::{AnalyticsSourceConfig, AnalyticsSourceTableConfig, AnalyticsStreamType};

use crate::{PollerConfig, SourceTablePlan, stream_conversion::convert_stream_record};

#[test]
fn given_stream_record_with_nested_image_when_converted_then_keys_and_nested_values_are_preserved()
{
    let record = storage_types::StreamRecord {
        cursor: None,
        source_table_name: None,
        keys: storage_types::KeyAttributes::from([(
            "id".to_string(),
            storage_types::AttributeValue::S("item-1".to_string()),
        )]),
        sequence_number: "1".to_string(),
        old_image: None,
        new_image: Some(storage_types::AttributeMap::from_iter([(
            "profile".to_string(),
            storage_types::AttributeValue::M(std::collections::HashMap::from([(
                "email".to_string(),
                storage_types::AttributeValue::S("a@example.com".to_string()),
            )])),
        )])),
        system_position: None,
    };

    let converted = convert_stream_record(record);
    assert_eq!(
        converted.keys.get("id"),
        Some(&StorageValue::S("item-1".to_string()))
    );
    assert_eq!(
        converted
            .new_image
            .as_ref()
            .and_then(|item| item.get("profile")),
        Some(&StorageValue::M(StorageItem::from_iter([(
            "email".to_string(),
            StorageValue::S("a@example.com".to_string())
        )])))
    );
}

#[test]
fn given_default_poller_when_request_budget_is_read_then_intervals_and_response_bounds_match_defaults()
 {
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
fn given_four_shards_and_capacity_three_when_requests_are_planned_then_first_three_share_the_response_budget()
 {
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
fn given_distinct_source_interval_and_timeout_when_poller_config_is_built_then_both_durations_are_preserved()
 {
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
fn given_configured_source_table_when_plans_are_built_then_manifest_destination_and_stream_type_are_preserved()
 {
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

    let plans = SourceTablePlan::from_source(&source, &manifest).expect("plans");

    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].source_table_name, "tenant_entities");
    assert_eq!(plans[0].analytics_table_names, vec!["users"]);
    assert_eq!(plans[0].stream_type, AnalyticsStreamType::AuxStorage);
}

#[test]
fn given_no_configured_table_list_when_plans_are_built_then_manifest_sources_are_grouped_with_their_destinations()
 {
    let manifest: AnalyticsManifest = serde_json::from_value(serde_json::json!({
        "version": 1,
        "tables": [
            {
                "source_table_name": "sys",
                "source_table_name_prefix": "sys-prefix",
                "analytics_table_name": "sys_tenants",
                "tenant_selector": { "kind": "none" },
                "row_identity": { "kind": "stream_keys" },
                "columns": []
            },
            {
                "source_table_name": "sys",
                "source_table_name_prefix": "other-prefix",
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

    let plans = SourceTablePlan::from_source(&source, &manifest).expect("plans");

    assert_eq!(plans.len(), 2);
    assert_eq!(plans[0].source_table_name, "sys");
    assert_eq!(
        plans[0].analytics_table_names,
        vec!["sys_tenants", "sys_domains"]
    );
    assert_eq!(
        plans[0].source_table_name_prefix.as_deref(),
        Some("sys-prefix")
    );
    assert_eq!(plans[0].registrations.len(), 2);
    assert_eq!(plans[1].source_table_name, "s00000");
    assert_eq!(plans[1].analytics_table_names, vec!["tenant_items"]);
    assert_eq!(plans[1].registrations.len(), 1);
}
