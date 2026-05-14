use std::collections::HashMap;

use analytics_contract::{AnalyticsManifest, StorageValue};
use config::{AnalyticsSourceConfig, AnalyticsSourceTableConfig, AnalyticsStreamType};

use crate::{PollerConfig, facade::storage_stream_record_from_facade, planning::table_plans};

#[test]
fn storage_facade_record_converts_to_contract_record() {
    let record = storage_types::StreamRecord {
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
fn poller_defaults_allow_many_responses_per_interval() {
    let config = PollerConfig::default();

    assert_eq!(config.poll_interval, std::time::Duration::from_millis(100));
    assert_eq!(config.max_shards, 16);
    assert_eq!(config.max_responses_per_interval, 160);
    assert_eq!(config.response_budget_per_shard(), 10);
}

#[test]
fn poller_plan_spreads_response_budget_across_concurrent_shards() {
    let config = PollerConfig {
        poll_interval: std::time::Duration::from_millis(100),
        max_shards: 3,
        max_responses_per_interval: 30,
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
