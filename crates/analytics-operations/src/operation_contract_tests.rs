use analytics_contract::{StorageItem, StorageStreamRecord, StorageValue};
use serde_json::{Value, json};

use crate::{
    BackfillExecutionRequest, BackfillRequest, BackfillSourceCapabilities, CheckComparisonStrategy,
    CheckOutputFormat, CheckRequest, DestinationBackend, LocalBackfillFixture, MaintenanceRequest,
    OperationActor, OperationId, OperationKind, OperationRequest, PrivacyFixDestructiveAction,
    PrivacyFixRequest, RateLimitPolicy, RawBackupObjectId, RawBackupRecord, RawBackupWriteRequest,
    SnapshotMethod, TableFixRequest, TableFixSelection, TableFixSource, TableFixValidation,
    TrimRequest, TrimTarget, ValidationMode,
};

#[test]
fn given_public_operation_contracts_when_serialized_then_they_round_trip_without_shape_loss() {
    round_trip(&operation_request());
    round_trip(&backfill_execution_request());
    round_trip(&check_request());
    round_trip(&raw_backup_write_request());
    round_trip(&table_fix_request());
    round_trip(&privacy_fix_request());
    round_trip(&trim_request());
}

#[test]
fn given_operation_newtypes_when_deserialized_empty_then_validation_rejects_them() {
    let error = serde_json::from_str::<OperationId>("\"\"").unwrap_err();
    assert!(error.to_string().contains("operation id cannot be empty"));

    let error = serde_json::from_str::<OperationActor>("\"  \"").unwrap_err();
    assert!(
        error
            .to_string()
            .contains("operation actor cannot be empty")
    );
}

#[test]
fn given_request_contracts_when_unknown_fields_are_present_then_deserialization_rejects_them() {
    let request = MaintenanceRequest::trim(trim_request());

    let mut top_level = serde_json::to_value(&request).expect("maintenance request JSON");
    top_level
        .as_object_mut()
        .expect("maintenance request object")
        .insert("legacy_mode".to_string(), json!(true));
    assert!(serde_json::from_value::<MaintenanceRequest>(top_level).is_err());

    let mut nested = serde_json::to_value(&request).expect("maintenance request JSON");
    nested["request"]["target"]["legacy_mode"] = json!(true);
    assert!(serde_json::from_value::<MaintenanceRequest>(nested).is_err());

    let mut privacy_fix =
        serde_json::to_value(MaintenanceRequest::privacy_fix(privacy_fix_request()))
            .expect("privacy fix request JSON");
    privacy_fix["request"]["policy"]["legacy_mode"] = json!(true);
    assert!(serde_json::from_value::<MaintenanceRequest>(privacy_fix).is_err());

    let mut raw_backup =
        serde_json::to_value(MaintenanceRequest::raw_backup(raw_backup_write_request()))
            .expect("raw backup request JSON");
    raw_backup["request"]["records"][0]["record"]["legacy_mode"] = json!(true);
    assert!(serde_json::from_value::<MaintenanceRequest>(raw_backup).is_err());

    let mut raw_backup =
        serde_json::to_value(MaintenanceRequest::raw_backup(raw_backup_write_request()))
            .expect("raw backup request JSON");
    raw_backup["request"]["records"][0]["source_position"] = json!({
        "type": "dynamo_db_stream_sequence",
        "stream_arn": "stream",
        "shard_id": "shard-1",
        "sequence_number": "1",
        "legacy_mode": true
    });
    assert!(serde_json::from_value::<MaintenanceRequest>(raw_backup).is_err());

    let mut policy = serde_json::to_value(RateLimitPolicy::default()).expect("rate policy JSON");
    policy["maintenance_windows_utc"] = json!([
        {
            "start_minute_utc": 60,
            "end_minute_utc": 120,
            "legacy_mode": true
        }
    ]);
    assert!(serde_json::from_value::<RateLimitPolicy>(policy).is_err());

    let mut backfill = serde_json::to_value(backfill_request()).expect("backfill request JSON");
    backfill["source"]["legacy_mode"] = json!(true);
    assert!(serde_json::from_value::<BackfillRequest>(backfill).is_err());

    let mut execution =
        serde_json::to_value(backfill_execution_request()).expect("backfill execution JSON");
    execution["legacy_mode"] = json!(true);
    assert!(serde_json::from_value::<BackfillExecutionRequest>(execution).is_err());

    let mut fixture = serde_json::to_value(LocalBackfillFixture::single_table(&[1], &[]))
        .expect("backfill fixture JSON");
    fixture["chunks"][0]["legacy_mode"] = json!(true);
    assert!(serde_json::from_value::<LocalBackfillFixture>(fixture).is_err());

    let mut operation = serde_json::to_value(operation_request()).expect("operation request JSON");
    operation["legacy_mode"] = json!(true);
    assert!(serde_json::from_value::<OperationRequest>(operation).is_err());
}

#[test]
fn given_request_schemas_when_generated_then_closed_objects_reject_additional_properties() {
    let maintenance_schema =
        serde_json::to_value(schemars::schema_for!(MaintenanceRequest)).expect("schema");
    assert_schema_definition_is_closed(&maintenance_schema, "MaintenanceRequest");

    let policy_schema =
        serde_json::to_value(schemars::schema_for!(RateLimitPolicy)).expect("rate limit schema");
    assert_schema_definition_is_closed(&policy_schema, "RateLimitPolicy");
    assert_schema_definition_is_closed(&policy_schema, "MaintenanceWindow");

    let backfill_schema =
        serde_json::to_value(schemars::schema_for!(BackfillRequest)).expect("backfill schema");
    assert_schema_definition_is_closed(&backfill_schema, "BackfillRequest");
    assert_schema_definition_is_closed(&backfill_schema, "BackfillSourceCapabilities");

    let execution_schema =
        serde_json::to_value(schemars::schema_for!(BackfillExecutionRequest)).expect("schema");
    assert_schema_definition_is_closed(&execution_schema, "BackfillExecutionRequest");

    let fixture_schema =
        serde_json::to_value(schemars::schema_for!(LocalBackfillFixture)).expect("schema");
    for type_name in [
        "LocalBackfillFixture",
        "LocalBackfillChunk",
        "LocalStreamUpdate",
    ] {
        assert_schema_definition_is_closed(&fixture_schema, type_name);
    }

    for type_name in [
        "TableFixRequest",
        "TableFixSelection",
        "TableFixSource",
        "TableFixValidation",
        "PrivacyFixRequest",
        "PrivacyPolicy",
        "TrimRequest",
        "TrimTarget",
        "RawBackupWriteRequest",
        "RawBackupObjectLayout",
        "RawBackupRecord",
        "SourcePosition",
        "RawBackupEncryption",
    ] {
        assert_schema_definition_is_closed(&maintenance_schema, type_name);
    }
}

fn assert_schema_definition_is_closed(schema: &Value, type_name: &str) {
    let definition = schema
        .get("$defs")
        .and_then(|definitions| definitions.get(type_name))
        .or_else(|| (schema.get("title")?.as_str()? == type_name).then_some(schema))
        .unwrap_or_else(|| panic!("schema definition {type_name} missing: {schema}"));
    if let Some(additional_properties) = definition.get("additionalProperties") {
        assert_eq!(
            additional_properties,
            &Value::Bool(false),
            "schema definition {type_name} must reject unknown fields: {definition}"
        );
        return;
    }

    let variants = definition
        .get("oneOf")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("schema definition {type_name} is not an object: {definition}"));
    assert!(
        !variants.is_empty(),
        "schema definition {type_name} has no variants"
    );
    for variant in variants {
        assert_eq!(
            variant.get("additionalProperties"),
            Some(&Value::Bool(false)),
            "schema definition {type_name} variant must reject unknown fields: {variant}"
        );
    }
}

fn round_trip<T>(value: &T)
where T: std::fmt::Debug + PartialEq + serde::Serialize + serde::de::DeserializeOwned {
    let encoded = serde_json::to_value(value).expect("serialize operation contract");
    let decoded = serde_json::from_value(encoded).expect("deserialize operation contract");
    assert_eq!(*value, decoded);
}

fn operation_request() -> OperationRequest {
    OperationRequest {
        operation_id: OperationId::try_new("op_contract").unwrap(),
        kind: OperationKind::Backfill,
        actor: OperationActor::try_new("operator").unwrap(),
        target_tables: vec!["users".to_string()],
        dry_run: true,
        rate_limit: RateLimitPolicy::default(),
        payload: serde_json::json!({"mode": "contract-test"}),
    }
}

fn backfill_execution_request() -> BackfillExecutionRequest {
    BackfillExecutionRequest {
        operation_id: OperationId::try_new("backfill_contract").unwrap(),
        actor: OperationActor::try_new("operator").unwrap(),
        target_tables: vec!["users".to_string()],
        rate_limit: RateLimitPolicy::default(),
        fail_once_at_chunk: Some(1),
    }
}

fn check_request() -> CheckRequest {
    CheckRequest {
        operation_id: OperationId::try_new("check_contract").unwrap(),
        actor: OperationActor::try_new("operator").unwrap(),
        target_tables: vec!["users".to_string()],
        strategy: CheckComparisonStrategy::FullRows,
        sample_limit: 3,
        privacy_policy_version: Some("privacy-v1".to_string()),
        output: CheckOutputFormat::Json,
        rate_limit: RateLimitPolicy::default(),
    }
}

fn raw_backup_write_request() -> RawBackupWriteRequest {
    RawBackupWriteRequest::try_new(
        OperationId::try_new("raw_backup_contract").unwrap(),
        RawBackupObjectId::try_new("object_1").unwrap(),
        "source_users",
        Some("privacy-v1".to_string()),
        vec![RawBackupRecord::try_new("users", "user-1", None, storage_record()).unwrap()],
    )
    .unwrap()
}

fn table_fix_request() -> TableFixRequest {
    TableFixRequest {
        operation_id: OperationId::try_new("table_fix_contract").unwrap(),
        table_name: "users".to_string(),
        dry_run: true,
        selection: TableFixSelection::RowKeys {
            keys: vec![b"user-1".to_vec()],
        },
        source: TableFixSource::RawBackup {
            object_ids: vec![RawBackupObjectId::try_new("object_1").unwrap()],
        },
        validation: TableFixValidation {
            require_post_fix_check: true,
            max_rows: 100,
            confirmation: None,
        },
    }
}

fn privacy_fix_request() -> PrivacyFixRequest {
    PrivacyFixRequest {
        operation_id: OperationId::try_new("privacy_fix_contract").unwrap(),
        source_backup_prefix: std::env::temp_dir()
            .join("analytics-privacy-contract-source")
            .to_string_lossy()
            .into_owned(),
        clean_target_prefix: std::env::temp_dir()
            .join("analytics-privacy-contract-clean")
            .to_string_lossy()
            .into_owned(),
        quarantine_target_prefix: std::env::temp_dir()
            .join("analytics-privacy-contract-quarantine")
            .to_string_lossy()
            .into_owned(),
        raw_backup_object_ids: vec![RawBackupObjectId::try_new("object_1").unwrap()],
        tables: vec!["users".to_string()],
        policy: analytics_contract::PrivacyPolicy::try_new("privacy-v1").unwrap(),
        dry_run: true,
        destructive_action: PrivacyFixDestructiveAction::None,
        confirmation_token: None,
    }
}

fn trim_request() -> TrimRequest {
    TrimRequest {
        operation_id: OperationId::try_new("trim_contract").unwrap(),
        table_name: "users".to_string(),
        dry_run: true,
        target: TrimTarget::RowKeys {
            keys: vec![b"user-1".to_vec()],
        },
        confirmation_token: None,
    }
}

fn backfill_request() -> BackfillRequest {
    BackfillRequest {
        target_tables: vec!["users".to_string()],
        source: BackfillSourceCapabilities::local_fixture(),
        requested_snapshot_method: Some(SnapshotMethod::LocalFixture),
        filters: Vec::new(),
        chunk_target_rows: 100,
        rate_limit: RateLimitPolicy::default(),
        destination_backend: DestinationBackend::DuckDb,
        dry_run: true,
        validation_mode: ValidationMode::CountsAndHashes,
        all_tables_have_row_identity: true,
        estimated_table_rows: 100,
    }
}

fn storage_record() -> StorageStreamRecord {
    StorageStreamRecord {
        sequence_number: "1".to_string(),
        keys: StorageItem::new(),
        old_image: None,
        new_image: Some(StorageItem::from_iter([(
            "user_id".to_string(),
            StorageValue::S("user-1".to_string()),
        )])),
    }
}

#[test]
fn given_partial_rate_policy_when_deserialized_then_missing_fields_use_defaults_and_null_is_preserved()
 {
    let empty: RateLimitPolicy = serde_json::from_value(json!({})).unwrap();
    assert_eq!(empty, RateLimitPolicy::default());
    let partial: RateLimitPolicy = serde_json::from_value(json!({
        "max_rows_per_second": null,
        "batch_size": 17
    }))
    .unwrap();
    assert_eq!(partial.max_rows_per_second, None);
    assert_eq!(partial.batch_size, 17);
    assert_eq!(partial.max_bytes_per_second, Some(1_000_000));
    assert_eq!(partial.max_parallel_chunks, 1);
    assert_eq!(partial.retry_backoff_ms, 1_000);
}
