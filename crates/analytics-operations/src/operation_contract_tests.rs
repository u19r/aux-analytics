use std::collections::HashMap;

use analytics_contract::{StorageStreamRecord, StorageValue};

use crate::{
    BackfillExecutionRequest, CheckComparisonStrategy, CheckOutputFormat, CheckRequest,
    OperationActor, OperationId, OperationKind, OperationRequest, PrivacyFixDestructiveAction,
    PrivacyFixRequest, RateLimitPolicy, RawBackupObjectId, RawBackupRecord, RawBackupWriteRequest,
    TableFixRequest, TableFixSelection, TableFixSource, TableFixValidation, TrimRequest,
    TrimTarget,
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

fn round_trip<T>(value: &T)
where T: std::fmt::Debug + PartialEq + serde::Serialize + serde::de::DeserializeOwned {
    let encoded = serde_json::to_value(value).expect("serialize operation contract");
    let decoded = serde_json::from_value(encoded).expect("deserialize operation contract");
    assert_eq!(*value, decoded);
}

fn operation_request() -> OperationRequest {
    OperationRequest {
        operation_id: OperationId::new("op_contract").unwrap(),
        kind: OperationKind::Backfill,
        actor: OperationActor::new("operator").unwrap(),
        target_tables: vec!["users".to_string()],
        dry_run: true,
        rate_limit: RateLimitPolicy::default(),
        payload: serde_json::json!({"mode": "contract-test"}),
    }
}

fn backfill_execution_request() -> BackfillExecutionRequest {
    BackfillExecutionRequest {
        operation_id: OperationId::new("backfill_contract").unwrap(),
        actor: OperationActor::new("operator").unwrap(),
        target_tables: vec!["users".to_string()],
        rate_limit: RateLimitPolicy::default(),
        fail_once_at_chunk: Some(1),
    }
}

fn check_request() -> CheckRequest {
    CheckRequest {
        operation_id: OperationId::new("check_contract").unwrap(),
        actor: OperationActor::new("operator").unwrap(),
        target_tables: vec!["users".to_string()],
        strategy: CheckComparisonStrategy::FullRows,
        sample_limit: 3,
        privacy_policy_version: Some("privacy-v1".to_string()),
        output: CheckOutputFormat::Json,
        rate_limit: RateLimitPolicy::default(),
    }
}

fn raw_backup_write_request() -> RawBackupWriteRequest {
    RawBackupWriteRequest::new(
        OperationId::new("raw_backup_contract").unwrap(),
        RawBackupObjectId::new("object_1").unwrap(),
        "source_users",
        Some("privacy-v1".to_string()),
        vec![RawBackupRecord::new("users", "user-1", None, storage_record()).unwrap()],
    )
    .unwrap()
}

fn table_fix_request() -> TableFixRequest {
    TableFixRequest {
        operation_id: OperationId::new("table_fix_contract").unwrap(),
        table_name: "users".to_string(),
        dry_run: true,
        selection: TableFixSelection::RowKeys {
            keys: vec![b"user-1".to_vec()],
        },
        source: TableFixSource::RawBackup {
            object_ids: vec![RawBackupObjectId::new("object_1").unwrap()],
        },
        validation: TableFixValidation {
            require_post_fix_check: true,
            max_rows: 100,
        },
    }
}

fn privacy_fix_request() -> PrivacyFixRequest {
    PrivacyFixRequest {
        operation_id: OperationId::new("privacy_fix_contract").unwrap(),
        source_backup_prefix: "source".to_string(),
        clean_target_prefix: "clean".to_string(),
        raw_backup_object_ids: vec![RawBackupObjectId::new("object_1").unwrap()],
        tables: vec!["users".to_string()],
        policy: analytics_contract::PrivacyPolicy::new("privacy-v1").unwrap(),
        dry_run: true,
        destructive_action: PrivacyFixDestructiveAction::None,
        confirmation_token: None,
    }
}

fn trim_request() -> TrimRequest {
    TrimRequest {
        operation_id: OperationId::new("trim_contract").unwrap(),
        table_name: "users".to_string(),
        dry_run: true,
        target: TrimTarget::RowKeys {
            keys: vec![b"user-1".to_vec()],
        },
        confirmation_token: None,
    }
}

fn storage_record() -> StorageStreamRecord {
    StorageStreamRecord {
        sequence_number: "1".to_string(),
        keys: HashMap::new(),
        old_image: None,
        new_image: Some(HashMap::from([(
            "user_id".to_string(),
            StorageValue::S("user-1".to_string()),
        )])),
    }
}
