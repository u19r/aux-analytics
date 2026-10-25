use analytics_contract::{
    PrivacyPolicy, PrivacyPolicyError, StorageItem, StorageStreamRecord, StorageValue,
};
use analytics_fixtures::{storage_key, user_item, users_manifest};
use analytics_operations::{
    FilesystemRawBackupStore, OperationId, RawBackupError, RawBackupObjectId, RawBackupRecord,
    RawBackupWriteRequest,
};
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;
use storage_types::VecMap;

use crate::{AnalyticsEngine, AnalyticsEngineError, IngestOutcome};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum PrivacyCase {
    NotChecked,
    PublicRecord,
    DeniedKey,
    DeniedPath,
    DeniedValue,
    MalformedPolicy,
    OversizedPolicy,
}

#[derive(Clone, Copy, Debug)]
enum InvalidPolicyKind {
    Malformed,
    Oversized,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum PrivacyResult {
    NotRun,
    Allowed,
    Denied,
    InvalidPolicyRejected,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct PrivacyReport {
    result: PrivacyResult,
    reached_raw_backup: bool,
    reached_projection: bool,
    raw_backup_denied_data_absent: bool,
    projection_denied_data_absent: bool,
    raw_backup_allowed_data_preserved: bool,
    projection_allowed_data_preserved: bool,
    dropped_fields: i64,
    policy_version_recorded: bool,
}

impl Default for PrivacyReport {
    fn default() -> Self {
        Self {
            result: PrivacyResult::NotRun,
            reached_raw_backup: false,
            reached_projection: false,
            raw_backup_denied_data_absent: true,
            projection_denied_data_absent: true,
            raw_backup_allowed_data_preserved: false,
            projection_allowed_data_preserved: false,
            dropped_fields: 0,
            policy_version_recorded: false,
        }
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct PrivacyMbtState {
    last_report: PrivacyReport,
    last_case: PrivacyCase,
}

impl State<PrivacyDriver> for PrivacyMbtState {
    fn from_driver(driver: &PrivacyDriver) -> Result<Self> {
        Ok(Self {
            last_report: driver.last_report,
            last_case: driver.last_case,
        })
    }
}

struct PrivacyDriver {
    last_report: PrivacyReport,
    last_case: PrivacyCase,
}

impl Default for PrivacyDriver {
    fn default() -> Self {
        Self {
            last_report: PrivacyReport::default(),
            last_case: PrivacyCase::NotChecked,
        }
    }
}

impl Driver for PrivacyDriver {
    type State = PrivacyMbtState;

    #[expect(
        non_snake_case,
        reason = "Quint action arguments preserve the model's camelCase names"
    )]
    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => *self = Self::default(),
            PrivacyScenario(privacyCase: PrivacyCase) => {
                self.last_case = privacyCase;
                self.last_report = run_privacy_case(privacyCase)?;
            },
            step(privacyCase: PrivacyCase?) => {
                if let Some(privacy_case) = privacyCase {
                    self.last_case = privacy_case;
                    self.last_report = run_privacy_case(privacy_case)?;
                }
            },
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/privacy_policy_mbt.qnt",
    max_samples = 400,
    max_steps = 1,
    seed = "0xA316"
)]
fn given_quint_privacy_policy_model_when_replayed_then_full_state_matches() -> impl Driver {
    PrivacyDriver::default()
}

#[test]
fn given_public_record_when_privacy_boundary_is_executed_then_backup_and_projection_preserve_allowed_data()
 {
    assert_boundary_report(PrivacyCase::PublicRecord, PrivacyResult::Allowed, 0);
}

#[test]
fn given_denied_key_when_privacy_boundary_is_executed_then_backup_and_projection_exclude_denied_data()
 {
    assert_boundary_report(PrivacyCase::DeniedKey, PrivacyResult::Denied, 1);
}

#[test]
fn given_denied_path_when_privacy_boundary_is_executed_then_backup_and_projection_exclude_denied_data()
 {
    assert_boundary_report(PrivacyCase::DeniedPath, PrivacyResult::Denied, 1);
}

#[test]
fn given_denied_value_when_privacy_boundary_is_executed_then_backup_and_projection_exclude_denied_data()
 {
    assert_boundary_report(PrivacyCase::DeniedValue, PrivacyResult::Denied, 1);
}

#[test]
fn given_malformed_privacy_policy_when_boundary_is_executed_then_backup_and_projection_are_not_reached()
 {
    let report = run_privacy_case(PrivacyCase::MalformedPolicy).expect("malformed policy");
    assert_eq!(report.result, PrivacyResult::InvalidPolicyRejected);
    assert!(!report.reached_raw_backup);
    assert!(!report.reached_projection);
    assert_eq!(report.dropped_fields, 0);
    assert!(!report.policy_version_recorded);
}

#[test]
fn given_oversized_privacy_policy_when_boundary_is_executed_then_backup_and_projection_are_not_reached()
 {
    let report = run_privacy_case(PrivacyCase::OversizedPolicy).expect("oversized policy");
    assert_eq!(report.result, PrivacyResult::InvalidPolicyRejected);
    assert!(!report.reached_raw_backup);
    assert!(!report.reached_projection);
    assert_eq!(report.dropped_fields, 0);
    assert!(!report.policy_version_recorded);
}

fn assert_boundary_report(case: PrivacyCase, expected_result: PrivacyResult, expected_drops: i64) {
    let report = run_privacy_case(case).expect("privacy boundary");
    assert_eq!(report.result, expected_result);
    assert!(report.reached_raw_backup);
    assert!(report.reached_projection);
    assert!(report.raw_backup_denied_data_absent);
    assert!(report.projection_denied_data_absent);
    assert!(report.raw_backup_allowed_data_preserved);
    assert!(report.projection_allowed_data_preserved);
    assert_eq!(report.dropped_fields, expected_drops);
    assert!(report.policy_version_recorded);
}

fn run_privacy_case(privacy_case: PrivacyCase) -> Result<PrivacyReport> {
    let (policy, record, expected_record, expected_projection, expected_drops) = match privacy_case
    {
        PrivacyCase::NotChecked => return Ok(PrivacyReport::default()),
        PrivacyCase::PublicRecord => (
            PrivacyPolicy::try_new("privacy-v1")?.with_denied_key_name("secret"),
            public_record(),
            public_record(),
            public_projection(),
            0,
        ),
        PrivacyCase::DeniedKey => (
            PrivacyPolicy::try_new("privacy-v1")?.with_denied_key_name("secret"),
            denied_key_record(),
            denied_key_sanitized_record(),
            public_projection(),
            1,
        ),
        PrivacyCase::DeniedPath => (
            PrivacyPolicy {
                version: "privacy-v1".to_string(),
                denied_exact_paths: vec!["profile.email".to_string()],
                ..PrivacyPolicy::default()
            },
            denied_path_record(),
            denied_path_sanitized_record(),
            denied_path_projection(),
            1,
        ),
        PrivacyCase::DeniedValue => (
            PrivacyPolicy::try_new("privacy-v1")?.with_denied_value_regex("(?i)secret"),
            denied_value_record(),
            denied_value_sanitized_record(),
            public_projection(),
            1,
        ),
        PrivacyCase::MalformedPolicy => {
            return run_invalid_policy_boundaries(
                &malformed_policy(),
                public_record(),
                InvalidPolicyKind::Malformed,
            );
        }
        PrivacyCase::OversizedPolicy => {
            return run_invalid_policy_boundaries(
                &oversized_policy(),
                public_record(),
                InvalidPolicyKind::Oversized,
            );
        }
    };
    run_output_boundaries(
        &policy,
        record,
        expected_record,
        expected_projection,
        expected_drops,
    )
}

struct RawBackupEvidence {
    dropped_fields: u64,
    policy_version_recorded: bool,
    content_matches: bool,
}

fn write_and_replay_raw_backup(
    policy: &PrivacyPolicy,
    record: StorageStreamRecord,
    expected_record: &StorageStreamRecord,
) -> Result<RawBackupEvidence> {
    let raw_backup_root = tempfile::tempdir()?;
    let raw_backup = FilesystemRawBackupStore::new(raw_backup_root.path());
    let operation_id = OperationId::try_new("privacy_policy_mbt")?;
    let object_id = RawBackupObjectId::try_new("privacy_policy_mbt_object")?;
    let request = RawBackupWriteRequest::try_new(
        operation_id,
        object_id.clone(),
        "tenant_01",
        None,
        vec![RawBackupRecord::try_new("users", "user-1", None, record)?],
    )?
    .with_privacy_policy(policy)?;
    let write_report = raw_backup.write_fixture_object_report(&request)?;
    let index = raw_backup.read_index(&object_id)?;
    let replay = raw_backup.replay_object_report(&index)?;
    assert_eq!(replay.records.len(), 1);
    let content_matches = replay.records[0].record == *expected_record;
    assert!(
        content_matches,
        "raw backup replay was not sanitized as expected"
    );
    assert_eq!(
        write_report.metrics.policy_drops,
        request.policy_dropped_records
    );
    assert_eq!(
        index.privacy_policy_version.as_deref(),
        Some(policy.version.as_str())
    );
    Ok(RawBackupEvidence {
        dropped_fields: write_report.metrics.policy_drops,
        policy_version_recorded: index.privacy_policy_version.as_deref()
            == Some(policy.version.as_str()),
        content_matches,
    })
}

struct EngineEvidence {
    dropped_fields: u64,
    policy_version_recorded: bool,
    content_matches: bool,
}

fn ingest_and_query_engine(
    policy: &PrivacyPolicy,
    record: StorageStreamRecord,
    expected_projection: serde_json::Value,
) -> Result<EngineEvidence> {
    let engine = AnalyticsEngine::connect_duckdb(":memory:")?;
    let manifest = users_manifest();
    engine.ensure_manifest(&manifest)?;
    let outcome = engine
        .ingest_stream_record_with_privacy_policy(&manifest, "users", b"user-1", record, policy)?;
    assert_eq!(outcome.outcome, IngestOutcome::Inserted);
    let rows = engine.query_unscoped_sql_json("select user_id, email, org_id, item from users")?;
    let content_matches = rows == vec![expected_projection];
    assert!(content_matches, "projection was not sanitized as expected");
    assert_eq!(outcome.policy_version, policy.version);
    Ok(EngineEvidence {
        dropped_fields: outcome.dropped_fields,
        policy_version_recorded: outcome.policy_version == policy.version,
        content_matches,
    })
}

fn run_output_boundaries(
    policy: &PrivacyPolicy,
    record: StorageStreamRecord,
    expected_record: StorageStreamRecord,
    expected_projection: serde_json::Value,
    expected_dropped_fields: u64,
) -> Result<PrivacyReport> {
    let raw = write_and_replay_raw_backup(policy, record.clone(), &expected_record)?;
    let engine = ingest_and_query_engine(policy, record, expected_projection)?;
    assert_eq!(raw.dropped_fields, expected_dropped_fields);
    assert_eq!(engine.dropped_fields, expected_dropped_fields);
    assert_eq!(raw.dropped_fields, engine.dropped_fields);
    let policy_version_recorded = raw.policy_version_recorded && engine.policy_version_recorded;
    assert!(policy_version_recorded);
    let content_matches = raw.content_matches && engine.content_matches;
    assert!(content_matches);
    let result = if expected_dropped_fields == 0 {
        PrivacyResult::Allowed
    } else {
        PrivacyResult::Denied
    };
    Ok(PrivacyReport {
        result,
        reached_raw_backup: true,
        reached_projection: true,
        raw_backup_denied_data_absent: content_matches,
        projection_denied_data_absent: content_matches,
        raw_backup_allowed_data_preserved: content_matches,
        projection_allowed_data_preserved: content_matches,
        dropped_fields: i64::try_from(expected_dropped_fields)?,
        policy_version_recorded,
    })
}

fn run_invalid_policy_boundaries(
    policy: &PrivacyPolicy,
    record: StorageStreamRecord,
    kind: InvalidPolicyKind,
) -> Result<PrivacyReport> {
    let raw_backup_root = tempfile::tempdir()?;
    let raw_backup = FilesystemRawBackupStore::new(raw_backup_root.path());
    let object_id = RawBackupObjectId::try_new("privacy_policy_mbt_invalid")?;
    let request = RawBackupWriteRequest::try_new(
        OperationId::try_new("privacy_policy_mbt_invalid")?,
        object_id.clone(),
        "tenant_01",
        None,
        vec![RawBackupRecord::try_new(
            "users",
            "user-1",
            None,
            record.clone(),
        )?],
    )?;
    let raw_result = request
        .with_privacy_policy(policy)
        .and_then(|request| raw_backup.write_fixture_object_report(&request));
    let raw_error = raw_result.expect_err("raw backup accepted an invalid policy");
    match raw_error {
        RawBackupError::PrivacyPolicy(error) => assert_expected_policy_error(&error, kind),
        error => panic!("raw backup returned the wrong rejection: {error:?}"),
    }
    assert!(raw_backup.read_index(&object_id).is_err());

    let engine = AnalyticsEngine::connect_duckdb(":memory:")?;
    let manifest = users_manifest();
    engine.ensure_manifest(&manifest)?;
    let engine_result = engine
        .ingest_stream_record_with_privacy_policy(&manifest, "users", b"user-1", record, policy);
    let engine_error = engine_result.expect_err("engine accepted an invalid policy");
    match engine_error {
        AnalyticsEngineError::PrivacyPolicy(error) => assert_expected_policy_error(&error, kind),
        error => panic!("engine returned the wrong rejection: {error:?}"),
    }
    let rows = engine.query_unscoped_sql_json("select count(*) as row_count from users")?;
    assert_eq!(rows, vec![serde_json::json!({"row_count": 0})]);
    Ok(PrivacyReport {
        result: PrivacyResult::InvalidPolicyRejected,
        ..PrivacyReport::default()
    })
}

fn assert_expected_policy_error(error: &PrivacyPolicyError, kind: InvalidPolicyKind) {
    let expected = match kind {
        InvalidPolicyKind::Malformed => matches!(error, PrivacyPolicyError::InvalidRegex(_)),
        InvalidPolicyKind::Oversized => matches!(error, PrivacyPolicyError::RegexTooLong),
    };
    assert!(expected, "unexpected privacy policy rejection: {error:?}");
}

fn public_record() -> StorageStreamRecord {
    record_with_item(
        "privacy-public",
        user_item("user-1", "public@example.test", "org-a"),
    )
}

fn denied_key_record() -> StorageStreamRecord {
    let mut item = user_item("user-1", "public@example.test", "org-a");
    item.insert("secret".to_string(), StorageValue::S("hidden".to_string()));
    record_with_item("privacy-denied-key", item)
}

fn denied_key_sanitized_record() -> StorageStreamRecord {
    record_with_item(
        "privacy-denied-key",
        user_item("user-1", "public@example.test", "org-a"),
    )
}

fn denied_path_record() -> StorageStreamRecord {
    record_with_item(
        "privacy-denied-path",
        VecMap::from_iter([
            ("user_id".to_string(), StorageValue::S("user-1".to_string())),
            (
                "profile".to_string(),
                StorageValue::M(VecMap::from_iter([
                    (
                        "email".to_string(),
                        StorageValue::S("nested-secret@example.test".to_string()),
                    ),
                    (
                        "display_name".to_string(),
                        StorageValue::S("Ada".to_string()),
                    ),
                ])),
            ),
            (
                "organization".to_string(),
                StorageValue::M(VecMap::from_iter([(
                    "id".to_string(),
                    StorageValue::S("org-a".to_string()),
                )])),
            ),
        ]),
    )
}

fn denied_path_sanitized_record() -> StorageStreamRecord {
    record_with_item(
        "privacy-denied-path",
        VecMap::from_iter([
            ("user_id".to_string(), StorageValue::S("user-1".to_string())),
            (
                "profile".to_string(),
                StorageValue::M(VecMap::from_iter([(
                    "display_name".to_string(),
                    StorageValue::S("Ada".to_string()),
                )])),
            ),
            (
                "organization".to_string(),
                StorageValue::M(VecMap::from_iter([(
                    "id".to_string(),
                    StorageValue::S("org-a".to_string()),
                )])),
            ),
        ]),
    )
}

fn denied_value_record() -> StorageStreamRecord {
    let mut item = user_item("user-1", "public@example.test", "org-a");
    item.insert(
        "token".to_string(),
        StorageValue::S("SECRET token".to_string()),
    );
    record_with_item("privacy-denied-value", item)
}

fn denied_value_sanitized_record() -> StorageStreamRecord {
    record_with_item(
        "privacy-denied-value",
        user_item("user-1", "public@example.test", "org-a"),
    )
}

fn malformed_policy() -> PrivacyPolicy {
    PrivacyPolicy {
        version: "privacy-v1".to_string(),
        denied_value_regexes: vec!["[".to_string()],
        ..PrivacyPolicy::default()
    }
}

fn oversized_policy() -> PrivacyPolicy {
    // This fixture is deliberately far above the production validation limit;
    // the model records only the typed rejection, not a copied bound.
    let policy = PrivacyPolicy {
        version: "privacy-v1".to_string(),
        denied_value_regexes: vec!["x".repeat(4 * 1024)],
        ..PrivacyPolicy::default()
    };
    assert!(matches!(
        policy.validate(),
        Err(PrivacyPolicyError::RegexTooLong)
    ));
    policy
}

fn record_with_item(sequence_number: &str, new_image: StorageItem) -> StorageStreamRecord {
    StorageStreamRecord {
        keys: storage_key("USER", "user-1"),
        sequence_number: sequence_number.to_string(),
        old_image: None,
        new_image: Some(new_image),
    }
}

fn public_projection() -> serde_json::Value {
    serde_json::json!({
        "user_id": "user-1",
        "email": "public@example.test",
        "org_id": "org-a",
        "item": {
            "user_id": "user-1",
            "profile": {"email": "public@example.test"},
            "organization": {"id": "org-a"}
        }
    })
}

fn denied_path_projection() -> serde_json::Value {
    serde_json::json!({
        "user_id": "user-1",
        "email": null,
        "org_id": "org-a",
        "item": {
            "user_id": "user-1",
            "profile": {"display_name": "Ada"},
            "organization": {"id": "org-a"}
        }
    })
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum PrivacyModelCase {
    #[serde(rename = "NotCheckedCase")]
    NotChecked,
    #[serde(rename = "PublicRecordCase")]
    PublicRecord,
    #[serde(rename = "DeniedKeyCase")]
    DeniedKey,
    #[serde(rename = "DeniedPathCase")]
    DeniedPath,
    #[serde(rename = "DeniedValueCase")]
    DeniedValue,
    #[serde(rename = "MalformedPolicyCase")]
    MalformedPolicy,
    #[serde(rename = "OversizedPolicyCase")]
    OversizedPolicy,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum PrivacyModelResult {
    NotChecked,
    Allowed,
    Denied,
    InvalidPolicyRejected,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum DenialScope {
    NoDeniedScope,
    KeyNameDenied,
    NestedPathDenied,
    ValueDenied,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct PrivacyModelState {
    result: PrivacyModelResult,
    reached_raw_backup: bool,
    reached_projection: bool,
    raw_backup_denied_data_absent: bool,
    projection_denied_data_absent: bool,
    raw_backup_allowed_data_preserved: bool,
    projection_allowed_data_preserved: bool,
    dropped_fields: i64,
    policy_version_recorded: bool,
    last_case: PrivacyModelCase,
    denial_scope: DenialScope,
    policy_version: i64,
}

impl State<PrivacyModelDriver> for PrivacyModelState {
    fn from_driver(driver: &PrivacyModelDriver) -> Result<Self> {
        Ok(driver.state.clone())
    }
}

struct PrivacyModelDriver {
    state: PrivacyModelState,
}

impl Default for PrivacyModelDriver {
    fn default() -> Self {
        Self {
            state: default_model_state(),
        }
    }
}

impl Driver for PrivacyModelDriver {
    type State = PrivacyModelState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => *self = Self::default(),
            AllowPublicRecord => {
                self.state = run_privacy_model_case(
                    PrivacyModelCase::PublicRecord,
                    DenialScope::NoDeniedScope,
                )?;
            },
            SanitizeDeniedKeyRecord => {
                self.state = run_privacy_model_case(
                    PrivacyModelCase::DeniedKey,
                    DenialScope::KeyNameDenied,
                )?;
            },
            SanitizeDeniedPathRecord => {
                self.state = run_privacy_model_case(
                    PrivacyModelCase::DeniedPath,
                    DenialScope::NestedPathDenied,
                )?;
            },
            SanitizeDeniedValueRecord => {
                self.state = run_privacy_model_case(
                    PrivacyModelCase::DeniedValue,
                    DenialScope::ValueDenied,
                )?;
            },
            RejectInvalidPolicy => {
                self.state = run_privacy_model_case(
                    PrivacyModelCase::MalformedPolicy,
                    DenialScope::NoDeniedScope,
                )?;
            },
            RejectOversizedPolicy => {
                self.state = run_privacy_model_case(
                    PrivacyModelCase::OversizedPolicy,
                    DenialScope::NoDeniedScope,
                )?;
            },
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/privacy_policy.qnt",
    max_samples = 400,
    max_steps = 1,
    seed = "0xA309"
)]
fn given_quint_privacy_policy_model_when_replayed_then_output_boundaries_matches() -> impl Driver {
    PrivacyModelDriver::default()
}

fn default_model_state() -> PrivacyModelState {
    PrivacyModelState {
        result: PrivacyModelResult::NotChecked,
        reached_raw_backup: false,
        reached_projection: false,
        raw_backup_denied_data_absent: true,
        projection_denied_data_absent: true,
        raw_backup_allowed_data_preserved: false,
        projection_allowed_data_preserved: false,
        dropped_fields: 0,
        policy_version_recorded: false,
        last_case: PrivacyModelCase::NotChecked,
        denial_scope: DenialScope::NoDeniedScope,
        policy_version: 0,
    }
}

fn run_privacy_model_case(
    case: PrivacyModelCase,
    denial_scope: DenialScope,
) -> Result<PrivacyModelState> {
    let privacy_case = match case {
        PrivacyModelCase::NotChecked => PrivacyCase::NotChecked,
        PrivacyModelCase::PublicRecord => PrivacyCase::PublicRecord,
        PrivacyModelCase::DeniedKey => PrivacyCase::DeniedKey,
        PrivacyModelCase::DeniedPath => PrivacyCase::DeniedPath,
        PrivacyModelCase::DeniedValue => PrivacyCase::DeniedValue,
        PrivacyModelCase::MalformedPolicy => PrivacyCase::MalformedPolicy,
        PrivacyModelCase::OversizedPolicy => PrivacyCase::OversizedPolicy,
    };
    let report = run_privacy_case(privacy_case)?;
    if matches!(case, PrivacyModelCase::NotChecked) {
        return Ok(default_model_state());
    }
    Ok(PrivacyModelState {
        result: model_result(report.result),
        reached_raw_backup: report.reached_raw_backup,
        reached_projection: report.reached_projection,
        raw_backup_denied_data_absent: report.raw_backup_denied_data_absent,
        projection_denied_data_absent: report.projection_denied_data_absent,
        raw_backup_allowed_data_preserved: report.raw_backup_allowed_data_preserved,
        projection_allowed_data_preserved: report.projection_allowed_data_preserved,
        dropped_fields: report.dropped_fields,
        policy_version_recorded: report.policy_version_recorded,
        last_case: case,
        denial_scope,
        policy_version: i64::from(report.policy_version_recorded),
    })
}

fn model_result(result: PrivacyResult) -> PrivacyModelResult {
    match result {
        PrivacyResult::NotRun => PrivacyModelResult::NotChecked,
        PrivacyResult::Allowed => PrivacyModelResult::Allowed,
        PrivacyResult::Denied => PrivacyModelResult::Denied,
        PrivacyResult::InvalidPolicyRejected => PrivacyModelResult::InvalidPolicyRejected,
    }
}
