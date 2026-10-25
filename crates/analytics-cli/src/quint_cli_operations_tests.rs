use std::{error::Error as _, fs, path::Path};

use analytics_contract::{PrivacyPolicy, StorageItem, StorageStreamRecord, StorageValue};
use analytics_operations::{
    BackfillExecutionReport, BackfillExecutionRequest, CancellationState, FilesystemRawBackupStore,
    LocalBackfillFixture, MaintenanceError, MaintenanceRequest, OperationActor, OperationId,
    OperationKind, OperationRequest, OperationStatus, OperationStore, PrivacyFixDestructiveAction,
    PrivacyFixError, PrivacyFixRequest, RateLimitPolicy, RawBackupObjectId, RawBackupRecord,
    RawBackupWriteRequest, TrimError, TrimReport, TrimRequest, TrimTarget,
};
use clap::Parser as _;
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::Cli;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum CliCase {
    NotStarted,
    OperationsStatus,
    OperationsAudit,
    OperationsCancel,
    BackfillRun,
    BackfillResume,
    TrimDryRun,
    TrimApplyMissingConfirmation,
    PrivacyDeleteMissingConfirmation,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum CliResult {
    NotRun,
    ParsedStatus,
    ParsedAudit,
    ParsedCancel,
    ParsedRun,
    ParsedResume,
    ParsedDryRun,
    RejectedTrimMissingConfirmation,
    RejectedPrivacyMissingConfirmation,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct CliMbtState {
    last_case: CliCase,
    last_result: CliResult,
    operation_persisted: bool,
    report_persisted: bool,
    effect_evidence: bool,
    audit_evidence: bool,
    sentinel_unchanged: bool,
}

impl State<CliDriver> for CliMbtState {
    fn from_driver(driver: &CliDriver) -> Result<Self> {
        Ok(Self {
            last_case: driver.last_case,
            last_result: driver.last_result,
            operation_persisted: driver.operation_persisted,
            report_persisted: driver.report_persisted,
            effect_evidence: driver.effect_evidence,
            audit_evidence: driver.audit_evidence,
            sentinel_unchanged: driver.sentinel_unchanged,
        })
    }
}

struct CliDriver {
    last_case: CliCase,
    last_result: CliResult,
    operation_persisted: bool,
    report_persisted: bool,
    effect_evidence: bool,
    audit_evidence: bool,
    sentinel_unchanged: bool,
}

impl Default for CliDriver {
    fn default() -> Self {
        Self {
            last_case: CliCase::NotStarted,
            last_result: CliResult::NotRun,
            operation_persisted: false,
            report_persisted: false,
            effect_evidence: false,
            audit_evidence: false,
            sentinel_unchanged: true,
        }
    }
}

impl Driver for CliDriver {
    type State = CliMbtState;

    #[allow(non_snake_case)]
    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => *self = Self::default(),
            CliScenario(cliCase: CliCase) => {
                self.apply_observation(cliCase, run_cli_case(cliCase)?);
            },
            step(cliCase: CliCase?) => {
                if let Some(cli_case) = cliCase {
                    self.apply_observation(cli_case, run_cli_case(cli_case)?);
                }
            },
        })
    }
}

impl CliDriver {
    fn apply_observation(&mut self, cli_case: CliCase, observation: CliObservation) {
        self.last_case = cli_case;
        self.last_result = observation.result;
        self.operation_persisted = observation.operation_persisted;
        self.report_persisted = observation.report_persisted;
        self.effect_evidence = observation.effect_evidence;
        self.audit_evidence = observation.audit_event_count > 0;
        self.sentinel_unchanged = observation.sentinel_unchanged;
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/cli_operations_mbt.qnt",
    max_samples = 40,
    max_steps = 4,
    seed = "0xc11"
)]
fn given_quint_cli_operations_model_when_replayed_then_full_state_matches() -> impl Driver {
    CliDriver::default()
}

#[derive(Clone, Copy)]
struct CliObservation {
    result: CliResult,
    operation_persisted: bool,
    report_persisted: bool,
    effect_evidence: bool,
    audit_event_count: i64,
    sentinel_unchanged: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct DurableEvidence {
    operation_count: i64,
    report_count: i64,
    effect_count: i64,
    audit_event_count: i64,
}

fn run_cli_case(cli_case: CliCase) -> Result<CliObservation> {
    match cli_case {
        CliCase::NotStarted => Err(anyhow::anyhow!(
            "NotStarted is an initialization-only CLI model state"
        )),
        CliCase::OperationsStatus => run_status_command(),
        CliCase::OperationsAudit => run_audit_command(),
        CliCase::OperationsCancel => run_cancel_command(),
        CliCase::BackfillRun => run_backfill_run_command(),
        CliCase::BackfillResume => run_backfill_resume_command(),
        CliCase::TrimDryRun => run_trim_dry_run(),
        CliCase::TrimApplyMissingConfirmation => reject_trim_without_confirmation(),
        CliCase::PrivacyDeleteMissingConfirmation => reject_privacy_delete_without_confirmation(),
    }
}

fn observation(
    result: CliResult,
    evidence: DurableEvidence,
    sentinel_unchanged: bool,
) -> CliObservation {
    CliObservation {
        result,
        operation_persisted: evidence.operation_count > 0,
        report_persisted: evidence.report_count > 0,
        effect_evidence: evidence.effect_count > 0,
        audit_event_count: evidence.audit_event_count,
        sentinel_unchanged,
    }
}

fn run_status_command() -> Result<CliObservation> {
    let tempdir = tempfile::tempdir()?;
    let store_path = tempdir.path().join("operations-status.duckdb");
    let operation_id = OperationId::try_new("cli_mbt_status")?;
    seed_operation_store(store_path.as_path(), &operation_id)?;
    let cli = parse_cli([
        "aux-analytics",
        "operations",
        "status",
        "--duckdb",
        path_string(store_path.as_path())?.as_str(),
        "--operation-id",
        operation_id.as_str(),
        "--output",
        "json",
    ])?;
    let output = run_output(cli)?;
    let rendered: serde_json::Value = serde_json::from_str(output.as_str())?;
    let store = OperationStore::connect_duckdb(store_path.as_path())?;
    let persisted = store.show_operation(&operation_id)?;
    if rendered != serde_json::to_value(&persisted)? {
        return Err(anyhow::anyhow!(
            "status output did not match reread operation state"
        ));
    }
    if persisted.status != OperationStatus::Submitted {
        return Err(anyhow::anyhow!(
            "status command changed the seeded operation"
        ));
    }
    let evidence = durable_evidence(store_path.as_path())?;
    if evidence.operation_count != 1
        || evidence.report_count != 0
        || evidence.effect_count != 0
        || evidence.audit_event_count == 0
    {
        return Err(anyhow::anyhow!(
            "status command did not preserve the seeded durable evidence: {evidence:?}"
        ));
    }
    Ok(observation(CliResult::ParsedStatus, evidence, true))
}

fn run_audit_command() -> Result<CliObservation> {
    let tempdir = tempfile::tempdir()?;
    let store_path = tempdir.path().join("operations-audit.duckdb");
    let operation_id = OperationId::try_new("cli_mbt_audit")?;
    seed_operation_store(store_path.as_path(), &operation_id)?;
    let cli = parse_cli([
        "aux-analytics",
        "operations",
        "audit",
        "--duckdb",
        path_string(store_path.as_path())?.as_str(),
        "--operation-id",
        operation_id.as_str(),
        "--output",
        "json",
    ])?;
    let output = run_output(cli)?;
    let rendered: serde_json::Value = serde_json::from_str(output.as_str())?;
    let store = OperationStore::connect_duckdb(store_path.as_path())?;
    let events = store.audit_events(&operation_id)?;
    if rendered != serde_json::to_value(&events)? {
        return Err(anyhow::anyhow!(
            "audit output did not match reread audit events"
        ));
    }
    if events.is_empty() {
        return Err(anyhow::anyhow!(
            "audit command did not reread the seeded audit event"
        ));
    }
    let evidence = durable_evidence(store_path.as_path())?;
    if evidence.operation_count != 1
        || evidence.report_count != 0
        || evidence.effect_count != 0
        || evidence.audit_event_count == 0
    {
        return Err(anyhow::anyhow!(
            "audit command did not preserve the seeded durable evidence: {evidence:?}"
        ));
    }
    Ok(observation(CliResult::ParsedAudit, evidence, true))
}

fn run_cancel_command() -> Result<CliObservation> {
    let tempdir = tempfile::tempdir()?;
    let store_path = tempdir.path().join("operations-cancel.duckdb");
    let operation_id = OperationId::try_new("cli_mbt_cancel")?;
    seed_operation_store(store_path.as_path(), &operation_id)?;
    let store = OperationStore::connect_duckdb(store_path.as_path())?;
    let actor = OperationActor::try_new("cli")?;
    store.prepare_for_execution(&operation_id, &actor)?;
    let cli = parse_cli([
        "aux-analytics",
        "operations",
        "cancel",
        "--duckdb",
        path_string(store_path.as_path())?.as_str(),
        "--operation-id",
        operation_id.as_str(),
        "--output",
        "json",
    ])?;
    let output = run_output(cli)?;
    let rendered: serde_json::Value = serde_json::from_str(output.as_str())?;
    if rendered["operation_id"] != operation_id.as_str()
        || rendered["cancellation_requested"] != true
    {
        return Err(anyhow::anyhow!(
            "cancel output did not acknowledge the requested operation"
        ));
    }
    let store = OperationStore::connect_duckdb(store_path.as_path())?;
    let persisted = store.show_operation(&operation_id)?;
    if persisted.status != OperationStatus::Cancelling
        || persisted.cancellation_state != CancellationState::Requested
    {
        return Err(anyhow::anyhow!(
            "cancel command did not persist cancellation state"
        ));
    }
    let events = store.audit_events(&operation_id)?;
    if events.len() < 2 {
        return Err(anyhow::anyhow!(
            "cancel command did not append a cancellation audit event"
        ));
    }
    let evidence = durable_evidence(store_path.as_path())?;
    if evidence.operation_count != 1
        || evidence.report_count != 0
        || evidence.effect_count != 0
        || evidence.audit_event_count < 2
    {
        return Err(anyhow::anyhow!(
            "cancel command did not expose complete durable evidence: {evidence:?}"
        ));
    }
    Ok(observation(CliResult::ParsedCancel, evidence, true))
}

fn run_backfill_run_command() -> Result<CliObservation> {
    let tempdir = tempfile::tempdir()?;
    let request_path = tempdir.path().join("backfill-run-request.json");
    let fixture_path = tempdir.path().join("backfill-run-fixture.json");
    let store_path = tempdir.path().join("backfill-run-operations.duckdb");
    let operation_id = OperationId::try_new("cli_mbt_backfill_run")?;
    write_backfill_inputs(
        request_path.as_path(),
        fixture_path.as_path(),
        BackfillExecutionRequest {
            operation_id: operation_id.clone(),
            actor: OperationActor::try_new("cli")?,
            target_tables: vec!["users".to_string()],
            rate_limit: RateLimitPolicy::default(),
            fail_once_at_chunk: None,
        },
        LocalBackfillFixture::single_table(&[2, 1], &[3]),
    )?;
    let cli = parse_cli([
        "aux-analytics",
        "backfill",
        "run",
        "--request",
        path_string(request_path.as_path())?.as_str(),
        "--fixture",
        path_string(fixture_path.as_path())?.as_str(),
        "--operation-store-duckdb",
        path_string(store_path.as_path())?.as_str(),
    ])?;
    let output = run_output(cli)?;
    let report: BackfillExecutionReport = serde_json::from_str(output.as_str())?;
    let store = OperationStore::connect_duckdb(store_path.as_path())?;
    let persisted_report = store.backfill_report(&operation_id)?;
    let persisted_operation = store.show_operation(&operation_id)?;
    if report != persisted_report
        || persisted_operation.status != OperationStatus::Succeeded
        || report.metrics.rows_written == 0
        || report.metrics.audit_events_emitted == 0
    {
        return Err(anyhow::anyhow!(
            "backfill run output was not derived from a successful reread"
        ));
    }
    let evidence = durable_evidence(store_path.as_path())?;
    if evidence.operation_count != 1
        || evidence.report_count != 1
        || evidence.effect_count == 0
        || evidence.audit_event_count == 0
    {
        return Err(anyhow::anyhow!(
            "backfill run did not persist complete durable evidence: {evidence:?}"
        ));
    }
    Ok(observation(CliResult::ParsedRun, evidence, true))
}

fn run_backfill_resume_command() -> Result<CliObservation> {
    let tempdir = tempfile::tempdir()?;
    let request_path = tempdir.path().join("backfill-resume-request.json");
    let fixture_path = tempdir.path().join("backfill-resume-fixture.json");
    let store_path = tempdir.path().join("backfill-resume-operations.duckdb");
    let operation_id = OperationId::try_new("cli_mbt_backfill_resume")?;
    write_backfill_inputs(
        request_path.as_path(),
        fixture_path.as_path(),
        BackfillExecutionRequest {
            operation_id: operation_id.clone(),
            actor: OperationActor::try_new("cli")?,
            target_tables: vec!["users".to_string()],
            rate_limit: RateLimitPolicy::default(),
            fail_once_at_chunk: Some(1),
        },
        LocalBackfillFixture::single_table(&[2, 1], &[3]),
    )?;
    // Seed and exercise the operation through the run mapping first. The
    // resume mapping then rereads the succeeded durable report rather than a
    // synthetic parser result.
    let run_cli = parse_backfill_cli(
        "run",
        request_path.as_path(),
        fixture_path.as_path(),
        store_path.as_path(),
    )?;
    let first_output = run_output(run_cli)?;
    let first_report: BackfillExecutionReport = serde_json::from_str(first_output.as_str())?;
    if first_report.metrics.chunks_retried == 0 {
        return Err(anyhow::anyhow!(
            "backfill resume setup did not exercise the retryable production path"
        ));
    }
    let resume_cli = parse_backfill_cli(
        "resume",
        request_path.as_path(),
        fixture_path.as_path(),
        store_path.as_path(),
    )?;
    let output = run_output(resume_cli)?;
    let report: BackfillExecutionReport = serde_json::from_str(output.as_str())?;
    let store = OperationStore::connect_duckdb(store_path.as_path())?;
    let persisted_report = store.backfill_report(&operation_id)?;
    let persisted_operation = store.show_operation(&operation_id)?;
    if report != persisted_report
        || report != first_report
        || persisted_operation.status != OperationStatus::Succeeded
        || report.metrics.rows_written == 0
    {
        return Err(anyhow::anyhow!(
            "backfill resume output was not derived from the reread durable report"
        ));
    }
    let evidence = durable_evidence(store_path.as_path())?;
    if evidence.operation_count != 1
        || evidence.report_count != 1
        || evidence.effect_count == 0
        || evidence.audit_event_count == 0
    {
        return Err(anyhow::anyhow!(
            "backfill resume did not preserve complete durable evidence: {evidence:?}"
        ));
    }
    Ok(observation(CliResult::ParsedResume, evidence, true))
}

fn run_trim_dry_run() -> Result<CliObservation> {
    let tempdir = tempfile::tempdir()?;
    let request_path = tempdir.path().join("trim-request.json");
    let destination_path = tempdir.path().join("analytics.duckdb");
    let store_path = tempdir.path().join("trim-operations.duckdb");
    write_trim_destination(destination_path.as_path())?;
    let operation_id = OperationId::try_new("trim_cli_mbt_dry_run")?;
    write_request(
        request_path.as_path(),
        &MaintenanceRequest::trim(TrimRequest {
            operation_id: operation_id.clone(),
            table_name: "users".to_string(),
            dry_run: true,
            target: TrimTarget::RowKeys {
                keys: vec![b"user-1".to_vec()],
            },
            confirmation_token: None,
        }),
    )?;
    ensure_empty_store(store_path.as_path())?;
    let before_rows = read_trim_rows(destination_path.as_path())?;
    let before_bytes = fs::read(destination_path.as_path())?;
    let cli = parse_trim_cli(
        request_path.as_path(),
        destination_path.as_path(),
        store_path.as_path(),
    )?;
    let output = run_output(cli)?;
    let report: TrimReport = serde_json::from_str(output.as_str())?;
    let after_bytes = fs::read(destination_path.as_path())?;
    let after_rows = read_trim_rows(destination_path.as_path())?;
    let sentinel_unchanged = before_bytes == after_bytes && before_rows == after_rows;
    if !report.dry_run || report.destination_mutated || report.candidate_rows != 1 {
        return Err(anyhow::anyhow!("trim dry-run report was not effect-free"));
    }
    if !sentinel_unchanged {
        return Err(anyhow::anyhow!(
            "trim dry-run changed destination bytes or sentinel rows"
        ));
    }
    let store = OperationStore::connect_duckdb(store_path.as_path())?;
    if store.trim_report(&operation_id)? != report || !operation_exists(&store, &operation_id)? {
        return Err(anyhow::anyhow!(
            "trim dry-run report did not match durable operation state"
        ));
    }
    let evidence = durable_evidence(store_path.as_path())?;
    if evidence.operation_count != 1
        || evidence.report_count != 1
        || evidence.effect_count == 0
        || evidence.audit_event_count == 0
    {
        return Err(anyhow::anyhow!(
            "trim dry-run did not persist complete durable evidence: {evidence:?}"
        ));
    }
    Ok(observation(
        CliResult::ParsedDryRun,
        evidence,
        sentinel_unchanged,
    ))
}

fn reject_trim_without_confirmation() -> Result<CliObservation> {
    let tempdir = tempfile::tempdir()?;
    let request_path = tempdir.path().join("trim-missing-confirmation.json");
    let destination_path = tempdir.path().join("analytics.duckdb");
    let store_path = tempdir.path().join("trim-rejection-operations.duckdb");
    write_trim_destination(destination_path.as_path())?;
    write_request(
        request_path.as_path(),
        &MaintenanceRequest::trim(TrimRequest {
            operation_id: OperationId::try_new("trim_cli_mbt_missing_confirmation")?,
            table_name: "users".to_string(),
            dry_run: false,
            target: TrimTarget::RowKeys {
                keys: vec![b"user-1".to_vec()],
            },
            confirmation_token: None,
        }),
    )?;
    ensure_empty_store(store_path.as_path())?;
    let before_rows = read_trim_rows(destination_path.as_path())?;
    let before_bytes = fs::read(destination_path.as_path())?;
    let cli = parse_trim_cli(
        request_path.as_path(),
        destination_path.as_path(),
        store_path.as_path(),
    )?;
    let error = current_thread_runtime()?
        .block_on(crate::run(cli))
        .expect_err("trim apply without confirmation should be rejected by the CLI boundary");
    if !has_trim_missing_confirmation(&error) {
        return Err(anyhow::anyhow!(
            "CLI returned a non-typed trim confirmation error: {error}"
        ));
    }
    let after_bytes = fs::read(destination_path.as_path())?;
    let after_rows = read_trim_rows(destination_path.as_path())?;
    let sentinel_unchanged = before_bytes == after_bytes && before_rows == after_rows;
    if !sentinel_unchanged {
        return Err(anyhow::anyhow!(
            "invalid trim request changed destination bytes or sentinel rows"
        ));
    }
    let evidence = durable_evidence(store_path.as_path())?;
    if evidence != DurableEvidence::default() {
        return Err(anyhow::anyhow!(
            "invalid trim request created durable evidence: {evidence:?}"
        ));
    }
    Ok(observation(
        CliResult::RejectedTrimMissingConfirmation,
        evidence,
        sentinel_unchanged,
    ))
}

fn reject_privacy_delete_without_confirmation() -> Result<CliObservation> {
    let tempdir = tempfile::tempdir()?;
    let request_path = tempdir.path().join("privacy-missing-confirmation.json");
    let store_path = tempdir.path().join("privacy-rejection-operations.duckdb");
    let source_path = tempdir.path().join("source");
    let clean_path = tempdir.path().join("clean");
    fs::create_dir_all(source_path.as_path())?;
    fs::create_dir_all(clean_path.as_path())?;
    let object_id = RawBackupObjectId::try_new("privacy_cli_mbt_object")?;
    let source_store = FilesystemRawBackupStore::new(source_path.as_path());
    source_store.write_fixture_object(&RawBackupWriteRequest::try_new(
        OperationId::try_new("privacy_cli_mbt_seed_object")?,
        object_id.clone(),
        "source_users",
        None,
        vec![RawBackupRecord::try_new(
            "users",
            "user-1",
            None,
            StorageStreamRecord {
                keys: StorageItem::new(),
                sequence_number: "1".to_string(),
                old_image: None,
                new_image: Some(StorageItem::from_iter([(
                    "email".to_string(),
                    StorageValue::S("private@example.test".to_string()),
                )])),
            },
        )?],
    )?)?;
    fs::write(clean_path.join("sentinel.bin"), b"keep-clean-backup")?;
    let source_index = source_store.read_index(&object_id)?;
    let before_records = source_store.replay_object(&source_index)?;
    write_request(
        request_path.as_path(),
        &MaintenanceRequest::privacy_fix(PrivacyFixRequest {
            operation_id: OperationId::try_new("privacy_cli_mbt_missing_confirmation")?,
            source_backup_prefix: path_string(source_path.as_path())?,
            clean_target_prefix: path_string(clean_path.as_path())?,
            quarantine_target_prefix: path_string(tempdir.path().join("quarantine").as_path())?,
            raw_backup_object_ids: vec![object_id.clone()],
            tables: vec!["users".to_string()],
            policy: PrivacyPolicy::try_new("privacy-v1")?,
            dry_run: false,
            destructive_action: PrivacyFixDestructiveAction::DeleteTainted,
            confirmation_token: None,
        }),
    )?;
    ensure_empty_store(store_path.as_path())?;
    let before_source = snapshot_tree(source_path.as_path())?;
    let before_clean = snapshot_tree(clean_path.as_path())?;
    let cli = parse_privacy_cli(request_path.as_path(), store_path.as_path())?;
    let error = current_thread_runtime()?
        .block_on(crate::run(cli))
        .expect_err("privacy delete without confirmation should be rejected by the CLI boundary");
    if !has_privacy_missing_confirmation(&error) {
        return Err(anyhow::anyhow!(
            "CLI returned a non-typed privacy confirmation error: {error}"
        ));
    }
    let after_source = snapshot_tree(source_path.as_path())?;
    let after_clean = snapshot_tree(clean_path.as_path())?;
    let after_index = source_store.read_index(&object_id)?;
    let after_records = source_store.replay_object(&after_index)?;
    let sentinel_unchanged = before_source == after_source
        && before_clean == after_clean
        && before_records == after_records;
    if !sentinel_unchanged {
        return Err(anyhow::anyhow!(
            "invalid privacy request changed raw-backup or clean-target bytes"
        ));
    }
    let evidence = durable_evidence(store_path.as_path())?;
    if evidence != DurableEvidence::default() {
        return Err(anyhow::anyhow!(
            "invalid privacy request created durable evidence: {evidence:?}"
        ));
    }
    Ok(observation(
        CliResult::RejectedPrivacyMissingConfirmation,
        evidence,
        sentinel_unchanged,
    ))
}

fn has_trim_missing_confirmation(error: &crate::CliError) -> bool {
    error.source().is_some_and(|source| {
        source
            .downcast_ref::<TrimError>()
            .is_some_and(|error| matches!(error, TrimError::MissingConfirmationToken))
            || source
                .downcast_ref::<MaintenanceError>()
                .is_some_and(|error| {
                    matches!(
                        error,
                        MaintenanceError::Trim(TrimError::MissingConfirmationToken)
                    )
                })
    })
}

fn has_privacy_missing_confirmation(error: &crate::CliError) -> bool {
    error.source().is_some_and(|source| {
        source
            .downcast_ref::<PrivacyFixError>()
            .is_some_and(|error| matches!(error, PrivacyFixError::MissingConfirmationToken))
            || source
                .downcast_ref::<MaintenanceError>()
                .is_some_and(|error| {
                    matches!(
                        error,
                        MaintenanceError::PrivacyFix(PrivacyFixError::MissingConfirmationToken)
                    )
                })
    })
}

fn seed_operation_store(path: &Path, operation_id: &OperationId) -> Result<()> {
    let store = OperationStore::connect_duckdb(path)?;
    store.create_operation(&OperationRequest {
        operation_id: operation_id.clone(),
        kind: OperationKind::Backfill,
        actor: OperationActor::try_new("cli")?,
        target_tables: vec!["users".to_string()],
        dry_run: false,
        rate_limit: RateLimitPolicy::default(),
        payload: serde_json::json!({"fixture": "cli_operations_mbt"}),
    })?;
    Ok(())
}

fn ensure_empty_store(path: &Path) -> Result<()> {
    let store = OperationStore::connect_duckdb(path)?;
    if !store.list_operations()?.is_empty() {
        return Err(anyhow::anyhow!("new operation store was not empty"));
    }
    Ok(())
}

fn durable_evidence(path: &Path) -> Result<DurableEvidence> {
    let connection = duckdb::Connection::open(path)?;
    let report_count = table_count(&connection, "__analytics_operation_reports")?;
    Ok(DurableEvidence {
        operation_count: table_count(&connection, "__analytics_operations")?,
        report_count,
        effect_count: table_count(&connection, "__analytics_operation_effects")?,
        audit_event_count: table_count(&connection, "__analytics_operation_events")?,
    })
}

fn table_count(connection: &duckdb::Connection, table: &str) -> duckdb::Result<i64> {
    connection.query_row(
        format!("SELECT count(*) FROM {table}").as_str(),
        [],
        |row| row.get::<_, i64>(0),
    )
}

fn operation_exists(store: &OperationStore, operation_id: &OperationId) -> Result<bool> {
    Ok(store
        .list_operations()?
        .iter()
        .any(|operation| &operation.operation_id == operation_id))
}

fn write_backfill_inputs(
    request_path: &Path,
    fixture_path: &Path,
    request: BackfillExecutionRequest,
    fixture: LocalBackfillFixture,
) -> Result<()> {
    write_request(request_path, &request)?;
    write_request(fixture_path, &fixture)
}

fn write_trim_destination(path: &Path) -> Result<()> {
    let connection = duckdb::Connection::open(path)?;
    connection.execute_batch(
        "create table users (
            __id varchar not null,
            email varchar not null,
            __source_position json
        );
        insert into users values ('757365722d31', 'one@example.com', NULL);",
    )?;
    Ok(())
}

fn read_trim_rows(path: &Path) -> Result<Vec<(String, String)>> {
    let connection = duckdb::Connection::open(path)?;
    let mut statement = connection.prepare("select __id, email from users order by __id")?;
    let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    Ok(rows.collect::<duckdb::Result<Vec<_>>>()?)
}

fn snapshot_tree(root: &Path) -> Result<Vec<(String, Vec<u8>)>> {
    let mut entries = Vec::new();
    if root.exists() {
        snapshot_tree_inner(root, root, &mut entries)?;
    }
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(entries)
}

fn snapshot_tree_inner(
    root: &Path,
    current: &Path,
    entries: &mut Vec<(String, Vec<u8>)>,
) -> Result<()> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            snapshot_tree_inner(root, path.as_path(), entries)?;
        } else if entry.file_type()?.is_file() {
            let relative = path
                .strip_prefix(root)
                .map_err(|error| anyhow::anyhow!(error.to_string()))?
                .to_string_lossy()
                .into_owned();
            entries.push((relative, fs::read(path.as_path())?));
        }
    }
    Ok(())
}

fn write_request<T: serde::Serialize>(path: &Path, request: &T) -> Result<()> {
    fs::write(path, serde_json::to_string(request)?)?;
    Ok(())
}

fn parse_backfill_cli(
    command: &str,
    request_path: &Path,
    fixture_path: &Path,
    store_path: &Path,
) -> Result<Cli> {
    parse_cli([
        "aux-analytics",
        "backfill",
        command,
        "--request",
        path_string(request_path)?.as_str(),
        "--fixture",
        path_string(fixture_path)?.as_str(),
        "--operation-store-duckdb",
        path_string(store_path)?.as_str(),
    ])
}

fn parse_trim_cli(request_path: &Path, destination_path: &Path, store_path: &Path) -> Result<Cli> {
    parse_cli([
        "aux-analytics",
        "trim",
        "plan",
        "--request",
        path_string(request_path)?.as_str(),
        "--destination-duckdb",
        path_string(destination_path)?.as_str(),
        "--operation-store-duckdb",
        path_string(store_path)?.as_str(),
    ])
}

fn parse_privacy_cli(request_path: &Path, store_path: &Path) -> Result<Cli> {
    parse_cli([
        "aux-analytics",
        "privacy-fix",
        "raw-backup",
        "--request",
        path_string(request_path)?.as_str(),
        "--operation-store-duckdb",
        path_string(store_path)?.as_str(),
    ])
}

fn run_output(cli: Cli) -> Result<String> {
    current_thread_runtime()?
        .block_on(crate::run(cli))?
        .ok_or_else(|| anyhow::anyhow!("CLI command did not return output"))
}

fn current_thread_runtime() -> Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?)
}

fn path_string(path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| anyhow::anyhow!("path is not UTF-8: {}", path.display()))
}

fn parse_cli<const N: usize>(args: [&str; N]) -> Result<Cli> {
    Cli::try_parse_from(args).map_err(|error| anyhow::anyhow!(error.to_string()))
}
