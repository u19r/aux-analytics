use std::{fs, path::Path};

use analytics_contract::PrivacyPolicy;
use analytics_operations::{
    FilesystemRawBackupStore, OperationActor, OperationEventKind, OperationId, OperationKind,
    OperationPhase, OperationRequest, OperationStatus, OperationStore, PrivacyFixDestructiveAction,
    PrivacyFixExecutor, PrivacyFixRequest, RateLimitPolicy, RawBackupObjectId,
};

use crate::{
    error::{CliError, CliErrorDebug, CliErrorKind, CliResult},
    privacy_fix_args::{PrivacyFixCommand, PrivacyFixDestructiveActionArg},
};

pub(crate) fn run_privacy_fix_command(command: PrivacyFixCommand) -> CliResult<String> {
    match command {
        PrivacyFixCommand::RawBackup {
            request,
            source_backup_root,
            clean_target_root,
            operation_store_duckdb,
            operation_id,
            object_ids,
            privacy_policy,
            tables,
            apply,
            destructive_action,
            confirmation_token,
        } => {
            let request = build_raw_backup_request(RawBackupCliRequestInput {
                request,
                source_backup_root,
                clean_target_root,
                operation_id,
                object_ids,
                privacy_policy,
                tables,
                apply,
                destructive_action,
                confirmation_token,
            })?;
            let source_store = FilesystemRawBackupStore::new(&request.source_backup_prefix);
            let clean_store = FilesystemRawBackupStore::new(&request.clean_target_prefix);
            let report = PrivacyFixExecutor.execute_raw_backup_rewrite(
                &request,
                &source_store,
                &clean_store,
                &[],
            )?;
            let store = OperationStore::connect_duckdb(operation_store_duckdb)?;
            create_or_resume_privacy_fix_operation(&store, &request)?;
            store.save_privacy_fix_report(&request.operation_id, &report)?;
            store.transition_with_counts(
                &request.operation_id,
                OperationPhase::Completed,
                OperationStatus::Succeeded,
                None,
                OperationEventKind::Succeeded,
                report.operation_counts(),
                Some("privacy fix completed"),
            )?;
            Ok(serde_json::to_string_pretty(&report)?)
        }
    }
}

struct RawBackupCliRequestInput {
    request: Option<std::path::PathBuf>,
    source_backup_root: Option<std::path::PathBuf>,
    clean_target_root: Option<std::path::PathBuf>,
    operation_id: Option<String>,
    object_ids: Vec<String>,
    privacy_policy: Option<std::path::PathBuf>,
    tables: Vec<String>,
    apply: bool,
    destructive_action: PrivacyFixDestructiveActionArg,
    confirmation_token: Option<String>,
}

fn build_raw_backup_request(input: RawBackupCliRequestInput) -> CliResult<PrivacyFixRequest> {
    if let Some(request_path) = input.request.as_ref() {
        reject_request_overrides(&input)?;
        let request =
            serde_json::from_str::<PrivacyFixRequest>(fs::read_to_string(request_path)?.as_str())?;
        request.validate()?;
        return Ok(request);
    }
    let source_backup_root = required_path(input.source_backup_root, "--source-backup-root")?;
    let clean_target_root = required_path(input.clean_target_root, "--clean-target-root")?;
    let operation_id = required_string(input.operation_id, "--operation-id")?;
    let privacy_policy = required_path(input.privacy_policy, "--privacy-policy")?;
    let raw_backup_object_ids = input
        .object_ids
        .into_iter()
        .map(RawBackupObjectId::new)
        .collect::<Result<Vec<_>, _>>()?;
    let request = PrivacyFixRequest {
        operation_id: OperationId::new(operation_id)?,
        source_backup_prefix: source_backup_root.to_string_lossy().into_owned(),
        clean_target_prefix: clean_target_root.to_string_lossy().into_owned(),
        raw_backup_object_ids,
        tables: input.tables,
        policy: read_privacy_policy(privacy_policy.as_path())?,
        dry_run: !input.apply,
        destructive_action: input.destructive_action.into(),
        confirmation_token: input.confirmation_token,
    };
    request.validate()?;
    Ok(request)
}

fn reject_request_overrides(input: &RawBackupCliRequestInput) -> CliResult<()> {
    let has_overrides = input.source_backup_root.is_some()
        || input.clean_target_root.is_some()
        || input.operation_id.is_some()
        || !input.object_ids.is_empty()
        || input.privacy_policy.is_some()
        || !input.tables.is_empty()
        || input.apply
        || !matches!(
            input.destructive_action,
            PrivacyFixDestructiveActionArg::None
        )
        || input.confirmation_token.is_some();
    if has_overrides {
        return Err(cli_message(
            "--request cannot be mixed with raw-backup request fields",
        ));
    }
    Ok(())
}

fn required_path(
    value: Option<std::path::PathBuf>,
    flag_name: &'static str,
) -> CliResult<std::path::PathBuf> {
    value.ok_or_else(|| cli_message(format!("{flag_name} is required without --request")))
}

fn required_string(value: Option<String>, flag_name: &'static str) -> CliResult<String> {
    value.ok_or_else(|| cli_message(format!("{flag_name} is required without --request")))
}

fn cli_message(message: impl Into<String>) -> CliError {
    CliError::with_debug(
        CliErrorKind::Operations,
        CliErrorDebug::Message(message.into()),
    )
}

impl From<PrivacyFixDestructiveActionArg> for PrivacyFixDestructiveAction {
    fn from(value: PrivacyFixDestructiveActionArg) -> Self {
        match value {
            PrivacyFixDestructiveActionArg::None => Self::None,
            PrivacyFixDestructiveActionArg::QuarantineTainted => Self::QuarantineTainted,
            PrivacyFixDestructiveActionArg::DeleteTainted => Self::DeleteTainted,
        }
    }
}

fn read_privacy_policy(path: &Path) -> CliResult<PrivacyPolicy> {
    let policy = serde_json::from_str::<PrivacyPolicy>(fs::read_to_string(path)?.as_str())?;
    policy.validate()?;
    Ok(policy)
}

fn create_or_resume_privacy_fix_operation(
    store: &OperationStore,
    request: &PrivacyFixRequest,
) -> CliResult<()> {
    match store.create_operation(&OperationRequest {
        operation_id: request.operation_id.clone(),
        kind: OperationKind::PrivacyFix,
        actor: OperationActor::new("cli")?,
        target_tables: request.tables.clone(),
        dry_run: request.dry_run,
        rate_limit: RateLimitPolicy::default(),
        payload: serde_json::to_value(request)?,
    }) {
        Ok(()) | Err(analytics_operations::OperationStoreError::DuplicateOperation(_)) => Ok(()),
        Err(error) => Err(error.into()),
    }
}
