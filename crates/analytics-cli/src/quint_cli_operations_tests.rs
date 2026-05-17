use analytics_contract::PrivacyPolicy;
use analytics_operations::{
    OperationId, PrivacyFixDestructiveAction, PrivacyFixRequest, RawBackupObjectId, TrimRequest,
    TrimTarget,
};
use clap::Parser as _;
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::Cli;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum CliCase {
    OperationsStatus,
    OperationsAudit,
    OperationsCancel,
    BackfillRun,
    BackfillResume,
    TrimApplyMissingConfirmation,
    PrivacyDeleteMissingConfirmation,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum CliResult {
    NotRun,
    ParsedReadOnly,
    ParsedRun,
    ParsedResume,
    ParsedCancel,
    RejectedMissingConfirmation,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct CliMbtState {
    last_case: CliCase,
    last_result: CliResult,
}

impl State<CliDriver> for CliMbtState {
    fn from_driver(driver: &CliDriver) -> Result<Self> {
        Ok(Self {
            last_case: driver.last_case,
            last_result: driver.last_result,
        })
    }
}

struct CliDriver {
    last_case: CliCase,
    last_result: CliResult,
}

impl Default for CliDriver {
    fn default() -> Self {
        Self {
            last_case: CliCase::OperationsStatus,
            last_result: CliResult::NotRun,
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
                self.last_case = cliCase;
                self.last_result = run_cli_case(cliCase)?;
            },
            step(cliCase: CliCase?) => {
                if let Some(cli_case) = cliCase {
                    self.last_case = cli_case;
                    self.last_result = run_cli_case(cli_case)?;
                }
            },
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/cli_operations_mbt.qnt",
    max_samples = 40,
    max_steps = 4,
    seed = "0xc11"
)]
fn quint_cli_operations_mbt_compares_full_state() -> impl Driver {
    CliDriver::default()
}

fn run_cli_case(cli_case: CliCase) -> Result<CliResult> {
    match cli_case {
        CliCase::OperationsStatus => parse_status_command(),
        CliCase::OperationsAudit => parse_audit_command(),
        CliCase::OperationsCancel => parse_cancel_command(),
        CliCase::BackfillRun => parse_backfill_run_command(),
        CliCase::BackfillResume => parse_backfill_resume_command(),
        CliCase::TrimApplyMissingConfirmation => reject_trim_without_confirmation(),
        CliCase::PrivacyDeleteMissingConfirmation => reject_privacy_delete_without_confirmation(),
    }
}

fn parse_status_command() -> Result<CliResult> {
    parse_cli([
        "aux-analytics",
        "operations",
        "status",
        "--duckdb",
        "ops.duckdb",
        "--operation-id",
        "op_1",
    ])?;
    Ok(CliResult::ParsedReadOnly)
}

fn parse_audit_command() -> Result<CliResult> {
    parse_cli([
        "aux-analytics",
        "operations",
        "audit",
        "--duckdb",
        "ops.duckdb",
        "--operation-id",
        "op_1",
    ])?;
    Ok(CliResult::ParsedReadOnly)
}

fn parse_cancel_command() -> Result<CliResult> {
    parse_cli([
        "aux-analytics",
        "operations",
        "cancel",
        "--duckdb",
        "ops.duckdb",
        "--operation-id",
        "op_1",
    ])?;
    Ok(CliResult::ParsedCancel)
}

fn parse_backfill_run_command() -> Result<CliResult> {
    parse_cli([
        "aux-analytics",
        "backfill",
        "run",
        "--request",
        "request.json",
        "--fixture",
        "fixture.json",
        "--operation-store-duckdb",
        "ops.duckdb",
    ])?;
    Ok(CliResult::ParsedRun)
}

fn parse_backfill_resume_command() -> Result<CliResult> {
    parse_cli([
        "aux-analytics",
        "backfill",
        "resume",
        "--request",
        "request.json",
        "--fixture",
        "fixture.json",
        "--operation-store-duckdb",
        "ops.duckdb",
    ])?;
    Ok(CliResult::ParsedResume)
}

fn reject_trim_without_confirmation() -> Result<CliResult> {
    let request = TrimRequest {
        operation_id: OperationId::new("trim_cli_mbt")?,
        table_name: "users".to_string(),
        dry_run: false,
        target: TrimTarget::RowKeys {
            keys: vec![b"user-1".to_vec()],
        },
        confirmation_token: None,
    };
    request
        .validate()
        .map_or_else(confirmation_rejection_or_error, |()| {
            Err(anyhow::anyhow!(
                "trim apply without confirmation was accepted"
            ))
        })
}

fn reject_privacy_delete_without_confirmation() -> Result<CliResult> {
    let request = PrivacyFixRequest {
        operation_id: OperationId::new("privacy_cli_mbt")?,
        source_backup_prefix: "source".to_string(),
        clean_target_prefix: "clean".to_string(),
        raw_backup_object_ids: vec![RawBackupObjectId::new("object_1")?],
        tables: vec!["users".to_string()],
        policy: PrivacyPolicy::new("privacy-v1")?,
        dry_run: false,
        destructive_action: PrivacyFixDestructiveAction::DeleteTainted,
        confirmation_token: None,
    };
    request
        .validate()
        .map_or_else(confirmation_rejection_or_error, |()| {
            Err(anyhow::anyhow!(
                "privacy delete without confirmation was accepted"
            ))
        })
}

fn confirmation_rejection_or_error(
    error: impl std::error::Error + Send + Sync + 'static,
) -> Result<CliResult> {
    if error.to_string().contains("confirmation token") {
        Ok(CliResult::RejectedMissingConfirmation)
    } else {
        Err(error.into())
    }
}

fn parse_cli<const N: usize>(args: [&str; N]) -> Result<Cli> {
    Cli::try_parse_from(args).map_err(|error| anyhow::anyhow!(error.to_string()))
}
