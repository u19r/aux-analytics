use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::{OperationId, TrimCandidateSource, TrimError, TrimExecutor, TrimRequest, TrimTarget};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum TrimCase {
    DryRunRowKeys,
    ApplyWithoutConfirmation,
    ApplyRowKeysWithConfirmation,
    ApplyDeletesTooManyRows,
    EmptySelectionRejected,
    UnsafePredicateRejected,
    RowPredicateApply,
    FullTableDropApply,
    HorizonTrimApply,
    SnapshotExpiryUnsupported,
    RawBackupExpirationUnsupported,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum TrimResult {
    NotRun,
    DryRunComplete,
    Applied,
    RejectedMissingConfirmation,
    RejectedApplyNotSupported,
    RejectedDeleteCountExceeded,
    RejectedEmptySelection,
    RejectedUnsafePredicate,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct TrimReport {
    result: TrimResult,
    candidate_rows: i64,
    rows_deleted: i64,
    destination_mutated: bool,
}

impl Default for TrimReport {
    fn default() -> Self {
        Self {
            result: TrimResult::NotRun,
            candidate_rows: 0,
            rows_deleted: 0,
            destination_mutated: false,
        }
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct TrimMbtState {
    last_report: TrimReport,
}

impl State<TrimDriver> for TrimMbtState {
    fn from_driver(driver: &TrimDriver) -> Result<Self> {
        Ok(Self {
            last_report: driver.last_report,
        })
    }
}

#[derive(Default)]
struct TrimDriver {
    last_report: TrimReport,
    counter: u64,
}

impl Driver for TrimDriver {
    type State = TrimMbtState;

    #[allow(non_snake_case)]
    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => *self = Self::default(),
            TrimScenario(trimCase: TrimCase) => {
                self.last_report = self.run_trim_case(trimCase)?;
            },
            step(trimCase: TrimCase?) => {
                if let Some(trim_case) = trimCase {
                    self.last_report = self.run_trim_case(trim_case)?;
                }
            },
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/trim_mbt.qnt",
    max_samples = 50,
    max_steps = 4,
    seed = "0x7a11"
)]
fn quint_trim_mbt_compares_full_state() -> impl Driver {
    TrimDriver::default()
}

impl TrimDriver {
    fn run_trim_case(&mut self, trim_case: TrimCase) -> Result<TrimReport> {
        self.counter += 1;
        match trim_case {
            TrimCase::DryRunRowKeys => self.run_dry_run(row_keys_target(2), 2),
            TrimCase::ApplyWithoutConfirmation => {
                let request = self.request(false, row_keys_target(1), None)?;
                let error = request.validate().unwrap_err();
                Ok(error_report(&error))
            }
            TrimCase::ApplyRowKeysWithConfirmation => {
                self.run_apply_with_confirmation(row_keys_target(1), 1, 1)
            }
            TrimCase::ApplyDeletesTooManyRows => {
                self.run_apply_with_confirmation(row_keys_target(1), 1, 2)
            }
            TrimCase::EmptySelectionRejected => {
                let request = self.request(true, TrimTarget::RowKeys { keys: Vec::new() }, None)?;
                let error = request.validate().unwrap_err();
                Ok(error_report(&error))
            }
            TrimCase::UnsafePredicateRejected => {
                let request = self.request(
                    true,
                    TrimTarget::RowPredicate {
                        predicate_sql: "1 = 1".to_string(),
                    },
                    None,
                )?;
                let error = request.validate().unwrap_err();
                Ok(error_report(&error))
            }
            TrimCase::RowPredicateApply => self.run_apply_with_confirmation(
                TrimTarget::RowPredicate {
                    predicate_sql: "__id = '757365722d30'".to_string(),
                },
                1,
                1,
            ),
            TrimCase::FullTableDropApply => {
                self.run_apply_with_confirmation(TrimTarget::FullTableDrop, 2, 2)
            }
            TrimCase::HorizonTrimApply => self.run_apply_with_confirmation(
                TrimTarget::HorizonTrim {
                    older_than_ms: 1_000,
                },
                1,
                1,
            ),
            TrimCase::SnapshotExpiryUnsupported => self.run_apply_with_confirmation(
                TrimTarget::DuckLakeSnapshotExpiry {
                    older_than_ms: 1_000,
                },
                0,
                0,
            ),
            TrimCase::RawBackupExpirationUnsupported => self.run_apply_with_confirmation(
                TrimTarget::RawBackupExpiration {
                    older_than_ms: 1_000,
                },
                0,
                0,
            ),
        }
    }

    fn run_dry_run(&self, target: TrimTarget, candidate_rows: u64) -> Result<TrimReport> {
        let request = self.request(true, target, None)?;
        let report = TrimExecutor.execute_dry_run(
            &request,
            &FixedCandidateSource {
                count: candidate_rows,
            },
        )?;
        Ok(TrimReport {
            result: TrimResult::DryRunComplete,
            candidate_rows: u64_to_i64(report.candidate_rows),
            rows_deleted: u64_to_i64(report.rows_deleted),
            destination_mutated: report.destination_mutated,
        })
    }

    fn run_apply_with_confirmation(
        &self,
        target: TrimTarget,
        candidate_rows: u64,
        rows_deleted: u64,
    ) -> Result<TrimReport> {
        let request = self.request(false, target, Some("trim-confirmed".to_string()))?;
        match TrimExecutor.execute_apply(
            &request,
            &FixedCandidateSource {
                count: candidate_rows,
            },
            &mut FixedTrimDestination { rows_deleted },
        ) {
            Ok(report) => Ok(TrimReport {
                result: TrimResult::Applied,
                candidate_rows: u64_to_i64(report.candidate_rows),
                rows_deleted: u64_to_i64(report.rows_deleted),
                destination_mutated: report.destination_mutated,
            }),
            Err(error) => Ok(error_report(&error)),
        }
    }

    fn request(
        &self,
        dry_run: bool,
        target: TrimTarget,
        confirmation_token: Option<String>,
    ) -> crate::TrimResult<TrimRequest> {
        Ok(TrimRequest {
            operation_id: OperationId::new(format!("trim_mbt_{}", self.counter))?,
            table_name: "users".to_string(),
            dry_run,
            target,
            confirmation_token,
        })
    }
}

struct FixedCandidateSource {
    count: u64,
}

impl TrimCandidateSource for FixedCandidateSource {
    fn count_candidates(&self, _table_name: &str, _target: &TrimTarget) -> crate::TrimResult<u64> {
        Ok(self.count)
    }
}

struct FixedTrimDestination {
    rows_deleted: u64,
}

impl crate::TrimDestination for FixedTrimDestination {
    fn delete_candidates(
        &mut self,
        _table_name: &str,
        target: &TrimTarget,
        _max_rows: u64,
    ) -> crate::TrimResult<u64> {
        match target {
            TrimTarget::RowKeys { .. }
            | TrimTarget::FullTableDrop
            | TrimTarget::HorizonTrim { .. }
            | TrimTarget::RowPredicate { .. } => Ok(self.rows_deleted),
            TrimTarget::DuckLakeSnapshotExpiry { .. } | TrimTarget::RawBackupExpiration { .. } => {
                Err(TrimError::ApplyNotSupported)
            }
        }
    }
}

fn row_keys_target(count: usize) -> TrimTarget {
    TrimTarget::RowKeys {
        keys: (0..count)
            .map(|index| format!("user-{index}").into_bytes())
            .collect(),
    }
}

fn error_report(error: &TrimError) -> TrimReport {
    if let TrimError::DeleteCountExceededDryRun {
        candidate_rows,
        deleted_rows,
    } = error
    {
        return TrimReport {
            result: TrimResult::RejectedDeleteCountExceeded,
            candidate_rows: u64_to_i64(*candidate_rows),
            rows_deleted: u64_to_i64(*deleted_rows),
            destination_mutated: false,
        };
    }
    TrimReport {
        result: match error {
            TrimError::MissingConfirmationToken => TrimResult::RejectedMissingConfirmation,
            TrimError::ApplyNotSupported => TrimResult::RejectedApplyNotSupported,
            TrimError::EmptyRowSelection => TrimResult::RejectedEmptySelection,
            TrimError::UnsafePredicate => TrimResult::RejectedUnsafePredicate,
            TrimError::OperationType(_)
            | TrimError::DuckDb(_)
            | TrimError::EmptyTableName
            | TrimError::InvalidTableIdentifier(_)
            | TrimError::EmptyRowKey
            | TrimError::EmptyPredicate
            | TrimError::EmptyHorizon
            | TrimError::UnsupportedTarget
            | TrimError::DeleteCountExceededDryRun { .. } => TrimResult::NotRun,
        },
        ..TrimReport::default()
    }
}

fn u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}
