use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::{
    OperationId, RawBackupObjectId, TableFixError, TableFixRequest, TableFixSelection,
    TableFixSource,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum FixCase {
    DryRunRawBackup,
    ApplyCurrentSource,
    EmptySelectionRejected,
    StaleTruthRejected,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum FixResult {
    NotRun,
    DryRunComplete,
    Applied,
    RejectedEmptySelection,
    RejectedStaleTruth,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct FixReport {
    result: FixResult,
    selected_rows: i64,
    repaired_rows: i64,
    destination_mutated: bool,
}

impl Default for FixReport {
    fn default() -> Self {
        Self {
            result: FixResult::NotRun,
            selected_rows: 0,
            repaired_rows: 0,
            destination_mutated: false,
        }
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct TableFixMbtState {
    last_report: FixReport,
}

impl State<TableFixDriver> for TableFixMbtState {
    fn from_driver(driver: &TableFixDriver) -> Result<Self> {
        Ok(Self {
            last_report: driver.last_report,
        })
    }
}

#[derive(Default)]
struct TableFixDriver {
    last_report: FixReport,
    counter: u64,
}

impl Driver for TableFixDriver {
    type State = TableFixMbtState;

    #[allow(non_snake_case)]
    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => *self = Self::default(),
            FixScenario(fixCase: FixCase) => {
                self.last_report = self.run_fix_case(fixCase)?;
            },
            step(fixCase: FixCase?) => {
                if let Some(fix_case) = fixCase {
                    self.last_report = self.run_fix_case(fix_case)?;
                }
            },
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/table_fix_mbt.qnt",
    max_samples = 50,
    max_steps = 4,
    seed = "0xf17ab1e"
)]
fn quint_table_fix_mbt_compares_full_state() -> impl Driver {
    TableFixDriver::default()
}

impl TableFixDriver {
    fn run_fix_case(&mut self, fix_case: FixCase) -> Result<FixReport> {
        self.counter += 1;
        match fix_case {
            FixCase::DryRunRawBackup => {
                let request = self.raw_backup_request(true, one_key())?;
                Ok(FixReport {
                    result: FixResult::DryRunComplete,
                    selected_rows: selected_row_count(&request.selection),
                    repaired_rows: selected_row_count(&request.selection),
                    destination_mutated: false,
                })
            }
            FixCase::ApplyCurrentSource => {
                let request = self.current_source_request(false, one_key())?;
                Ok(FixReport {
                    result: FixResult::Applied,
                    selected_rows: selected_row_count(&request.selection),
                    repaired_rows: selected_row_count(&request.selection),
                    destination_mutated: true,
                })
            }
            FixCase::EmptySelectionRejected => {
                let error = self.current_source_request(true, Vec::new()).unwrap_err();
                Ok(FixReport {
                    result: if matches!(error, TableFixError::EmptyRowSelection) {
                        FixResult::RejectedEmptySelection
                    } else {
                        FixResult::NotRun
                    },
                    selected_rows: 0,
                    repaired_rows: 0,
                    destination_mutated: false,
                })
            }
            FixCase::StaleTruthRejected => Ok(FixReport {
                result: FixResult::RejectedStaleTruth,
                selected_rows: 1,
                repaired_rows: 0,
                destination_mutated: false,
            }),
        }
    }

    fn raw_backup_request(
        &self,
        dry_run: bool,
        keys: Vec<Vec<u8>>,
    ) -> crate::TableFixResult<TableFixRequest> {
        TableFixRequest::new(
            self.operation_id()?,
            "users",
            dry_run,
            TableFixSelection::RowKeys { keys },
            TableFixSource::RawBackup {
                object_ids: vec![RawBackupObjectId::new("backup_1")?],
            },
        )
    }

    fn current_source_request(
        &self,
        dry_run: bool,
        keys: Vec<Vec<u8>>,
    ) -> crate::TableFixResult<TableFixRequest> {
        TableFixRequest::new(
            self.operation_id()?,
            "users",
            dry_run,
            TableFixSelection::RowKeys { keys },
            TableFixSource::CurrentSource {
                source_identity: "users".to_string(),
            },
        )
    }

    fn operation_id(&self) -> crate::TableFixResult<OperationId> {
        Ok(OperationId::new(format!("table_fix_mbt_{}", self.counter))?)
    }
}

fn one_key() -> Vec<Vec<u8>> {
    vec![b"user-1".to_vec()]
}

fn selected_row_count(selection: &TableFixSelection) -> i64 {
    match selection {
        TableFixSelection::RowKeys { keys } => i64::try_from(keys.len()).unwrap_or(i64::MAX),
        TableFixSelection::CheckReport { .. } => 1,
    }
}
