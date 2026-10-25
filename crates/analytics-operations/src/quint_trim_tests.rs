use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::{
    DuckDbTrimRows, MaintenanceError, MaintenanceExecutionReport, MaintenanceOperations,
    OperationActor, OperationChunkKey, OperationId, OperationStore, RateLimitPolicy,
    TrimCandidateSnapshot, TrimCandidateSource, TrimDeletionOutcome, TrimDestination, TrimError,
    TrimPredicate, TrimPredicateValue, TrimRequest, TrimTarget,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum TrimCase {
    DryRunRowKeys,
    ApplyWithoutConfirmation,
    ApplyRowKeysWithConfirmation,
    ApplyDeletesTooManyRows,
    ApplyStaleCandidates,
    EmptySelectionRejected,
    UnsafePredicateRejected,
    RowPredicateApply,
    FullTableDropApply,
    HorizonTrimApply,
    SnapshotExpiryUnsupported,
    RawBackupExpirationUnsupported,
    SchemaUnsupported,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct TrimReport {
    candidate_rows: i64,
    rows_deleted: i64,
    destination_mutated: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum TrimOutcome {
    NotRun,
    DryRun(TrimReport),
    Applied(TrimReport),
    RejectedMissingConfirmation,
    RejectedUnsupportedTarget,
    RejectedDeleteCountExceeded,
    RejectedStaleCandidate,
    RejectedSchemaUnsupported,
    RejectedEmptySelection,
    RejectedUnsafePredicate,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct TrimMbtState {
    last_outcome: TrimOutcome,
    operation_persisted: bool,
    report_persisted: bool,
    destination_mutated: bool,
}

impl State<TrimDriver> for TrimMbtState {
    fn from_driver(driver: &TrimDriver) -> Result<Self> {
        Ok(Self {
            last_outcome: driver.last_outcome,
            operation_persisted: driver.operation_persisted,
            report_persisted: driver.report_persisted,
            destination_mutated: driver.destination_mutated,
        })
    }
}

struct TrimDriver {
    last_outcome: TrimOutcome,
    operation_persisted: bool,
    report_persisted: bool,
    destination_mutated: bool,
    counter: u64,
}

impl Default for TrimDriver {
    fn default() -> Self {
        Self {
            last_outcome: TrimOutcome::NotRun,
            operation_persisted: false,
            report_persisted: false,
            destination_mutated: false,
            counter: 0,
        }
    }
}

impl Driver for TrimDriver {
    type State = TrimMbtState;

    #[allow(non_snake_case)]
    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => *self = Self::default(),
            TrimScenario(trimCase: TrimCase) => {
                let observation = self.run_trim_case(trimCase)?;
                self.record(observation)
            },
            step(trimCase: TrimCase?) => {
                if let Some(trim_case) = trimCase {
                    let observation = self.run_trim_case(trim_case)?;
                    self.record(observation)
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
fn given_quint_trim_model_when_replayed_then_full_state_matches() -> impl Driver {
    TrimDriver::default()
}

#[derive(Clone, Copy)]
struct TrimObservation {
    outcome: TrimOutcome,
    operation_persisted: bool,
    report_persisted: bool,
    destination_mutated: bool,
}

impl TrimDriver {
    fn record(&mut self, observation: TrimObservation) {
        self.last_outcome = observation.outcome;
        self.operation_persisted = observation.operation_persisted;
        self.report_persisted = observation.report_persisted;
        self.destination_mutated = observation.destination_mutated;
    }

    fn run_trim_case(&mut self, trim_case: TrimCase) -> Result<TrimObservation> {
        self.counter += 1;
        match trim_case {
            TrimCase::DryRunRowKeys => {
                self.run_real(true, row_keys_target(2), None, TrimFixture::Complete)
            }
            TrimCase::ApplyWithoutConfirmation => self.run_without_confirmation(),
            TrimCase::ApplyRowKeysWithConfirmation => self.run_real(
                false,
                row_keys_target(1),
                Some("trim-confirmed"),
                TrimFixture::Complete,
            ),
            TrimCase::ApplyDeletesTooManyRows => self.run_overcount(),
            TrimCase::ApplyStaleCandidates => self.run_stale(),
            TrimCase::EmptySelectionRejected => self.run_empty_selection(),
            TrimCase::UnsafePredicateRejected => self.run_unsafe_predicate(),
            TrimCase::RowPredicateApply => self.run_real(
                false,
                TrimTarget::RowPredicate {
                    predicate: TrimPredicate::Eq {
                        column: "__id".to_string(),
                        value: TrimPredicateValue::Text("757365722d31".to_string()),
                    },
                },
                Some("trim-confirmed"),
                TrimFixture::Complete,
            ),
            TrimCase::FullTableDropApply => self.run_real(
                false,
                TrimTarget::FullTableDrop,
                Some("trim-confirmed"),
                TrimFixture::Complete,
            ),
            TrimCase::HorizonTrimApply => self.run_real(
                false,
                TrimTarget::HorizonTrim {
                    older_than_ms: 1_000,
                },
                Some("trim-confirmed"),
                TrimFixture::Complete,
            ),
            TrimCase::SnapshotExpiryUnsupported => self.run_real(
                false,
                TrimTarget::DuckLakeSnapshotExpiry {
                    older_than_ms: 1_000,
                },
                Some("trim-confirmed"),
                TrimFixture::Complete,
            ),
            TrimCase::RawBackupExpirationUnsupported => self.run_real(
                false,
                TrimTarget::RawBackupExpiration {
                    older_than_ms: 1_000,
                },
                Some("trim-confirmed"),
                TrimFixture::Complete,
            ),
            TrimCase::SchemaUnsupported => self.run_real(
                false,
                row_keys_target(1),
                Some("trim-confirmed"),
                TrimFixture::MissingId,
            ),
        }
    }

    fn run_without_confirmation(&self) -> Result<TrimObservation> {
        self.run_real(false, row_keys_target(1), None, TrimFixture::Complete)
    }

    fn run_empty_selection(&self) -> Result<TrimObservation> {
        self.run_real(
            false,
            TrimTarget::RowKeys { keys: Vec::new() },
            None,
            TrimFixture::Complete,
        )
    }

    fn run_unsafe_predicate(&self) -> Result<TrimObservation> {
        self.run_real(
            true,
            TrimTarget::RowPredicate {
                predicate: TrimPredicate::Eq {
                    column: "email; drop table users".to_string(),
                    value: TrimPredicateValue::Text("one@example.com".to_string()),
                },
            },
            None,
            TrimFixture::Complete,
        )
    }

    fn run_real(
        &self,
        dry_run: bool,
        target: TrimTarget,
        confirmation_token: Option<&str>,
        fixture: TrimFixture,
    ) -> Result<TrimObservation> {
        let tempdir = seed_trim_database(fixture)?;
        let database_path = database_path(&tempdir);
        let before = inspect_trim_table(database_path.as_str())?;
        let request = self.request(dry_run, target, confirmation_token.map(str::to_string))?;
        let store = OperationStore::connect_in_memory()?;
        let actor = OperationActor::try_new("quint")?;
        let source = DuckDbTrimRows::new(database_path.clone());
        let mut destination = DuckDbTrimRows::new(database_path.clone());
        observe_trim_execution(
            &request,
            &store,
            &actor,
            &source,
            &mut destination,
            database_path.as_str(),
            &before,
        )
    }

    fn run_stale(&self) -> Result<TrimObservation> {
        let tempdir = seed_trim_database(TrimFixture::Complete)?;
        let database_path = database_path(&tempdir);
        let target = row_keys_target(1);
        let real_source = DuckDbTrimRows::new(database_path.clone());
        let stale_snapshot = real_source.candidate_snapshot("users", &target)?;
        anyhow::ensure!(
            stale_snapshot.candidate_rows() > 0,
            "stale trim fixture must contain candidates"
        );
        let connection = duckdb::Connection::open(database_path.as_str())?;
        connection.execute(
            "DELETE FROM users WHERE __id = ?",
            duckdb::params!["757365722d31"],
        )?;
        drop(connection);
        let before = inspect_trim_table(database_path.as_str())?;
        anyhow::ensure!(
            before.rows == Some(1),
            "stale trim fixture must remove exactly the snapshotted row before execution"
        );
        let request = self.request(false, target, Some("trim-confirmed".to_string()))?;
        let store = OperationStore::connect_in_memory()?;
        let actor = OperationActor::try_new("quint")?;
        let source = StaleSnapshotSource {
            snapshot: stale_snapshot,
        };
        let mut destination = DuckDbTrimRows::new(database_path.clone());
        observe_trim_execution(
            &request,
            &store,
            &actor,
            &source,
            &mut destination,
            database_path.as_str(),
            &before,
        )
    }

    fn run_overcount(&self) -> Result<TrimObservation> {
        let tempdir = seed_trim_database(TrimFixture::Complete)?;
        let database_path = database_path(&tempdir);
        let before = inspect_trim_table(database_path.as_str())?;
        let request = self.request(
            false,
            row_keys_target(1),
            Some("trim-confirmed".to_string()),
        )?;
        let store = OperationStore::connect_in_memory()?;
        let actor = OperationActor::try_new("quint")?;
        let source = DuckDbTrimRows::new(database_path.clone());
        let mut destination = OvercountingTrimDestination;
        observe_trim_execution(
            &request,
            &store,
            &actor,
            &source,
            &mut destination,
            database_path.as_str(),
            &before,
        )
    }

    fn request(
        &self,
        dry_run: bool,
        target: TrimTarget,
        confirmation_token: Option<String>,
    ) -> crate::TrimResult<TrimRequest> {
        Ok(TrimRequest {
            operation_id: OperationId::try_new(format!("trim_mbt_{}", self.counter))?,
            table_name: "users".to_string(),
            dry_run,
            target,
            confirmation_token,
        })
    }
}

fn observation_from_report(
    store: &OperationStore,
    operation_id: &OperationId,
    execution: MaintenanceExecutionReport,
    destination_mutated: bool,
) -> Result<TrimObservation> {
    let MaintenanceExecutionReport::Trim(returned) = execution else {
        return Err(anyhow::anyhow!(
            "trim facade returned a different report kind"
        ));
    };
    let persisted = store.trim_report(operation_id)?;
    if persisted != returned {
        return Err(anyhow::anyhow!(
            "trim facade report differs from its persisted report"
        ));
    }
    if persisted.destination_mutated != destination_mutated {
        return Err(anyhow::anyhow!(
            "trim report mutation flag differs from destination observation"
        ));
    }
    let report = TrimReport {
        candidate_rows: i64::try_from(persisted.candidate_rows)?,
        rows_deleted: i64::try_from(persisted.rows_deleted)?,
        destination_mutated: persisted.destination_mutated,
    };
    Ok(TrimObservation {
        outcome: if persisted.dry_run {
            TrimOutcome::DryRun(report)
        } else {
            TrimOutcome::Applied(report)
        },
        operation_persisted: operation_exists(store, operation_id)?,
        report_persisted: true,
        destination_mutated,
    })
}

fn rejected_observation(
    store: &OperationStore,
    operation_id: &OperationId,
    outcome: TrimOutcome,
    destination_mutated: bool,
    expect_operation: bool,
) -> Result<TrimObservation> {
    if destination_mutated {
        return Err(anyhow::anyhow!(
            "rejected trim request mutated the destination"
        ));
    }
    let operation_persisted = operation_exists(store, operation_id)?;
    if operation_persisted != expect_operation {
        return Err(anyhow::anyhow!(
            "trim operation persistence did not match the rejection boundary"
        ));
    }
    match store.trim_report(operation_id) {
        Ok(_) => Err(anyhow::anyhow!("rejected trim request persisted a report")),
        Err(crate::OperationStoreError::OperationNotFound(_)) => Ok(TrimObservation {
            outcome,
            operation_persisted,
            report_persisted: false,
            destination_mutated: false,
        }),
        Err(error) => Err(error.into()),
    }
}

fn operation_exists(store: &OperationStore, operation_id: &OperationId) -> Result<bool> {
    let operations = store.list_operations()?;
    Ok(operations
        .iter()
        .any(|operation| &operation.operation_id == operation_id))
}

fn observe_trim_execution<Source, Destination>(
    request: &TrimRequest,
    store: &OperationStore,
    actor: &OperationActor,
    source: &Source,
    destination: &mut Destination,
    database_path: &str,
    before: &TrimTableState,
) -> Result<TrimObservation>
where
    Source: TrimCandidateSource,
    Destination: TrimDestination,
{
    let operations = MaintenanceOperations::new(store);
    let execution = operations.execute_trim(
        request,
        actor,
        RateLimitPolicy::default(),
        source,
        destination,
    );
    let after = inspect_trim_table(database_path)?;
    let destination_mutated = before != &after;
    match execution {
        Ok(execution) => {
            observation_from_report(store, &request.operation_id, execution, destination_mutated)
        }
        Err(MaintenanceError::Trim(TrimError::MissingConfirmationToken)) => rejected_observation(
            store,
            &request.operation_id,
            TrimOutcome::RejectedMissingConfirmation,
            destination_mutated,
            false,
        ),
        Err(MaintenanceError::Trim(TrimError::UnsupportedTarget)) => rejected_observation(
            store,
            &request.operation_id,
            TrimOutcome::RejectedUnsupportedTarget,
            destination_mutated,
            true,
        ),
        Err(MaintenanceError::Trim(TrimError::DeleteCountExceededDryRun { .. })) => {
            rejected_observation(
                store,
                &request.operation_id,
                TrimOutcome::RejectedDeleteCountExceeded,
                destination_mutated,
                true,
            )
        }
        Err(MaintenanceError::Trim(TrimError::StaleCandidateSnapshot)) => rejected_observation(
            store,
            &request.operation_id,
            TrimOutcome::RejectedStaleCandidate,
            destination_mutated,
            true,
        ),
        Err(MaintenanceError::Trim(TrimError::DuckDb(_))) => rejected_observation(
            store,
            &request.operation_id,
            TrimOutcome::RejectedSchemaUnsupported,
            destination_mutated,
            true,
        ),
        Err(MaintenanceError::Trim(TrimError::EmptyRowSelection)) => rejected_observation(
            store,
            &request.operation_id,
            TrimOutcome::RejectedEmptySelection,
            destination_mutated,
            false,
        ),
        Err(MaintenanceError::Trim(TrimError::UnsafePredicate)) => rejected_observation(
            store,
            &request.operation_id,
            TrimOutcome::RejectedUnsafePredicate,
            destination_mutated,
            false,
        ),
        Err(error) => Err(error.into()),
    }
}

#[derive(Clone, Copy)]
enum TrimFixture {
    Complete,
    MissingId,
}

#[derive(Debug, Eq, PartialEq)]
struct TrimTableState {
    columns: Vec<String>,
    rows: Option<u64>,
}

fn seed_trim_database(fixture: TrimFixture) -> Result<tempfile::TempDir> {
    let tempdir = tempfile::tempdir()?;
    let path = tempdir.path().join("analytics.duckdb");
    let connection = duckdb::Connection::open(path)?;
    match fixture {
        TrimFixture::Complete => connection.execute_batch(
            "CREATE TABLE users (__id VARCHAR NOT NULL, email VARCHAR, __expiry BIGINT);
             INSERT INTO users VALUES ('757365722d31', 'one@example.com', 500);
             INSERT INTO users VALUES ('757365722d32', 'two@example.com', 1500);",
        )?,
        TrimFixture::MissingId => connection.execute_batch(
            "CREATE TABLE users (email VARCHAR, __expiry BIGINT);
             INSERT INTO users VALUES ('one@example.com', 500);",
        )?,
    }
    Ok(tempdir)
}

fn database_path(tempdir: &tempfile::TempDir) -> String {
    tempdir
        .path()
        .join("analytics.duckdb")
        .to_string_lossy()
        .into_owned()
}

fn inspect_trim_table(database_path: &str) -> Result<TrimTableState> {
    let connection = duckdb::Connection::open(database_path)?;
    let mut statement = connection.prepare(
        "SELECT column_name
         FROM information_schema.columns
         WHERE table_name = 'users'
         ORDER BY ordinal_position",
    )?;
    let mut rows = statement.query([])?;
    let mut columns = Vec::new();
    while let Some(row) = rows.next()? {
        columns.push(row.get::<_, String>(0)?);
    }
    let row_count = if columns.is_empty() {
        None
    } else {
        let count =
            connection.query_row("SELECT count(*) FROM users", [], |row| row.get::<_, i64>(0))?;
        Some(u64::try_from(count)?)
    };
    Ok(TrimTableState {
        columns,
        rows: row_count,
    })
}

struct StaleSnapshotSource {
    snapshot: TrimCandidateSnapshot,
}

impl TrimCandidateSource for StaleSnapshotSource {
    fn candidate_snapshot(
        &self,
        _table_name: &str,
        _target: &TrimTarget,
    ) -> crate::TrimResult<TrimCandidateSnapshot> {
        Ok(self.snapshot.clone())
    }
}

/// Deliberately violates the destination contract to prove the executor's
/// over-count guard. All real trim cases use `DuckDbTrimRows` directly.
struct OvercountingTrimDestination;

impl TrimDestination for OvercountingTrimDestination {
    fn delete_candidate_snapshot_with_key(
        &mut self,
        _authority: &crate::MaintenanceExecutionAuthority,
        _key: &OperationChunkKey,
        _table_name: &str,
        _target: &TrimTarget,
        expected: &TrimCandidateSnapshot,
    ) -> crate::TrimResult<TrimDeletionOutcome> {
        Ok(TrimDeletionOutcome {
            candidate_rows: expected.candidate_rows(),
            rows_deleted: expected.candidate_rows().saturating_add(1),
        })
    }
}

fn row_keys_target(count: usize) -> TrimTarget {
    TrimTarget::RowKeys {
        keys: (1..=count)
            .map(|index| format!("user-{index}").into_bytes())
            .collect(),
    }
}
