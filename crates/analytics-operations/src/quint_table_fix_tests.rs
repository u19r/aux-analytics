use std::cell::RefCell;

use analytics_contract::{StorageItem, StorageStreamRecord, StorageValue};
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::{
    FilesystemRawBackupStore, MaintenanceError, MaintenanceExecutionAuthority,
    MaintenanceExecutionReport, MaintenanceOperations, OperationActor, OperationChunkKey,
    OperationId, OperationStatus, OperationStore, RateLimitPolicy, RawBackupObjectId,
    RawBackupRecord, RawBackupWriteRequest, TableFixConfirmation, TableFixError, TableFixRequest,
    TableFixSelection, TableFixSource, TableFixSourceSnapshot, TableFixTruthRecord,
    TableFixValidation, VerifiedTableFixDestination, VersionedTableFixCurrentSource,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum FixCase {
    DryRunRawBackup,
    ApplyCurrentSource,
    ApplyTwoCurrentSource,
    EmptySelectionRejected,
    MissingConfirmationRejected,
    OversizedSelectionRejected,
    PostFixValidationRejected,
    StaleTruthRejected,
    VerificationFailedRejected,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct FixReport {
    selected_rows: i64,
    repaired_rows: i64,
    destination_mutated: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum FixOutcome {
    NotRun,
    DryRun(FixReport),
    Applied(FixReport),
    RejectedEmptySelection,
    RejectedMissingConfirmation,
    RejectedRepairSetTooLarge,
    RejectedPostFixValidation,
    RejectedStaleTruth,
    RejectedDestinationVerification,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct TableFixMbtState {
    last_outcome: FixOutcome,
    operation_persisted: bool,
    report_persisted: bool,
    operation_succeeded: bool,
    destination_mutated: bool,
}

impl State<TableFixDriver> for TableFixMbtState {
    fn from_driver(driver: &TableFixDriver) -> Result<Self> {
        Ok(Self {
            last_outcome: driver.last_outcome,
            operation_persisted: driver.operation_persisted,
            report_persisted: driver.report_persisted,
            operation_succeeded: driver.operation_succeeded,
            destination_mutated: driver.destination_mutated,
        })
    }
}

struct TableFixDriver {
    last_outcome: FixOutcome,
    operation_persisted: bool,
    report_persisted: bool,
    operation_succeeded: bool,
    destination_mutated: bool,
    counter: u64,
}

impl Default for TableFixDriver {
    fn default() -> Self {
        Self {
            last_outcome: FixOutcome::NotRun,
            operation_persisted: false,
            report_persisted: false,
            operation_succeeded: false,
            destination_mutated: false,
            counter: 0,
        }
    }
}

impl Driver for TableFixDriver {
    type State = TableFixMbtState;

    #[allow(non_snake_case)]
    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => *self = Self::default(),
            FixScenario(fixCase: FixCase) => {
                let observation = self.run_fix_case(fixCase)?;
                self.record(observation)
            },
            step(fixCase: FixCase?) => {
                if let Some(fix_case) = fixCase {
                    let observation = self.run_fix_case(fix_case)?;
                    self.record(observation)
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
fn given_quint_table_fix_model_when_replayed_then_full_state_matches() -> impl Driver {
    TableFixDriver::default()
}

#[derive(Clone, Copy)]
struct FixObservation {
    outcome: FixOutcome,
    operation_persisted: bool,
    report_persisted: bool,
    operation_succeeded: bool,
    destination_mutated: bool,
}

struct CurrentSourceScenario {
    dry_run: bool,
    stale: bool,
    max_rows: u64,
    with_confirmation: bool,
    keys: Vec<Vec<u8>>,
    returns_truth: bool,
    verification_succeeds: bool,
    expect_source_read: bool,
}

impl TableFixDriver {
    fn record(&mut self, observation: FixObservation) {
        self.last_outcome = observation.outcome;
        self.operation_persisted = observation.operation_persisted;
        self.report_persisted = observation.report_persisted;
        self.operation_succeeded = observation.operation_succeeded;
        self.destination_mutated = observation.destination_mutated;
    }

    fn run_fix_case(&mut self, fix_case: FixCase) -> Result<FixObservation> {
        self.counter += 1;
        match fix_case {
            FixCase::DryRunRawBackup => self.run_raw_backup(),
            FixCase::ApplyCurrentSource => self.run_current_source(CurrentSourceScenario {
                dry_run: false,
                stale: false,
                max_rows: 1,
                with_confirmation: true,
                keys: one_key(),
                returns_truth: true,
                verification_succeeds: true,
                expect_source_read: true,
            }),
            FixCase::ApplyTwoCurrentSource => self.run_current_source(CurrentSourceScenario {
                dry_run: false,
                stale: false,
                max_rows: 2,
                with_confirmation: true,
                keys: two_keys(),
                returns_truth: true,
                verification_succeeds: true,
                expect_source_read: true,
            }),
            FixCase::EmptySelectionRejected => self.run_current_source(CurrentSourceScenario {
                dry_run: false,
                stale: false,
                max_rows: 1,
                with_confirmation: false,
                keys: Vec::new(),
                returns_truth: true,
                verification_succeeds: true,
                expect_source_read: false,
            }),
            FixCase::MissingConfirmationRejected => {
                self.run_current_source(CurrentSourceScenario {
                    dry_run: false,
                    stale: false,
                    max_rows: 1,
                    with_confirmation: false,
                    keys: one_key(),
                    returns_truth: true,
                    verification_succeeds: true,
                    expect_source_read: false,
                })
            }
            FixCase::OversizedSelectionRejected => self.run_current_source(CurrentSourceScenario {
                dry_run: false,
                stale: false,
                max_rows: 0,
                with_confirmation: true,
                keys: one_key(),
                returns_truth: true,
                verification_succeeds: true,
                expect_source_read: true,
            }),
            FixCase::PostFixValidationRejected => self.run_current_source(CurrentSourceScenario {
                dry_run: false,
                stale: false,
                max_rows: 1,
                with_confirmation: true,
                keys: one_key(),
                returns_truth: false,
                verification_succeeds: true,
                expect_source_read: true,
            }),
            FixCase::StaleTruthRejected => self.run_current_source(CurrentSourceScenario {
                dry_run: false,
                stale: true,
                max_rows: 1,
                with_confirmation: true,
                keys: one_key(),
                returns_truth: true,
                verification_succeeds: true,
                expect_source_read: true,
            }),
            FixCase::VerificationFailedRejected => self.run_current_source(CurrentSourceScenario {
                dry_run: false,
                stale: false,
                max_rows: 1,
                with_confirmation: true,
                keys: one_key(),
                returns_truth: true,
                verification_succeeds: false,
                expect_source_read: true,
            }),
        }
    }

    fn run_raw_backup(&self) -> Result<FixObservation> {
        let tempdir = tempfile::tempdir()?;
        let source_store = FilesystemRawBackupStore::new(tempdir.path());
        let object_id = RawBackupObjectId::try_new(format!("backup_{}", self.counter))?;
        let index = source_store.write_object(&RawBackupWriteRequest::try_new(
            self.operation_id("backup")?,
            object_id.clone(),
            "users",
            Some("privacy-v1".to_string()),
            vec![RawBackupRecord::try_new(
                "users",
                "user-1",
                None,
                storage_record("user-1"),
            )?],
        )?)?;
        let request = self.raw_backup_request(object_id)?;
        let store = OperationStore::connect_in_memory()?;
        let operations = MaintenanceOperations::new(&store);
        let actor = OperationActor::try_new("quint")?;
        let mut destination = RecordingDestination::new(true, request.operation_id.clone());
        let execution = operations.execute_table_fix_raw_backup_verified(
            &request,
            &actor,
            RateLimitPolicy::default(),
            &[index],
            &source_store,
            &mut destination,
        )?;
        destination.assert_selected_effect(&request.operation_id, &[])?;
        observation_from_report(
            &store,
            &request.operation_id,
            execution,
            destination.was_mutated(),
        )
    }

    fn run_current_source(&self, scenario: CurrentSourceScenario) -> Result<FixObservation> {
        let mut request = self.current_source_request(
            scenario.dry_run,
            scenario.keys.clone(),
            scenario.max_rows,
        )?;
        if scenario.with_confirmation {
            request.validation.confirmation =
                Some(TableFixConfirmation::try_new("table-fix-confirmed")?);
        }
        let store = OperationStore::connect_in_memory()?;
        let operations = MaintenanceOperations::new(&store);
        let actor = OperationActor::try_new("quint")?;
        let source = FixedCurrentSource::with_truth(
            scenario.stale,
            scenario.returns_truth,
            scenario.keys.clone(),
        );
        let mut destination =
            RecordingDestination::new(scenario.verification_succeeds, request.operation_id.clone());
        let result = operations.execute_table_fix_current_source_verified(
            &request,
            &actor,
            RateLimitPolicy::default(),
            &source,
            &mut destination,
        );
        source.assert_request(scenario.expect_source_read, scenario.keys.as_slice())?;
        let expected_applied_keys: &[Vec<u8>] = match &result {
            Ok(_) if !scenario.dry_run => scenario.keys.as_slice(),
            Err(MaintenanceError::TableFix(TableFixError::DestinationVerificationFailed(_))) => {
                scenario.keys.as_slice()
            }
            _ => &[],
        };
        destination.assert_selected_effect(&request.operation_id, expected_applied_keys)?;
        match result {
            Ok(execution) => observation_from_report(
                &store,
                &request.operation_id,
                execution,
                destination.was_mutated(),
            ),
            Err(MaintenanceError::TableFix(TableFixError::EmptyRowSelection)) => {
                rejected_observation(
                    &store,
                    &request.operation_id,
                    FixOutcome::RejectedEmptySelection,
                    destination.was_mutated(),
                    RejectionExpectation::before_submission(),
                )
            }
            Err(MaintenanceError::TableFix(TableFixError::MissingConfirmationToken)) => {
                rejected_observation(
                    &store,
                    &request.operation_id,
                    FixOutcome::RejectedMissingConfirmation,
                    destination.was_mutated(),
                    RejectionExpectation::before_submission(),
                )
            }
            Err(MaintenanceError::TableFix(TableFixError::RepairSetTooLarge { .. })) => {
                rejected_observation(
                    &store,
                    &request.operation_id,
                    FixOutcome::RejectedRepairSetTooLarge,
                    destination.was_mutated(),
                    RejectionExpectation::after_submission(),
                )
            }
            Err(MaintenanceError::TableFix(TableFixError::PostFixValidationFailed { .. })) => {
                rejected_observation(
                    &store,
                    &request.operation_id,
                    FixOutcome::RejectedPostFixValidation,
                    destination.was_mutated(),
                    RejectionExpectation::after_submission(),
                )
            }
            Err(MaintenanceError::TableFix(TableFixError::StaleSourceTruth)) => {
                rejected_observation(
                    &store,
                    &request.operation_id,
                    FixOutcome::RejectedStaleTruth,
                    destination.was_mutated(),
                    RejectionExpectation::after_submission(),
                )
            }
            Err(MaintenanceError::TableFix(TableFixError::DestinationVerificationFailed(_))) => {
                rejected_observation(
                    &store,
                    &request.operation_id,
                    FixOutcome::RejectedDestinationVerification,
                    destination.was_mutated(),
                    RejectionExpectation::verification_failed(),
                )
            }
            Err(error) => Err(error.into()),
        }
    }

    fn raw_backup_request(
        &self,
        object_id: RawBackupObjectId,
    ) -> crate::TableFixResult<TableFixRequest> {
        Ok(TableFixRequest {
            operation_id: self.operation_id("raw")?,
            table_name: "users".to_string(),
            dry_run: true,
            selection: TableFixSelection::RowKeys { keys: one_key() },
            source: TableFixSource::RawBackup {
                object_ids: vec![object_id],
            },
            validation: TableFixValidation::default(),
        })
    }

    fn current_source_request(
        &self,
        dry_run: bool,
        keys: Vec<Vec<u8>>,
        max_rows: u64,
    ) -> crate::TableFixResult<TableFixRequest> {
        Ok(TableFixRequest {
            operation_id: self.operation_id("current")?,
            table_name: "users".to_string(),
            dry_run,
            selection: TableFixSelection::RowKeys { keys },
            source: TableFixSource::CurrentSource {
                source_identity: "users".to_string(),
            },
            validation: TableFixValidation {
                require_post_fix_check: true,
                max_rows,
                confirmation: None,
            },
        })
    }

    fn operation_id(&self, suffix: &str) -> crate::TableFixResult<OperationId> {
        Ok(OperationId::try_new(format!(
            "table_fix_mbt_{}_{}",
            self.counter, suffix
        ))?)
    }
}

fn observation_from_report(
    store: &OperationStore,
    operation_id: &OperationId,
    execution: MaintenanceExecutionReport,
    destination_mutated: bool,
) -> Result<FixObservation> {
    let MaintenanceExecutionReport::TableFix(returned) = execution else {
        return Err(anyhow::anyhow!(
            "table-fix facade returned a different report kind"
        ));
    };
    let persisted = store.table_fix_report(operation_id)?;
    if persisted != returned {
        return Err(anyhow::anyhow!(
            "table-fix facade report differs from its persisted report"
        ));
    }
    if persisted.destination_mutated != destination_mutated {
        return Err(anyhow::anyhow!(
            "table-fix report mutation flag differs from destination observation"
        ));
    }
    if store.show_operation(operation_id)?.status != OperationStatus::Succeeded {
        return Err(anyhow::anyhow!(
            "table-fix report was returned without a succeeded operation"
        ));
    }
    let report = FixReport {
        selected_rows: i64::try_from(persisted.selected_rows)?,
        repaired_rows: i64::try_from(persisted.repaired_rows)?,
        destination_mutated: persisted.destination_mutated,
    };
    Ok(FixObservation {
        outcome: if persisted.dry_run {
            FixOutcome::DryRun(report)
        } else {
            FixOutcome::Applied(report)
        },
        operation_persisted: operation_exists(store, operation_id)?,
        report_persisted: true,
        operation_succeeded: true,
        destination_mutated,
    })
}

struct RejectionExpectation {
    operation_persisted: bool,
    operation_status: Option<OperationStatus>,
    destination_mutated: bool,
}

impl RejectionExpectation {
    fn before_submission() -> Self {
        Self {
            operation_persisted: false,
            operation_status: None,
            destination_mutated: false,
        }
    }

    fn after_submission() -> Self {
        Self {
            operation_persisted: true,
            operation_status: Some(OperationStatus::Paused),
            destination_mutated: false,
        }
    }

    fn verification_failed() -> Self {
        Self {
            operation_persisted: true,
            operation_status: Some(OperationStatus::Paused),
            destination_mutated: true,
        }
    }
}

fn rejected_observation(
    store: &OperationStore,
    operation_id: &OperationId,
    outcome: FixOutcome,
    destination_mutated: bool,
    expectation: RejectionExpectation,
) -> Result<FixObservation> {
    if destination_mutated != expectation.destination_mutated {
        return Err(anyhow::anyhow!(
            "rejected table-fix destination mutation did not match the typed error boundary"
        ));
    }
    let operation_persisted = operation_exists(store, operation_id)?;
    if operation_persisted != expectation.operation_persisted {
        return Err(anyhow::anyhow!(
            "table-fix operation persistence did not match the rejection boundary"
        ));
    }
    if let Some(expected_status) = expectation.operation_status {
        let operation = store.show_operation(operation_id)?;
        if operation.status != expected_status {
            return Err(anyhow::anyhow!(
                "table-fix rejected operation status did not match the typed error boundary"
            ));
        }
    }
    match store.table_fix_report(operation_id) {
        Ok(_) => Err(anyhow::anyhow!(
            "rejected table-fix request persisted a report"
        )),
        Err(crate::OperationStoreError::OperationNotFound(_)) => Ok(FixObservation {
            outcome,
            operation_persisted,
            report_persisted: false,
            operation_succeeded: false,
            destination_mutated,
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

fn one_key() -> Vec<Vec<u8>> {
    vec![b"user-1".to_vec()]
}

fn two_keys() -> Vec<Vec<u8>> {
    vec![b"user-1".to_vec(), b"user-2".to_vec()]
}

fn storage_record(record_key: &str) -> StorageStreamRecord {
    StorageStreamRecord {
        keys: StorageItem::from_iter([("pk".to_string(), StorageValue::S(record_key.to_string()))]),
        sequence_number: record_key.to_string(),
        old_image: None,
        new_image: Some(StorageItem::from_iter([(
            "pk".to_string(),
            StorageValue::S(record_key.to_string()),
        )])),
    }
}

const EXPECTED_SOURCE_IDENTITY: &str = "users";
const EXPECTED_TABLE_NAME: &str = "users";
const EXPECTED_RECORD_KEY: &[u8] = b"user-1";
const EXPECTED_SOURCE_REVISION: &str = "quint-current-source";

#[derive(Debug, PartialEq, Eq)]
struct SourceRead {
    source_identity: String,
    table_name: String,
    row_keys: Vec<Vec<u8>>,
}

struct FixedCurrentSource {
    stale: bool,
    returns_truth: bool,
    expected_keys: Vec<Vec<u8>>,
    reads: RefCell<Vec<SourceRead>>,
}

impl FixedCurrentSource {
    fn new(stale: bool) -> Self {
        Self::with_truth(stale, true, one_key())
    }

    fn with_truth(stale: bool, returns_truth: bool, expected_keys: Vec<Vec<u8>>) -> Self {
        Self {
            stale,
            returns_truth,
            expected_keys,
            reads: RefCell::new(Vec::new()),
        }
    }

    fn assert_request(&self, expect_read: bool, expected_keys: &[Vec<u8>]) -> Result<()> {
        let reads = self.reads.borrow();
        if !expect_read {
            if !reads.is_empty() {
                return Err(anyhow::anyhow!(
                    "table-fix source was read before the request passed validation: {reads:?}"
                ));
            }
            return Ok(());
        }
        let Some(read) = reads.first() else {
            return Err(anyhow::anyhow!(
                "table-fix source did not receive the selected-row read"
            ));
        };
        if reads.len() != 1
            || read.source_identity != EXPECTED_SOURCE_IDENTITY
            || read.table_name != EXPECTED_TABLE_NAME
            || read.row_keys != expected_keys
        {
            return Err(anyhow::anyhow!(
                "table-fix source request did not preserve exact identity and selection"
            ));
        }
        Ok(())
    }
}

impl VersionedTableFixCurrentSource for FixedCurrentSource {
    fn read_truth_snapshot(
        &self,
        source_identity: &str,
        table_name: &str,
        row_keys: &[Vec<u8>],
    ) -> crate::TableFixResult<TableFixSourceSnapshot> {
        if source_identity != EXPECTED_SOURCE_IDENTITY
            || table_name != EXPECTED_TABLE_NAME
            || row_keys != self.expected_keys.as_slice()
        {
            return Err(TableFixError::UnsupportedSource);
        }
        self.reads.borrow_mut().push(SourceRead {
            source_identity: source_identity.to_string(),
            table_name: table_name.to_string(),
            row_keys: row_keys.to_vec(),
        });
        Ok(TableFixSourceSnapshot {
            source_revision: EXPECTED_SOURCE_REVISION.to_string(),
            records: if self.returns_truth {
                self.expected_keys
                    .iter()
                    .map(|record_key| TableFixTruthRecord {
                        table_name: EXPECTED_TABLE_NAME.to_string(),
                        record_key: record_key.clone(),
                        record: storage_record(String::from_utf8_lossy(record_key).as_ref()),
                        stale: self.stale,
                    })
                    .collect()
            } else {
                Vec::new()
            },
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
struct AppliedRepair {
    key: OperationChunkKey,
    table_name: String,
    record_key: Vec<u8>,
    record: StorageStreamRecord,
    source_revision: String,
}

struct RecordingDestination {
    applied: Vec<AppliedRepair>,
    verification_succeeds: bool,
    operation_id: OperationId,
}

impl RecordingDestination {
    fn new(verification_succeeds: bool, operation_id: OperationId) -> Self {
        Self {
            applied: Vec::new(),
            verification_succeeds,
            operation_id,
        }
    }

    fn was_mutated(&self) -> bool {
        !self.applied.is_empty()
    }

    fn assert_selected_effect(
        &self,
        operation_id: &OperationId,
        expected_keys: &[Vec<u8>],
    ) -> Result<()> {
        if self.applied.len() != expected_keys.len()
            || self
                .applied
                .iter()
                .zip(expected_keys)
                .any(|(applied, expected_key)| {
                    applied.table_name != EXPECTED_TABLE_NAME
                        || applied.record_key != *expected_key
                        || applied.record
                            != storage_record(String::from_utf8_lossy(expected_key).as_ref())
                        || applied.source_revision != EXPECTED_SOURCE_REVISION
                        || applied.key
                            != OperationChunkKey::for_repair(
                                operation_id,
                                EXPECTED_TABLE_NAME,
                                expected_key,
                            )
                })
        {
            return Err(anyhow::anyhow!(
                "table-fix destination did not apply exactly the selected rows"
            ));
        }
        Ok(())
    }
}

impl VerifiedTableFixDestination for RecordingDestination {
    fn apply_repair_with_key(
        &mut self,
        _authority: &MaintenanceExecutionAuthority,
        key: &OperationChunkKey,
        table_name: &str,
        record_key: &[u8],
        record: StorageStreamRecord,
        expected_source_revision: &str,
    ) -> crate::TableFixResult<crate::TableFixApplyOutcome> {
        self.applied.push(AppliedRepair {
            key: key.clone(),
            table_name: table_name.to_string(),
            record_key: record_key.to_vec(),
            record,
            source_revision: expected_source_revision.to_string(),
        });
        Ok(crate::TableFixApplyOutcome::Applied)
    }

    fn verify_repair(
        &self,
        _authority: &MaintenanceExecutionAuthority,
        key: &OperationChunkKey,
        table_name: &str,
        record_key: &[u8],
        expected: &StorageStreamRecord,
        expected_source_revision: &str,
    ) -> crate::TableFixResult<bool> {
        let Some(applied) = self.applied.last() else {
            return Ok(false);
        };
        let tuple_matches = applied.table_name == table_name
            && applied.record_key == record_key
            && applied.record.eq(expected)
            && applied.source_revision == expected_source_revision
            && applied.key.as_str() == key.as_str()
            && applied.key
                == OperationChunkKey::for_repair(&self.operation_id, table_name, record_key);
        Ok(self.verification_succeeds && tuple_matches)
    }
}

#[test]
fn given_current_source_when_identity_or_selection_differs_then_request_is_rejected() {
    let source = FixedCurrentSource::new(false);
    let keys = one_key();

    assert!(matches!(
        source.read_truth_snapshot("other", EXPECTED_TABLE_NAME, keys.as_slice()),
        Err(TableFixError::UnsupportedSource)
    ));
    assert!(matches!(
        source.read_truth_snapshot(EXPECTED_SOURCE_IDENTITY, "orders", keys.as_slice()),
        Err(TableFixError::UnsupportedSource)
    ));
    assert!(matches!(
        source.read_truth_snapshot(
            EXPECTED_SOURCE_IDENTITY,
            EXPECTED_TABLE_NAME,
            &[b"user-2".to_vec()]
        ),
        Err(TableFixError::UnsupportedSource)
    ));
    assert!(
        source
            .read_truth_snapshot(
                EXPECTED_SOURCE_IDENTITY,
                EXPECTED_TABLE_NAME,
                keys.as_slice()
            )
            .is_ok()
    );
}

#[test]
fn given_recorded_repair_when_tuple_changes_then_verification_is_rejected() -> Result<()> {
    let operation_id = OperationId::try_new("table_fix_tuple")?;
    let key =
        OperationChunkKey::for_repair(&operation_id, EXPECTED_TABLE_NAME, EXPECTED_RECORD_KEY);
    let record = storage_record("user-1");
    let mut destination = RecordingDestination::new(true, operation_id.clone());
    let authority = MaintenanceExecutionAuthority::try_new(operation_id.clone(), 1)?;

    destination.apply_repair_with_key(
        &authority,
        &key,
        EXPECTED_TABLE_NAME,
        EXPECTED_RECORD_KEY,
        record.clone(),
        "source-revision",
    )?;

    assert!(destination.verify_repair(
        &authority,
        &key,
        EXPECTED_TABLE_NAME,
        EXPECTED_RECORD_KEY,
        &record,
        "source-revision",
    )?);
    assert!(!destination.verify_repair(
        &authority,
        &key,
        "orders",
        EXPECTED_RECORD_KEY,
        &record,
        "source-revision",
    )?);
    assert!(!destination.verify_repair(
        &authority,
        &key,
        EXPECTED_TABLE_NAME,
        b"user-2",
        &record,
        "source-revision",
    )?);
    assert!(!destination.verify_repair(
        &authority,
        &key,
        EXPECTED_TABLE_NAME,
        EXPECTED_RECORD_KEY,
        &storage_record("user-2"),
        "source-revision",
    )?);
    assert!(!destination.verify_repair(
        &authority,
        &key,
        EXPECTED_TABLE_NAME,
        EXPECTED_RECORD_KEY,
        &record,
        "source-revision-2",
    )?);

    let other_operation_id = OperationId::try_new("table_fix_other_tuple")?;
    let other_key = OperationChunkKey::for_repair(
        &other_operation_id,
        EXPECTED_TABLE_NAME,
        EXPECTED_RECORD_KEY,
    );
    let mut wrong_operation_destination = RecordingDestination::new(true, operation_id.clone());
    let other_authority = MaintenanceExecutionAuthority::try_new(other_operation_id, 1)?;
    wrong_operation_destination.apply_repair_with_key(
        &other_authority,
        &other_key,
        EXPECTED_TABLE_NAME,
        EXPECTED_RECORD_KEY,
        record,
        "source-revision",
    )?;
    assert!(!wrong_operation_destination.verify_repair(
        &other_authority,
        &other_key,
        EXPECTED_TABLE_NAME,
        EXPECTED_RECORD_KEY,
        &storage_record("user-1"),
        "source-revision",
    )?);
    assert!(
        wrong_operation_destination
            .assert_selected_effect(&operation_id, &[EXPECTED_RECORD_KEY.to_vec()])
            .is_err()
    );
    Ok(())
}

#[test]
fn given_facade_two_row_apply_when_executed_then_selected_effect_is_complete() -> Result<()> {
    let store = OperationStore::connect_in_memory()?;
    let keys = two_keys();
    let request = TableFixRequest {
        operation_id: OperationId::try_new("table_fix_facade_two_row_apply")?,
        table_name: EXPECTED_TABLE_NAME.to_string(),
        dry_run: false,
        selection: TableFixSelection::RowKeys { keys: keys.clone() },
        source: TableFixSource::CurrentSource {
            source_identity: EXPECTED_SOURCE_IDENTITY.to_string(),
        },
        validation: TableFixValidation {
            require_post_fix_check: true,
            max_rows: 2,
            confirmation: Some(TableFixConfirmation::try_new("table-fix-confirmed")?),
        },
    };
    let operations = MaintenanceOperations::new(&store);
    let actor = OperationActor::try_new("quint")?;
    let source = FixedCurrentSource::with_truth(false, true, keys.clone());
    let mut destination = RecordingDestination::new(true, request.operation_id.clone());
    let execution = operations.execute_table_fix_current_source_verified(
        &request,
        &actor,
        RateLimitPolicy::default(),
        &source,
        &mut destination,
    )?;

    source.assert_request(true, keys.as_slice())?;
    destination.assert_selected_effect(&request.operation_id, keys.as_slice())?;
    let MaintenanceExecutionReport::TableFix(report) = execution else {
        return Err(anyhow::anyhow!(
            "table-fix facade returned a different report kind"
        ));
    };
    assert_eq!(report.selected_rows, 2);
    assert_eq!(report.repaired_rows, 2);
    assert_eq!(report.missing_rows, 0);
    assert!(report.destination_mutated);
    assert_eq!(report.sample_keys, vec!["user-1", "user-2"]);
    assert_eq!(store.table_fix_report(&request.operation_id)?, report);
    assert_eq!(
        store.show_operation(&request.operation_id)?.status,
        OperationStatus::Succeeded
    );
    Ok(())
}

#[test]
fn given_facade_verification_failure_when_executed_then_operation_pauses_without_report()
-> Result<()> {
    let store = OperationStore::connect_in_memory()?;
    let request = TableFixRequest {
        operation_id: OperationId::try_new("table_fix_facade_verification_failure")?,
        table_name: EXPECTED_TABLE_NAME.to_string(),
        dry_run: false,
        selection: TableFixSelection::RowKeys { keys: one_key() },
        source: TableFixSource::CurrentSource {
            source_identity: EXPECTED_SOURCE_IDENTITY.to_string(),
        },
        validation: TableFixValidation {
            require_post_fix_check: true,
            max_rows: 1,
            confirmation: Some(TableFixConfirmation::try_new("table-fix-confirmed")?),
        },
    };
    let operations = MaintenanceOperations::new(&store);
    let actor = OperationActor::try_new("quint")?;
    let source = FixedCurrentSource::new(false);
    let mut destination = RecordingDestination::new(false, request.operation_id.clone());
    let result = operations.execute_table_fix_current_source_verified(
        &request,
        &actor,
        RateLimitPolicy::default(),
        &source,
        &mut destination,
    );

    source.assert_request(true, one_key().as_slice())?;
    destination.assert_selected_effect(&request.operation_id, one_key().as_slice())?;
    assert!(matches!(
        result,
        Err(MaintenanceError::TableFix(TableFixError::DestinationVerificationFailed(key)))
            if key == "user-1"
    ));
    assert_eq!(
        store.show_operation(&request.operation_id)?.status,
        OperationStatus::Paused
    );
    assert!(matches!(
        store.table_fix_report(&request.operation_id),
        Err(crate::OperationStoreError::OperationNotFound(_))
    ));
    Ok(())
}
