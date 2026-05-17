#![allow(
    clippy::enum_variant_names,
    clippy::fn_params_excessive_bools,
    clippy::match_same_arms,
    clippy::struct_excessive_bools,
    reason = "Quint MBT records intentionally mirror finite boolean model state"
)]

use analytics_quint_test_support::map_result;
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::{
    BackfillPlanError, BackfillPlanner, BackfillRequest, BackfillSourceCapabilities,
    ConservativeBackfillPlanner, DestinationBackend, RateLimitPolicy, SnapshotMethod,
    ValidationMode,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum PlanState {
    Requested,
    SourceInspected,
    DestinationInspected,
    StrategyChosen,
    ChunksEstimated,
    OperationReserved,
    Rejected,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum SourceKind {
    DynamoDb,
    AuxStorage,
    RawBackup,
    LocalFixture,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum ModelSnapshotMethod {
    DynamoDbPitrExport,
    DynamoDbParallelScan,
    AuxStorageEnumeration,
    RawBackupReplay,
    LocalFixtureSnapshot,
    StreamOnly,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum RequestedMethod {
    NoRequestedMethod,
    RequestedDynamoDbPitrExport,
    RequestedDynamoDbParallelScan,
    RequestedAuxStorageEnumeration,
    RequestedRawBackupReplay,
    RequestedLocalFixtureSnapshot,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct PlanningState {
    state: PlanState,
    source_kind: SourceKind,
    has_row_identity: bool,
    supports_pitr_export: bool,
    supports_parallel_scan: bool,
    supports_enumeration: bool,
    supports_raw_replay: bool,
    stream_retention_hours: i64,
    chosen_method: ModelSnapshotMethod,
    requested_method: RequestedMethod,
    chunk_count: i64,
    deletes_can_be_proven: bool,
}

struct PlanningDriver {
    state: PlanningState,
}

impl Default for PlanningDriver {
    fn default() -> Self {
        Self {
            state: PlanningState {
                state: PlanState::Requested,
                source_kind: SourceKind::DynamoDb,
                has_row_identity: true,
                supports_pitr_export: true,
                supports_parallel_scan: true,
                supports_enumeration: false,
                supports_raw_replay: false,
                stream_retention_hours: 24,
                chosen_method: ModelSnapshotMethod::StreamOnly,
                requested_method: RequestedMethod::NoRequestedMethod,
                chunk_count: 0,
                deletes_can_be_proven: false,
            },
        }
    }
}

impl Driver for PlanningDriver {
    type State = ();

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            InspectSource(kind: SourceKind) => {
                self.inspect_source(kind);
            },
            InspectDestination => {
                if self.state.state == PlanState::SourceInspected {
                    self.state.state = PlanState::DestinationInspected;
                }
            },
            ChooseStrategy => {
                self.choose_strategy();
            },
            EstimateChunks => {
                if self.state.state == PlanState::StrategyChosen {
                    self.state.state = PlanState::ChunksEstimated;
                    self.state.chunk_count = 1;
                }
            },
            ReserveOperation => {
                if self.state.state == PlanState::ChunksEstimated {
                    self.state.state = PlanState::OperationReserved;
                }
            },
            MissingRowIdentity => {
                if self.state.state == PlanState::Requested {
                    self.state.has_row_identity = false;
                }
            },
            step(action_choice: String?, kind: SourceKind?) => {
                self.model_step(action_choice.as_deref(), kind);
            },
            RequestMethod(requested: RequestedMethod) => {
                if self.state.state == PlanState::Requested {
                    self.state.requested_method = requested;
                }
            },
        })
    }
}

impl PlanningDriver {
    fn model_step(&mut self, action_choice: Option<&str>, kind: Option<SourceKind>) {
        match action_choice {
            Some("inspect_source") => {
                if let Some(kind) = kind {
                    self.inspect_source(kind);
                }
            }
            Some("inspect_destination") if self.state.state == PlanState::SourceInspected => {
                self.state.state = PlanState::DestinationInspected;
            }
            Some("choose_strategy") => self.choose_strategy(),
            Some("estimate_chunks") if self.state.state == PlanState::StrategyChosen => {
                self.state.state = PlanState::ChunksEstimated;
                self.state.chunk_count = 1;
            }
            Some("reserve_operation") if self.state.state == PlanState::ChunksEstimated => {
                self.state.state = PlanState::OperationReserved;
            }
            Some("missing_row_identity") if self.state.state == PlanState::Requested => {
                self.state.has_row_identity = false;
            }
            Some("request_method") if self.state.state == PlanState::Requested => {}
            _ => {}
        }
    }

    fn inspect_source(&mut self, kind: SourceKind) {
        if self.state.state != PlanState::Requested {
            return;
        }
        self.state.state = PlanState::SourceInspected;
        self.state.source_kind = kind;
        self.state.supports_pitr_export = kind == SourceKind::DynamoDb;
        self.state.supports_parallel_scan = kind == SourceKind::DynamoDb;
        self.state.supports_enumeration =
            matches!(kind, SourceKind::AuxStorage | SourceKind::LocalFixture);
        self.state.supports_raw_replay = kind == SourceKind::RawBackup;
        self.state.stream_retention_hours = if kind == SourceKind::DynamoDb { 24 } else { 0 };
        self.state.deletes_can_be_proven =
            matches!(kind, SourceKind::AuxStorage | SourceKind::LocalFixture);
    }

    fn choose_strategy(&mut self) {
        if self.state.state != PlanState::DestinationInspected {
            return;
        }
        let request = BackfillRequest {
            target_tables: vec!["users".to_string()],
            source: capabilities_for(self.state.source_kind),
            requested_snapshot_method: snapshot_for_requested(self.state.requested_method),
            filters: Vec::new(),
            chunk_target_rows: 100,
            rate_limit: RateLimitPolicy::default(),
            destination_backend: DestinationBackend::DuckDb,
            dry_run: true,
            validation_mode: ValidationMode::CountsAndHashes,
            all_tables_have_row_identity: self.state.has_row_identity,
            estimated_table_rows: 100,
        };
        self.state.chosen_method = map_result(
            ConservativeBackfillPlanner.plan(&request),
            |plan| model_method(plan.snapshot_method),
            |err| match err {
                BackfillPlanError::MissingRowIdentity { .. }
                | BackfillPlanError::UnsafeStreamOnlyBackfill { .. }
                | BackfillPlanError::HistoricalEnumerationUnsupported
                | BackfillPlanError::MissingTargetTables
                | BackfillPlanError::UnsupportedSourceKind(_)
                | BackfillPlanError::CapabilityDiscoveryFailed(_)
                | BackfillPlanError::PartialCapabilitySupport { .. } => {
                    ModelSnapshotMethod::StreamOnly
                }
            },
        );
        self.state.state = if self.state.has_row_identity
            && self.state.chosen_method != ModelSnapshotMethod::StreamOnly
        {
            PlanState::StrategyChosen
        } else {
            PlanState::Rejected
        };
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/backfill_planning.qnt",
    max_samples = 50,
    max_steps = 8,
    seed = "0x515f"
)]
fn quint_backfill_planning_model_matches_planner() -> impl Driver {
    PlanningDriver::default()
}

fn capabilities_for(kind: SourceKind) -> BackfillSourceCapabilities {
    match kind {
        SourceKind::DynamoDb => BackfillSourceCapabilities::dynamodb_export(),
        SourceKind::AuxStorage => BackfillSourceCapabilities::aux_storage(),
        SourceKind::RawBackup => BackfillSourceCapabilities::raw_backup(),
        SourceKind::LocalFixture => BackfillSourceCapabilities::local_fixture(),
    }
}

fn model_method(method: SnapshotMethod) -> ModelSnapshotMethod {
    match method {
        SnapshotMethod::DynamoDbPitrExport => ModelSnapshotMethod::DynamoDbPitrExport,
        SnapshotMethod::DynamoDbParallelScan => ModelSnapshotMethod::DynamoDbParallelScan,
        SnapshotMethod::AuxStorageEnumeration => ModelSnapshotMethod::AuxStorageEnumeration,
        SnapshotMethod::RawBackupReplay => ModelSnapshotMethod::RawBackupReplay,
        SnapshotMethod::LocalFixture => ModelSnapshotMethod::LocalFixtureSnapshot,
    }
}

#[test]
fn backfill_strategy_examples_match_quint_model() {
    let planner = ConservativeBackfillPlanner;
    for (source, expected) in [
        (
            BackfillSourceCapabilities::dynamodb_export(),
            SnapshotMethod::DynamoDbPitrExport,
        ),
        (
            BackfillSourceCapabilities {
                supports_pitr_export: false,
                ..BackfillSourceCapabilities::dynamodb_export()
            },
            SnapshotMethod::DynamoDbParallelScan,
        ),
        (
            BackfillSourceCapabilities::aux_storage(),
            SnapshotMethod::AuxStorageEnumeration,
        ),
        (
            BackfillSourceCapabilities::raw_backup(),
            SnapshotMethod::RawBackupReplay,
        ),
        (
            BackfillSourceCapabilities::local_fixture(),
            SnapshotMethod::LocalFixture,
        ),
    ] {
        let plan = planner
            .plan(&BackfillRequest {
                target_tables: vec!["users".to_string()],
                source,
                requested_snapshot_method: None,
                filters: Vec::new(),
                chunk_target_rows: 100,
                rate_limit: RateLimitPolicy::default(),
                destination_backend: DestinationBackend::DuckDb,
                dry_run: true,
                validation_mode: ValidationMode::CountsAndHashes,
                all_tables_have_row_identity: true,
                estimated_table_rows: 100,
            })
            .expect("plan");
        assert_eq!(plan.snapshot_method, expected);
    }
}

#[test]
fn requested_strategy_examples_match_quint_model() {
    let planner = ConservativeBackfillPlanner;
    let mut wrong_request = BackfillRequest {
        target_tables: vec!["users".to_string()],
        source: BackfillSourceCapabilities::local_fixture(),
        requested_snapshot_method: Some(SnapshotMethod::AuxStorageEnumeration),
        filters: Vec::new(),
        chunk_target_rows: 100,
        rate_limit: RateLimitPolicy::default(),
        destination_backend: DestinationBackend::DuckDb,
        dry_run: true,
        validation_mode: ValidationMode::CountsAndHashes,
        all_tables_have_row_identity: true,
        estimated_table_rows: 100,
    };

    assert!(matches!(
        planner.plan(&wrong_request),
        Err(BackfillPlanError::HistoricalEnumerationUnsupported)
    ));

    wrong_request.requested_snapshot_method = Some(SnapshotMethod::LocalFixture);
    assert_eq!(
        planner.plan(&wrong_request).expect("plan").snapshot_method,
        SnapshotMethod::LocalFixture
    );
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum BughuntSource {
    DynamoDb,
    AuxStorage,
    RawBackup,
    LocalFixture,
    Unsupported,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum BughuntStrategy {
    Export,
    Scan,
    Enumerate,
    Replay,
    Fixture,
    Reject,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum BughuntRequested {
    None,
    WantExport,
    WantScan,
    WantEnumerate,
    WantReplay,
    WantFixture,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct BughuntState {
    source: BughuntSource,
    requested: BughuntRequested,
    has_identity: bool,
    pitr: bool,
    scan: bool,
    enumerate: bool,
    replay: bool,
    strategy: BughuntStrategy,
    reserved: bool,
    unsafe_accepted: bool,
    delete_proof_recorded: bool,
}

struct BughuntDriver {
    state: BughuntState,
}

impl Default for BughuntDriver {
    fn default() -> Self {
        Self {
            state: BughuntState {
                source: BughuntSource::Unsupported,
                requested: BughuntRequested::None,
                has_identity: true,
                pitr: false,
                scan: false,
                enumerate: false,
                replay: false,
                strategy: BughuntStrategy::Reject,
                reserved: false,
                unsafe_accepted: false,
                delete_proof_recorded: false,
            },
        }
    }
}

impl Driver for BughuntDriver {
    type State = ();

    #[allow(non_snake_case)]
    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Discover(source_kind: BughuntSource, identity: bool) => {
                self.discover(source_kind, identity);
            },
            Plan => {
                self.plan();
            },
            step(action_choice: String?, sourceKind: BughuntSource?, identity: bool?) => {
                if action_choice.as_deref() == Some("discover") {
                    let Some(source_kind) = sourceKind else {
                        return Ok(());
                    };
                    self.discover(source_kind, identity.unwrap_or(true));
                } else if action_choice.as_deref() == Some("plan") {
                    self.plan();
                }
            },
            RequestMethod(requestedMethod: BughuntRequested) => {
                self.request_method(requestedMethod);
            },
        })
    }
}

impl BughuntDriver {
    fn discover(&mut self, source: BughuntSource, has_identity: bool) {
        self.state.source = source;
        self.state.has_identity = has_identity;
        self.state.pitr = source == BughuntSource::DynamoDb;
        self.state.scan = source == BughuntSource::DynamoDb;
        self.state.enumerate = matches!(
            source,
            BughuntSource::AuxStorage | BughuntSource::LocalFixture
        );
        self.state.replay = source == BughuntSource::RawBackup;
        self.state.strategy = BughuntStrategy::Reject;
        self.state.reserved = false;
        self.state.unsafe_accepted = false;
        self.state.delete_proof_recorded = matches!(
            source,
            BughuntSource::AuxStorage | BughuntSource::LocalFixture
        );
    }

    fn request_method(&mut self, requested: BughuntRequested) {
        self.state.requested = requested;
        self.state.strategy = BughuntStrategy::Reject;
        self.state.reserved = false;
        self.state.unsafe_accepted = false;
    }

    fn plan(&mut self) {
        let Some(source) = self.source_capabilities() else {
            self.state.strategy = BughuntStrategy::Reject;
            self.state.reserved = false;
            return;
        };
        let request = BackfillRequest {
            target_tables: vec!["users".to_string()],
            source,
            requested_snapshot_method: snapshot_for_bughunt_requested(self.state.requested),
            filters: Vec::new(),
            chunk_target_rows: 100,
            rate_limit: RateLimitPolicy::default(),
            destination_backend: DestinationBackend::DuckDb,
            dry_run: true,
            validation_mode: ValidationMode::CountsAndHashes,
            all_tables_have_row_identity: self.state.has_identity,
            estimated_table_rows: 100,
        };
        let result = ConservativeBackfillPlanner.plan(&request);
        self.state.strategy = map_result(
            result,
            |plan| bughunt_strategy(plan.snapshot_method),
            |_| BughuntStrategy::Reject,
        );
        self.state.reserved =
            self.state.has_identity && self.state.strategy != BughuntStrategy::Reject;
    }

    fn source_capabilities(&self) -> Option<BackfillSourceCapabilities> {
        match self.state.source {
            BughuntSource::DynamoDb => Some(BackfillSourceCapabilities::dynamodb_export()),
            BughuntSource::AuxStorage => Some(BackfillSourceCapabilities::aux_storage()),
            BughuntSource::RawBackup => Some(BackfillSourceCapabilities::raw_backup()),
            BughuntSource::LocalFixture => Some(BackfillSourceCapabilities::local_fixture()),
            BughuntSource::Unsupported => None,
        }
    }
}

fn snapshot_for_requested(requested: RequestedMethod) -> Option<SnapshotMethod> {
    match requested {
        RequestedMethod::NoRequestedMethod => None,
        RequestedMethod::RequestedDynamoDbPitrExport => Some(SnapshotMethod::DynamoDbPitrExport),
        RequestedMethod::RequestedDynamoDbParallelScan => {
            Some(SnapshotMethod::DynamoDbParallelScan)
        }
        RequestedMethod::RequestedAuxStorageEnumeration => {
            Some(SnapshotMethod::AuxStorageEnumeration)
        }
        RequestedMethod::RequestedRawBackupReplay => Some(SnapshotMethod::RawBackupReplay),
        RequestedMethod::RequestedLocalFixtureSnapshot => Some(SnapshotMethod::LocalFixture),
    }
}

fn snapshot_for_bughunt_requested(requested: BughuntRequested) -> Option<SnapshotMethod> {
    match requested {
        BughuntRequested::None => None,
        BughuntRequested::WantExport => Some(SnapshotMethod::DynamoDbPitrExport),
        BughuntRequested::WantScan => Some(SnapshotMethod::DynamoDbParallelScan),
        BughuntRequested::WantEnumerate => Some(SnapshotMethod::AuxStorageEnumeration),
        BughuntRequested::WantReplay => Some(SnapshotMethod::RawBackupReplay),
        BughuntRequested::WantFixture => Some(SnapshotMethod::LocalFixture),
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/backfill_planning_bughunt.qnt",
    max_samples = 50,
    max_steps = 8,
    seed = "0x5160"
)]
fn quint_backfill_planning_bughunt_model_matches_planner() -> impl Driver {
    BughuntDriver::default()
}

fn bughunt_strategy(method: SnapshotMethod) -> BughuntStrategy {
    match method {
        SnapshotMethod::DynamoDbPitrExport => BughuntStrategy::Export,
        SnapshotMethod::DynamoDbParallelScan => BughuntStrategy::Scan,
        SnapshotMethod::AuxStorageEnumeration => BughuntStrategy::Enumerate,
        SnapshotMethod::RawBackupReplay => BughuntStrategy::Replay,
        SnapshotMethod::LocalFixture => BughuntStrategy::Fixture,
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum MbtPlanResult {
    NotChecked,
    PlannedDynamoDbPitrExport,
    PlannedDynamoDbParallelScan,
    PlannedAuxStorageEnumeration,
    PlannedRawBackupReplay,
    PlannedLocalFixtureSnapshot,
    RejectedMissingIdentity,
    RejectedUnsafeStreamOnly,
    RejectedHistoricalEnumerationUnsupported,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct MbtPlanState {
    #[serde(rename = "lastResult")]
    last_result: MbtPlanResult,
}

impl State<MbtPlanningDriver> for MbtPlanState {
    fn from_driver(driver: &MbtPlanningDriver) -> Result<Self> {
        Ok(Self {
            last_result: driver.last_result,
        })
    }
}

struct MbtPlanningDriver {
    last_result: MbtPlanResult,
}

impl Default for MbtPlanningDriver {
    fn default() -> Self {
        Self {
            last_result: MbtPlanResult::NotChecked,
        }
    }
}

impl Driver for MbtPlanningDriver {
    type State = MbtPlanState;

    #[allow(non_snake_case)]
    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.last_result = MbtPlanResult::NotChecked,
            CheckPlan(
                sourceKind: SourceKind,
                requestedMethod: RequestedMethod,
                hasRowIdentity: bool,
                supportsPitrExport: bool,
                supportsParallelScan: bool,
                supportsEnumeration: bool,
                supportsRawReplay: bool,
            ) => {
                self.last_result = check_mbt_plan(
                    sourceKind,
                    requestedMethod,
                    hasRowIdentity,
                    supportsPitrExport,
                    supportsParallelScan,
                    supportsEnumeration,
                    supportsRawReplay,
                );
            },
            step(
                sourceKind: SourceKind?,
                requestedMethod: RequestedMethod?,
                hasRowIdentity: bool?,
                supportsPitrExport: bool?,
                supportsParallelScan: bool?,
                supportsEnumeration: bool?,
                supportsRawReplay: bool?,
            ) => {
                if let (
                    Some(source_kind),
                    Some(requested_method),
                    Some(has_row_identity),
                    Some(supports_pitr_export),
                    Some(supports_parallel_scan),
                    Some(supports_enumeration),
                    Some(supports_raw_replay),
                ) = (
                    sourceKind,
                    requestedMethod,
                    hasRowIdentity,
                    supportsPitrExport,
                    supportsParallelScan,
                    supportsEnumeration,
                    supportsRawReplay,
                ) {
                    self.last_result = check_mbt_plan(
                        source_kind,
                        requested_method,
                        has_row_identity,
                        supports_pitr_export,
                        supports_parallel_scan,
                        supports_enumeration,
                        supports_raw_replay,
                    );
                }
            },
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/backfill_planning_mbt.qnt",
    max_samples = 100,
    max_steps = 6,
    seed = "0x5161"
)]
fn quint_backfill_planning_mbt_compares_full_result_state() -> impl Driver {
    MbtPlanningDriver::default()
}

fn check_mbt_plan(
    source_kind: SourceKind,
    requested_method: RequestedMethod,
    has_row_identity: bool,
    supports_pitr_export: bool,
    supports_parallel_scan: bool,
    supports_enumeration: bool,
    supports_raw_replay: bool,
) -> MbtPlanResult {
    let request = BackfillRequest {
        target_tables: vec!["users".to_string()],
        source: BackfillSourceCapabilities {
            source_kind: rust_source_kind(source_kind),
            stream_retention_hours: if source_kind == SourceKind::DynamoDb {
                Some(24)
            } else {
                None
            },
            supports_pitr_export,
            supports_parallel_scan,
            supports_full_enumeration: supports_enumeration,
            supports_raw_replay,
            can_prove_deletes: matches!(
                source_kind,
                SourceKind::AuxStorage | SourceKind::LocalFixture
            ),
        },
        requested_snapshot_method: snapshot_for_requested(requested_method),
        filters: Vec::new(),
        chunk_target_rows: 100,
        rate_limit: RateLimitPolicy::default(),
        destination_backend: DestinationBackend::DuckDb,
        dry_run: true,
        validation_mode: ValidationMode::CountsAndHashes,
        all_tables_have_row_identity: has_row_identity,
        estimated_table_rows: 100,
    };
    match ConservativeBackfillPlanner.plan(&request) {
        Ok(plan) => mbt_result_for_method(plan.snapshot_method),
        Err(BackfillPlanError::MissingRowIdentity { .. }) => MbtPlanResult::RejectedMissingIdentity,
        Err(BackfillPlanError::UnsafeStreamOnlyBackfill { .. }) => {
            MbtPlanResult::RejectedUnsafeStreamOnly
        }
        Err(_) => MbtPlanResult::RejectedHistoricalEnumerationUnsupported,
    }
}

fn rust_source_kind(source_kind: SourceKind) -> crate::BackfillSourceKind {
    match source_kind {
        SourceKind::DynamoDb => crate::BackfillSourceKind::DynamoDb,
        SourceKind::AuxStorage => crate::BackfillSourceKind::AuxStorage,
        SourceKind::RawBackup => crate::BackfillSourceKind::RawBackup,
        SourceKind::LocalFixture => crate::BackfillSourceKind::LocalFixture,
    }
}

fn mbt_result_for_method(method: SnapshotMethod) -> MbtPlanResult {
    match method {
        SnapshotMethod::DynamoDbPitrExport => MbtPlanResult::PlannedDynamoDbPitrExport,
        SnapshotMethod::DynamoDbParallelScan => MbtPlanResult::PlannedDynamoDbParallelScan,
        SnapshotMethod::AuxStorageEnumeration => MbtPlanResult::PlannedAuxStorageEnumeration,
        SnapshotMethod::RawBackupReplay => MbtPlanResult::PlannedRawBackupReplay,
        SnapshotMethod::LocalFixture => MbtPlanResult::PlannedLocalFixtureSnapshot,
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum DiscoveryBackendKind {
    DynamoDb,
    AuxStorage,
    RawBackup,
    LocalFixture,
    Unsupported,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum DiscoveryResult {
    NotChecked,
    DiscoveredDynamoDbFull,
    DiscoveredDynamoDbScanOnly,
    DiscoveredAuxStorageFull,
    DiscoveredRawBackupReplay,
    DiscoveredLocalFixtureFull,
    RejectedUnsupported,
    RejectedPartialUnsupported,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct DiscoveryMbtState {
    #[serde(rename = "lastResult")]
    last_result: DiscoveryResult,
}

impl State<DiscoveryMbtDriver> for DiscoveryMbtState {
    fn from_driver(driver: &DiscoveryMbtDriver) -> Result<Self> {
        Ok(Self {
            last_result: driver.last_result,
        })
    }
}

struct DiscoveryMbtDriver {
    last_result: DiscoveryResult,
}

impl Default for DiscoveryMbtDriver {
    fn default() -> Self {
        Self {
            last_result: DiscoveryResult::NotChecked,
        }
    }
}

impl Driver for DiscoveryMbtDriver {
    type State = DiscoveryMbtState;

    #[allow(non_snake_case)]
    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.last_result = DiscoveryResult::NotChecked,
            CheckDiscovery(
                backendKind: DiscoveryBackendKind,
                metadataReachable: bool,
                pitrEnabled: bool,
                scanEnabled: bool,
                enumerationEnabled: bool,
                rawReplayEnabled: bool,
            ) => {
                self.last_result = check_discovery(
                    backendKind,
                    metadataReachable,
                    pitrEnabled,
                    scanEnabled,
                    enumerationEnabled,
                    rawReplayEnabled,
                );
            },
            step(
                backendKind: DiscoveryBackendKind?,
                metadataReachable: bool?,
                pitrEnabled: bool?,
                scanEnabled: bool?,
                enumerationEnabled: bool?,
                rawReplayEnabled: bool?,
            ) => {
                if let (
                    Some(backend_kind),
                    Some(metadata_reachable),
                    Some(pitr_enabled),
                    Some(scan_enabled),
                    Some(enumeration_enabled),
                    Some(raw_replay_enabled),
                ) = (
                    backendKind,
                    metadataReachable,
                    pitrEnabled,
                    scanEnabled,
                    enumerationEnabled,
                    rawReplayEnabled,
                ) {
                    self.last_result = check_discovery(
                        backend_kind,
                        metadata_reachable,
                        pitr_enabled,
                        scan_enabled,
                        enumeration_enabled,
                        raw_replay_enabled,
                    );
                }
            },
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/backfill_capability_discovery_mbt.qnt",
    max_samples = 100,
    max_steps = 6,
    seed = "0x5162"
)]
fn quint_backfill_capability_discovery_mbt_compares_full_result_state() -> impl Driver {
    DiscoveryMbtDriver::default()
}

fn check_discovery(
    backend_kind: DiscoveryBackendKind,
    metadata_reachable: bool,
    pitr_enabled: bool,
    scan_enabled: bool,
    enumeration_enabled: bool,
    raw_replay_enabled: bool,
) -> DiscoveryResult {
    match backend_kind {
        DiscoveryBackendKind::DynamoDb => {
            check_dynamodb_discovery(metadata_reachable, pitr_enabled, scan_enabled)
        }
        DiscoveryBackendKind::AuxStorage => {
            check_aux_storage_discovery(metadata_reachable, enumeration_enabled)
        }
        DiscoveryBackendKind::RawBackup if metadata_reachable && raw_replay_enabled => {
            DiscoveryResult::DiscoveredRawBackupReplay
        }
        DiscoveryBackendKind::LocalFixture if metadata_reachable && enumeration_enabled => {
            DiscoveryResult::DiscoveredLocalFixtureFull
        }
        DiscoveryBackendKind::Unsupported if metadata_reachable => {
            DiscoveryResult::RejectedUnsupported
        }
        DiscoveryBackendKind::Unsupported => DiscoveryResult::RejectedPartialUnsupported,
        _ => DiscoveryResult::RejectedPartialUnsupported,
    }
}

fn check_dynamodb_discovery(
    metadata_reachable: bool,
    pitr_enabled: bool,
    scan_enabled: bool,
) -> DiscoveryResult {
    if !metadata_reachable {
        return DiscoveryResult::RejectedPartialUnsupported;
    }
    if pitr_enabled && scan_enabled {
        DiscoveryResult::DiscoveredDynamoDbFull
    } else if scan_enabled {
        DiscoveryResult::DiscoveredDynamoDbScanOnly
    } else {
        DiscoveryResult::RejectedPartialUnsupported
    }
}

fn check_aux_storage_discovery(
    metadata_reachable: bool,
    enumeration_enabled: bool,
) -> DiscoveryResult {
    if metadata_reachable && enumeration_enabled {
        DiscoveryResult::DiscoveredAuxStorageFull
    } else {
        DiscoveryResult::RejectedPartialUnsupported
    }
}
