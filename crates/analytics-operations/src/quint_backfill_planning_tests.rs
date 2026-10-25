#![allow(
    clippy::enum_variant_names,
    clippy::match_same_arms,
    non_snake_case,
    reason = "Quint MBT records intentionally mirror finite boolean model state"
)]

use analytics_quint_test_support::{map_result, run_fresh_traces};
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
    operation_identity: i64,
    source_kind: SourceKind,
    metadata_reachable: bool,
    destination_ready: bool,
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

impl State<PlanningDriver> for PlanningState {
    fn from_driver(driver: &PlanningDriver) -> Result<Self> {
        Ok(driver.state.clone())
    }
}

struct PlanningDriver {
    state: PlanningState,
}

impl Default for PlanningDriver {
    fn default() -> Self {
        Self {
            state: PlanningState {
                state: PlanState::Requested,
                operation_identity: 1,
                source_kind: SourceKind::DynamoDb,
                metadata_reachable: false,
                destination_ready: false,
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
    type State = PlanningState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            InspectSource(
                kind: SourceKind,
                identity: bool,
                reachable: bool,
            ) => {
                self.inspect_source(kind, identity, reachable);
            },
            InspectDestination => {
                if self.state.state == PlanState::SourceInspected {
                    self.state.state = PlanState::DestinationInspected;
                    self.state.destination_ready = true;
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
            step(
                actionChoice: String?,
                kind: SourceKind?,
                identity: bool?,
                reachable: bool?,
                requested: RequestedMethod?,
            ) => {
                self.model_step(
                    actionChoice.as_deref(),
                    kind,
                    identity,
                    reachable,
                    requested,
                );
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
    fn model_step(
        &mut self,
        action_choice: Option<&str>,
        kind: Option<SourceKind>,
        identity: Option<bool>,
        reachable: Option<bool>,
        requested: Option<RequestedMethod>,
    ) {
        match action_choice {
            Some("inspect_source") => {
                if let (Some(kind), Some(identity), Some(reachable)) = (kind, identity, reachable) {
                    self.inspect_source(kind, identity, reachable);
                }
            }
            Some("inspect_destination") if self.state.state == PlanState::SourceInspected => {
                self.state.state = PlanState::DestinationInspected;
                self.state.destination_ready = true;
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
            Some("request_method") if self.state.state == PlanState::Requested => {
                if let Some(requested) = requested {
                    self.state.requested_method = requested;
                }
            }
            _ => {}
        }
    }

    fn inspect_source(&mut self, kind: SourceKind, identity: bool, reachable: bool) {
        if self.state.state != PlanState::Requested {
            return;
        }
        self.state.state = PlanState::SourceInspected;
        self.state.operation_identity = 1;
        self.state.source_kind = kind;
        self.state.metadata_reachable = reachable;
        self.state.destination_ready = false;
        self.state.has_row_identity = identity;
        self.state.supports_pitr_export = kind == SourceKind::DynamoDb;
        self.state.supports_parallel_scan = kind == SourceKind::DynamoDb;
        self.state.supports_enumeration = kind == SourceKind::AuxStorage;
        self.state.supports_raw_replay = kind == SourceKind::RawBackup;
        self.state.stream_retention_hours = if kind == SourceKind::DynamoDb { 24 } else { 0 };
        self.state.deletes_can_be_proven = kind == SourceKind::AuxStorage;
    }

    fn choose_strategy(&mut self) {
        if self.state.state != PlanState::DestinationInspected {
            return;
        }
        let request = BackfillRequest {
            target_tables: vec!["users".to_string()],
            source: capabilities_for_state(
                self.state.source_kind,
                self.state.supports_pitr_export,
                self.state.supports_parallel_scan,
                self.state.supports_enumeration,
                self.state.supports_raw_replay,
                self.state.deletes_can_be_proven,
            ),
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
        let result = ConservativeBackfillPlanner.plan(&request);
        self.state.chosen_method = match result {
            Ok(plan) => model_method(plan.snapshot_method),
            Err(BackfillPlanError::MissingRowIdentity { .. }) => {
                // The model records the strategy selected from source
                // capabilities before rejecting an unsafe plan. Ask the
                // production planner for that strategy on the same request
                // after satisfying its independent identity precondition.
                let mut strategy_request = request;
                strategy_request.all_tables_have_row_identity = true;
                match ConservativeBackfillPlanner.plan(&strategy_request) {
                    Ok(plan) => model_method(plan.snapshot_method),
                    Err(_) => ModelSnapshotMethod::StreamOnly,
                }
            }
            Err(
                BackfillPlanError::UnsafeStreamOnlyBackfill { .. }
                | BackfillPlanError::HistoricalEnumerationUnsupported
                | BackfillPlanError::MissingTargetTables
                | BackfillPlanError::UnsupportedSourceKind(_)
                | BackfillPlanError::CapabilityDiscoveryFailed(_)
                | BackfillPlanError::PartialCapabilitySupport { .. },
            ) => ModelSnapshotMethod::StreamOnly,
        };
        self.state.state = if self.state.has_row_identity
            && self.state.metadata_reachable
            && self.state.destination_ready
            && self.state.chosen_method != ModelSnapshotMethod::StreamOnly
        {
            PlanState::StrategyChosen
        } else {
            PlanState::Rejected
        };
    }
}

#[test]
fn given_quint_backfill_planning_model_when_cases_are_replayed_then_planner_matches() {
    let seeds = (0..50).map(|offset| format!("0x{:x}", 0x515f_u64 + offset));
    run_fresh_traces(
        "given_quint_backfill_planning_model_when_cases_are_replayed_then_planner_matches",
        "../../specs/backfill_planning.qnt",
        8,
        seeds,
        PlanningDriver::default,
    )
    .expect("backfill planning traces");
}

#[test]
fn given_backfill_planning_seeds_when_drivers_created_then_each_seed_is_isolated() {
    let trace_plan = ["0x515f", "0x5160"];
    let receipt = run_fresh_traces(
        "backfill_planning_fresh_driver_hostile",
        "../../specs/backfill_planning.qnt",
        8,
        trace_plan,
        FreshOnlyPlanningDriver::default,
    )
    .expect("fresh driver must isolate each hostile trace");

    assert_eq!(receipt.expected_trace_count, trace_plan.len());
    assert_eq!(receipt.factory_invocations, trace_plan.len());
    assert_eq!(receipt.seeds, trace_plan);
}

#[test]
fn given_multi_trace_runner_when_driver_reuse_is_forbidden_then_control_fails() {
    let config = quint_connect::runner::Config {
        test_name: "backfill_planning_reused_driver_control".to_owned(),
        gen_config: quint_connect::runner::RunConfig {
            spec: "../../specs/backfill_planning.qnt".to_owned(),
            main: None,
            init: None,
            step: None,
            max_samples: Some(2),
            // One-step traces are sufficient for this control and force each
            // generated trace to include its explicit init action. This
            // verifies runner reuse without claiming that the generated traces
            // match the separate fresh-trace plan.
            max_steps: Some(1),
            seed: "0x515f".to_owned(),
        },
    };
    let error = quint_connect::runner::run_test(FreshOnlyPlanningDriver::default(), config)
        .expect_err("the multi-trace runner must reuse one driver");

    assert!(
        error
            .to_string()
            .contains("planning driver was reused across explicit fresh traces")
    );
}

#[derive(Default)]
struct FreshOnlyPlanningDriver {
    inner: PlanningDriver,
    initialized: bool,
}

impl State<FreshOnlyPlanningDriver> for PlanningState {
    fn from_driver(driver: &FreshOnlyPlanningDriver) -> Result<Self> {
        Ok(driver.inner.state.clone())
    }
}

impl Driver for FreshOnlyPlanningDriver {
    type State = PlanningState;

    fn step(&mut self, step: &Step) -> Result {
        if step.action_taken == "init" {
            anyhow::ensure!(
                !self.initialized,
                "planning driver was reused across explicit fresh traces"
            );
            self.initialized = true;
        }
        self.inner.step(step)
    }
}

fn capabilities_for_state(
    kind: SourceKind,
    supports_pitr_export: bool,
    supports_parallel_scan: bool,
    supports_enumeration: bool,
    supports_raw_replay: bool,
    can_prove_deletes: bool,
) -> BackfillSourceCapabilities {
    BackfillSourceCapabilities {
        source_kind: match kind {
            SourceKind::DynamoDb => crate::BackfillSourceKind::DynamoDb,
            SourceKind::AuxStorage => crate::BackfillSourceKind::AuxStorage,
            SourceKind::RawBackup => crate::BackfillSourceKind::RawBackup,
            SourceKind::LocalFixture => crate::BackfillSourceKind::LocalFixture,
        },
        stream_retention_hours: (kind == SourceKind::DynamoDb).then_some(24),
        supports_pitr_export,
        supports_parallel_scan,
        supports_full_enumeration: supports_enumeration,
        supports_raw_replay,
        can_prove_deletes,
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
fn given_backfill_strategies_when_planned_then_quint_model_agrees() {
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
fn given_requested_strategies_when_planned_then_quint_model_agrees() {
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

impl State<BughuntDriver> for BughuntState {
    fn from_driver(driver: &BughuntDriver) -> Result<Self> {
        Ok(driver.state.clone())
    }
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
    type State = BughuntState;

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
            step(
                action_choice: String?,
                sourceKind: BughuntSource?,
                identity: bool?,
                requestedMethod: BughuntRequested?,
            ) => {
                if action_choice.as_deref() == Some("discover") {
                    let Some(source_kind) = sourceKind else {
                        return Ok(());
                    };
                    self.discover(source_kind, identity.unwrap_or(true));
                } else if action_choice.as_deref() == Some("request_method") {
                    if let Some(requested_method) = requestedMethod {
                        self.request_method(requested_method);
                    }
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
        self.state.enumerate = source == BughuntSource::AuxStorage;
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
        let source_kind = match self.state.source {
            BughuntSource::DynamoDb => crate::BackfillSourceKind::DynamoDb,
            BughuntSource::AuxStorage => crate::BackfillSourceKind::AuxStorage,
            BughuntSource::RawBackup => crate::BackfillSourceKind::RawBackup,
            BughuntSource::LocalFixture => crate::BackfillSourceKind::LocalFixture,
            BughuntSource::Unsupported => return None,
        };
        Some(BackfillSourceCapabilities {
            source_kind,
            stream_retention_hours: (self.state.source == BughuntSource::DynamoDb).then_some(24),
            supports_pitr_export: self.state.pitr,
            supports_parallel_scan: self.state.scan,
            supports_full_enumeration: self.state.enumerate,
            supports_raw_replay: self.state.replay,
            can_prove_deletes: self.state.delete_proof_recorded,
        })
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

#[test]
fn given_quint_backfill_planning_bughunt_model_when_cases_are_replayed_then_planner_matches() {
    let seeds = (0..50).map(|offset| format!("0x{:x}", 0x5160_u64 + offset));
    run_fresh_traces(
        "given_quint_backfill_planning_bughunt_model_when_cases_are_replayed_then_planner_matches",
        "../../specs/backfill_planning_bughunt.qnt",
        8,
        seeds,
        BughuntDriver::default,
    )
    .expect("backfill planning bug-hunt traces");
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
    #[serde(rename = "lastCase")]
    last_case: MbtPlanCase,
    #[serde(rename = "lastResult")]
    last_result: MbtPlanResult,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct MbtPlanCase {
    source_kind: SourceKind,
    requested_method: RequestedMethod,
    has_row_identity: bool,
    supports_pitr_export: bool,
    supports_parallel_scan: bool,
    supports_enumeration: bool,
    supports_raw_replay: bool,
}

impl State<MbtPlanningDriver> for MbtPlanState {
    fn from_driver(driver: &MbtPlanningDriver) -> Result<Self> {
        Ok(Self {
            last_case: driver.last_case,
            last_result: driver.last_result,
        })
    }
}

struct MbtPlanningDriver {
    last_case: MbtPlanCase,
    last_result: MbtPlanResult,
}

impl Default for MbtPlanningDriver {
    fn default() -> Self {
        Self {
            last_case: MbtPlanCase {
                source_kind: SourceKind::DynamoDb,
                requested_method: RequestedMethod::NoRequestedMethod,
                has_row_identity: true,
                supports_pitr_export: true,
                supports_parallel_scan: true,
                supports_enumeration: false,
                supports_raw_replay: false,
            },
            last_result: MbtPlanResult::NotChecked,
        }
    }
}

impl Driver for MbtPlanningDriver {
    type State = MbtPlanState;

    #[allow(non_snake_case)]
    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            CheckPlan(
                sourceKind: SourceKind,
                requestedMethod: RequestedMethod,
                hasRowIdentity: bool,
                supportsPitrExport: bool,
                supportsParallelScan: bool,
                supportsEnumeration: bool,
                supportsRawReplay: bool,
            ) => {
                self.last_case = MbtPlanCase {
                    source_kind: sourceKind,
                    requested_method: requestedMethod,
                    has_row_identity: hasRowIdentity,
                    supports_pitr_export: supportsPitrExport,
                    supports_parallel_scan: supportsParallelScan,
                    supports_enumeration: supportsEnumeration,
                    supports_raw_replay: supportsRawReplay,
                };
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
                    self.last_case = MbtPlanCase {
                        source_kind,
                        requested_method,
                        has_row_identity,
                        supports_pitr_export,
                        supports_parallel_scan,
                        supports_enumeration,
                        supports_raw_replay,
                    };
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
fn given_quint_backfill_planning_model_when_replayed_then_full_result_state_matches() -> impl Driver
{
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
    #[serde(rename = "lastCase")]
    last_case: DiscoveryMbtCase,
    #[serde(rename = "lastResult")]
    last_result: DiscoveryResult,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct DiscoveryMbtCase {
    backend_kind: DiscoveryBackendKind,
    metadata_reachable: bool,
    pitr_enabled: bool,
    scan_enabled: bool,
    enumeration_enabled: bool,
    raw_replay_enabled: bool,
}

impl State<DiscoveryMbtDriver> for DiscoveryMbtState {
    fn from_driver(driver: &DiscoveryMbtDriver) -> Result<Self> {
        Ok(Self {
            last_case: driver.last_case,
            last_result: driver.last_result,
        })
    }
}

struct DiscoveryMbtDriver {
    last_case: DiscoveryMbtCase,
    last_result: DiscoveryResult,
}

impl Default for DiscoveryMbtDriver {
    fn default() -> Self {
        Self {
            last_case: DiscoveryMbtCase {
                backend_kind: DiscoveryBackendKind::Unsupported,
                metadata_reachable: false,
                pitr_enabled: false,
                scan_enabled: false,
                enumeration_enabled: false,
                raw_replay_enabled: false,
            },
            last_result: DiscoveryResult::NotChecked,
        }
    }
}

impl Driver for DiscoveryMbtDriver {
    type State = DiscoveryMbtState;

    #[allow(non_snake_case)]
    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            CheckDiscovery(
                backendKind: DiscoveryBackendKind,
                metadataReachable: bool,
                pitrEnabled: bool,
                scanEnabled: bool,
                enumerationEnabled: bool,
                rawReplayEnabled: bool,
            ) => {
                self.last_case = DiscoveryMbtCase {
                    backend_kind: backendKind,
                    metadata_reachable: metadataReachable,
                    pitr_enabled: pitrEnabled,
                    scan_enabled: scanEnabled,
                    enumeration_enabled: enumerationEnabled,
                    raw_replay_enabled: rawReplayEnabled,
                };
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
                    self.last_case = DiscoveryMbtCase {
                        backend_kind,
                        metadata_reachable,
                        pitr_enabled,
                        scan_enabled,
                        enumeration_enabled,
                        raw_replay_enabled,
                    };
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
fn given_quint_backfill_capability_discovery_model_when_replayed_then_full_result_state_matches()
-> impl Driver {
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
