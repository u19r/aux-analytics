use analytics_contract::{AnalyticsManifest, StorageItem, StorageStreamRecord, StorageValue};
use analytics_engine::{
    AnalyticsEngine, AnalyticsEngineError, RecordCheckpoint as EngineRecordCheckpoint,
    SourceBatchRecord, SourceCheckpoint as EngineSourceCheckpoint, SourceEffectAuthority,
    SourceEffectDisposition, SourceEffectRequest, SourceEffectRequestInput, SourceEffectScope,
    SourceEffectSummary, StreamRecordBatchItem,
};
use analytics_fixtures::metric_points_manifest;
use analytics_storage::SourceCheckpoint;
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::{
    SourceHealth, SourceHealthStatus,
    source_polling::{
        SourceBatchOutcome, SourcePollingStartup, apply_source_durable_checkpoints,
        apply_source_poll_error_health, is_iterator_checkpoint, persistable_checkpoints,
        record_checkpoints, source_polling_startup, storage_checkpoints_from_engine,
    },
};

const METRIC_SOURCE_TABLE: &str = "metric_points_v1_stream";
const METRIC_ANALYTICS_TABLE: &str = "metric_points_v1";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum ApiPollingResult {
    NotChecked,
    PersistableCheckpointsFiltered,
    SuccessfulBatchCommits,
    IngestFailureBlocksCommit,
    CheckpointFailureBlocksCommit,
    SuccessHealthUpdated,
    PartialFailureHealthDegraded,
    PollErrorHealthDegraded,
    StartupDisabled,
    StartupStarting,
    StartupCheckpointsLoaded,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum ApiHealthState {
    Healthy,
    Degraded,
    Disabled,
    Starting,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum ApiPollingPhase {
    Idle,
    StartingPhase,
    Polling,
    HealthyPhase,
    DegradedPhase,
    DisabledPhase,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct ApiPollingState {
    last_result: ApiPollingResult,
    health: ApiHealthState,
    phase: ApiPollingPhase,
    durable_checkpoint_count: i64,
    durable_checkpoint_before: i64,
    last_persistable_checkpoint_count: i64,
    last_batch_clean: bool,
    last_checkpoint_save_succeeded: bool,
    last_lease_permitted: bool,
    last_committed: bool,
    last_current_batch_has_errors: bool,
    last_health_durable_checkpoint_count: i64,
    processor_enabled: bool,
    configured_table_count: i64,
    poller_is_empty: bool,
    loaded_checkpoint_count: i64,
}

impl State<ApiPollingDriver> for ApiPollingState {
    fn from_driver(driver: &ApiPollingDriver) -> Result<Self> {
        let durable_checkpoint_count = driver.durable_checkpoint_count()?;
        Ok(Self {
            last_result: driver.last_result,
            health: api_health_state(&driver.health),
            phase: driver.phase,
            durable_checkpoint_count,
            durable_checkpoint_before: driver.durable_checkpoint_before,
            last_persistable_checkpoint_count: driver.last_persistable_checkpoint_count,
            last_batch_clean: driver.last_batch_clean,
            last_checkpoint_save_succeeded: driver.last_checkpoint_save_succeeded,
            last_lease_permitted: driver.last_lease_permitted,
            last_committed: driver.last_committed,
            last_current_batch_has_errors: driver.last_current_batch_has_errors,
            last_health_durable_checkpoint_count: driver.last_health_durable_checkpoint_count,
            processor_enabled: driver.processor_enabled,
            configured_table_count: driver.configured_table_count,
            poller_is_empty: driver.poller_is_empty,
            loaded_checkpoint_count: driver.loaded_checkpoint_count,
        })
    }
}

struct ApiPollingDriver {
    engine: AnalyticsEngine,
    manifest: AnalyticsManifest,
    health: SourceHealth,
    phase: ApiPollingPhase,
    last_result: ApiPollingResult,
    durable_checkpoint_before: i64,
    last_persistable_checkpoint_count: i64,
    last_batch_clean: bool,
    last_checkpoint_save_succeeded: bool,
    last_lease_permitted: bool,
    last_committed: bool,
    last_current_batch_has_errors: bool,
    last_health_durable_checkpoint_count: i64,
    processor_enabled: bool,
    configured_table_count: i64,
    poller_is_empty: bool,
    loaded_checkpoint_count: i64,
    next_shard: u64,
    next_effect: u64,
}

impl Default for ApiPollingDriver {
    fn default() -> Self {
        let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect polling engine");
        let manifest = metric_points_manifest();
        engine
            .ensure_manifest(&manifest)
            .expect("initialize polling manifest");
        Self {
            engine,
            manifest,
            health: SourceHealth::starting(0),
            phase: ApiPollingPhase::Idle,
            last_result: ApiPollingResult::NotChecked,
            durable_checkpoint_before: 0,
            last_persistable_checkpoint_count: 0,
            last_batch_clean: true,
            last_checkpoint_save_succeeded: true,
            last_lease_permitted: true,
            last_committed: false,
            last_current_batch_has_errors: false,
            last_health_durable_checkpoint_count: 0,
            processor_enabled: false,
            configured_table_count: 0,
            poller_is_empty: true,
            loaded_checkpoint_count: 0,
            next_shard: 0,
            next_effect: 0,
        }
    }
}

impl Driver for ApiPollingDriver {
    type State = ApiPollingState;

    #[allow(non_snake_case)]
    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            CheckPersistableCheckpointsFiltered(leasePermitted: bool) => {
                self.check_persistable_checkpoints_filtered(leasePermitted)?;
            },
            CheckSuccessfulBatchCommits => self.check_successful_batch_commits()?,
            CheckIngestFailureBlocksCommit => self.check_ingest_failure_blocks_commit()?,
            CheckCheckpointFailureBlocksCommit => self.check_checkpoint_failure_blocks_commit()?,
            CheckSuccessHealth => self.check_success_health()?,
            CheckPartialFailureHealth => self.check_partial_failure_health()?,
            CheckPollErrorHealth => self.check_poll_error_health()?,
            CheckStartupDisabled => self.check_startup_disabled()?,
            CheckStartupStarting => self.check_startup_starting()?,
            CheckStartupCheckpointMapping => self.check_startup_checkpoint_mapping()?,
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/polling_api.qnt",
    max_samples = 50,
    max_steps = 8,
    seed = "0x6"
)]
fn given_quint_polling_api_model_when_replayed_then_source_polling_matches() -> impl Driver {
    ApiPollingDriver::default()
}

impl ApiPollingDriver {
    fn durable_checkpoint_count(&self) -> Result<i64> {
        i64::try_from(self.engine.load_source_checkpoints()?.len())
            .map_err(|error| anyhow::anyhow!("durable checkpoint count overflowed: {error}"))
    }

    fn begin_batch(&mut self) -> Result<i64> {
        let before = self.durable_checkpoint_count()?;
        self.durable_checkpoint_before = before;
        Ok(before)
    }

    fn next_shard(&mut self) -> String {
        self.next_shard = self.next_shard.saturating_add(1);
        format!("api-shard-{}", self.next_shard)
    }

    fn next_effect_authority(&mut self) -> ([u8; 16], u64) {
        self.next_effect = self.next_effect.saturating_add(1);
        (
            source_effect_execution_id(self.next_effect),
            self.next_effect,
        )
    }

    fn check_persistable_checkpoints_filtered(&mut self, lease_permitted: bool) -> Result {
        let before = self.begin_batch()?;
        let checkpoints = [
            record_checkpoint(METRIC_SOURCE_TABLE, "shard-0001", "2"),
            record_checkpoint(
                METRIC_SOURCE_TABLE,
                "__iterator:shard-0001",
                "next-iterator",
            ),
        ];
        let persistable = persistable_checkpoints(&checkpoints).collect::<Vec<_>>();
        if persistable.len() != 1
            || persistable[0].shard_id != "shard-0001"
            || is_iterator_checkpoint(persistable[0])
            || !is_iterator_checkpoint(&checkpoints[1])
        {
            return Err(anyhow::anyhow!(
                "iterator checkpoints were not filtered before persistence"
            ));
        }
        if self.durable_checkpoint_count()? != before {
            return Err(anyhow::anyhow!(
                "checkpoint filtering changed durable state"
            ));
        }
        self.last_result = ApiPollingResult::PersistableCheckpointsFiltered;
        self.phase = ApiPollingPhase::Polling;
        self.last_persistable_checkpoint_count = 0;
        self.last_batch_clean = true;
        self.last_checkpoint_save_succeeded = true;
        self.last_lease_permitted = lease_permitted;
        self.last_committed = false;
        self.last_current_batch_has_errors = false;
        Ok(())
    }

    fn check_successful_batch_commits(&mut self) -> Result {
        let before = self.begin_batch()?;
        let position = before.saturating_add(2).to_string();
        let shard_a = self.next_shard();
        let shard_b = self.next_shard();
        let checkpoints = [
            engine_checkpoint(METRIC_SOURCE_TABLE, shard_a.as_str(), position.as_str())?,
            engine_checkpoint(METRIC_SOURCE_TABLE, shard_b.as_str(), position.as_str())?,
        ];
        let event_a = format!("api-success-{}-a", self.next_shard);
        let event_b = format!("api-success-{}-b", self.next_shard);
        let records = [
            source_batch_item(
                METRIC_SOURCE_TABLE,
                shard_a.as_str(),
                METRIC_ANALYTICS_TABLE,
                event_a.as_str(),
                metric_point_item(event_a.as_str(), "standard", "1"),
            ),
            source_batch_item(
                METRIC_SOURCE_TABLE,
                shard_b.as_str(),
                METRIC_ANALYTICS_TABLE,
                event_b.as_str(),
                metric_point_item(event_b.as_str(), "standard", "2"),
            ),
        ];
        let (first_execution_id, first_fence) = self.next_effect_authority();
        let (second_execution_id, second_fence) = self.next_effect_authority();
        let first = fenced_source_batch(FencedSourceBatchRequest {
            engine: &self.engine,
            manifest: &self.manifest,
            execution_id: first_execution_id,
            fence: first_fence,
            source_table_name: METRIC_SOURCE_TABLE,
            shard_id: shard_a.as_str(),
            records: records[..1].to_vec(),
            checkpoints: vec![checkpoints[0].clone()],
        })?;
        let second = fenced_source_batch(FencedSourceBatchRequest {
            engine: &self.engine,
            manifest: &self.manifest,
            execution_id: second_execution_id,
            fence: second_fence,
            source_table_name: METRIC_SOURCE_TABLE,
            shard_id: shard_b.as_str(),
            records: records[1..].to_vec(),
            checkpoints: vec![checkpoints[1].clone()],
        })?;
        if first.inserted() != 1 || second.inserted() != 1 {
            return Err(anyhow::anyhow!(
                "successful fenced source batches produced unexpected outcomes"
            ));
        }
        let after = self.durable_checkpoint_count()?;
        if after != before + 2 {
            return Err(anyhow::anyhow!(
                "successful source batch advanced {before} to {after}, expected two checkpoints"
            ));
        }
        let storage_checkpoints = vec![
            record_checkpoint(METRIC_SOURCE_TABLE, shard_a.as_str(), position.as_str()),
            record_checkpoint(METRIC_SOURCE_TABLE, shard_b.as_str(), position.as_str()),
        ];
        self.health.apply_source_batch_health(
            SourceBatchOutcome {
                is_committed: true,
                records_ingested: 2,
                ingest_errors: 0,
                checkpoints_saved: 2,
                checkpoint_errors: 0,
            },
            &storage_checkpoints,
            500,
        );
        self.last_result = ApiPollingResult::SuccessfulBatchCommits;
        self.phase = ApiPollingPhase::HealthyPhase;
        self.last_persistable_checkpoint_count = 2;
        self.last_batch_clean = true;
        self.last_checkpoint_save_succeeded = true;
        self.last_lease_permitted = true;
        self.last_committed = true;
        self.last_current_batch_has_errors = false;
        self.last_health_durable_checkpoint_count = after;
        self.processor_enabled = true;
        self.configured_table_count = 2;
        self.poller_is_empty = false;
        Ok(())
    }

    fn check_ingest_failure_blocks_commit(&mut self) -> Result {
        let before = self.begin_batch()?;
        let persistable = vec![
            record_checkpoint(METRIC_SOURCE_TABLE, "api-failure-a", "3"),
            record_checkpoint(METRIC_SOURCE_TABLE, "api-failure-b", "4"),
            record_checkpoint(
                METRIC_SOURCE_TABLE,
                "__iterator:api-failure",
                "iterator-token",
            ),
        ];
        if persistable_checkpoints(&persistable).count() != 2 {
            return Err(anyhow::anyhow!(
                "failure fixture lost persistable checkpoints"
            ));
        }
        let (execution_id, fence) = self.next_effect_authority();
        let result = fenced_source_batch(FencedSourceBatchRequest {
            engine: &self.engine,
            manifest: &self.manifest,
            execution_id,
            fence,
            source_table_name: METRIC_SOURCE_TABLE,
            shard_id: "api-failure-a",
            records: vec![
                source_batch_item(
                    METRIC_SOURCE_TABLE,
                    "api-failure-a",
                    METRIC_ANALYTICS_TABLE,
                    "api-failure-valid",
                    metric_point_item("api-failure-valid", "standard", "1"),
                ),
                source_batch_item(
                    METRIC_SOURCE_TABLE,
                    "api-failure-a",
                    "unregistered_table",
                    "api-failure-invalid",
                    metric_point_item("api-failure-invalid", "standard", "2"),
                ),
            ],
            checkpoints: Vec::new(),
        });
        if !result_is_engine_error(&result, |error| {
            matches!(error, AnalyticsEngineError::TableNotRegistered(_))
        }) || self.durable_checkpoint_count()? != before
        {
            return Err(anyhow::anyhow!(
                "ingest failure did not roll back the production boundary"
            ));
        }
        self.health.apply_source_batch_health(
            SourceBatchOutcome {
                is_committed: false,
                records_ingested: 1,
                ingest_errors: 1,
                checkpoints_saved: 0,
                checkpoint_errors: 0,
            },
            &[],
            550,
        );
        self.last_result = ApiPollingResult::IngestFailureBlocksCommit;
        self.phase = ApiPollingPhase::DegradedPhase;
        self.last_persistable_checkpoint_count = 2;
        self.last_batch_clean = false;
        self.last_checkpoint_save_succeeded = false;
        self.last_lease_permitted = true;
        self.last_committed = false;
        self.last_current_batch_has_errors = true;
        self.last_health_durable_checkpoint_count = before;
        self.processor_enabled = true;
        self.configured_table_count = 2;
        self.poller_is_empty = false;
        Ok(())
    }

    fn check_checkpoint_failure_blocks_commit(&mut self) -> Result {
        let before = self.begin_batch()?;
        let shard_a = self.next_shard();
        let position = before.saturating_add(1).to_string();
        let checkpoints = vec![engine_checkpoint(
            METRIC_SOURCE_TABLE,
            shard_a.as_str(),
            position.as_str(),
        )?];
        let existing = self.engine.load_source_checkpoints()?;
        self.engine
            .connection()
            .execute_batch(CHECKPOINT_FAILURE_TABLE_DDL)?;
        let (execution_id, fence) = self.next_effect_authority();
        let result = fenced_source_batch(FencedSourceBatchRequest {
            engine: &self.engine,
            manifest: &self.manifest,
            execution_id,
            fence,
            source_table_name: METRIC_SOURCE_TABLE,
            shard_id: shard_a.as_str(),
            records: vec![source_batch_item(
                METRIC_SOURCE_TABLE,
                shard_a.as_str(),
                METRIC_ANALYTICS_TABLE,
                "api-checkpoint-failure",
                metric_point_item("api-checkpoint-failure", "standard", "1"),
            )],
            checkpoints,
        });
        restore_checkpoint_table(&self.engine, &existing)?;
        if !result_is_engine_error(&result, |error| {
            matches!(error, AnalyticsEngineError::DuckDb(_))
        }) || self.durable_checkpoint_count()? != before
        {
            return Err(anyhow::anyhow!(
                "checkpoint failure did not preserve the production durable boundary"
            ));
        }
        self.health.apply_source_batch_health(
            SourceBatchOutcome {
                is_committed: false,
                records_ingested: 1,
                ingest_errors: 0,
                checkpoints_saved: 0,
                checkpoint_errors: 1,
            },
            &[],
            600,
        );
        self.last_result = ApiPollingResult::CheckpointFailureBlocksCommit;
        self.phase = ApiPollingPhase::DegradedPhase;
        self.last_persistable_checkpoint_count = 2;
        self.last_batch_clean = false;
        self.last_checkpoint_save_succeeded = false;
        self.last_lease_permitted = true;
        self.last_committed = false;
        self.last_current_batch_has_errors = true;
        self.last_health_durable_checkpoint_count = before;
        self.processor_enabled = true;
        self.configured_table_count = 2;
        self.poller_is_empty = false;
        Ok(())
    }

    fn check_success_health(&mut self) -> Result {
        let before = self.begin_batch()?;
        self.health.apply_source_batch_health(
            SourceBatchOutcome {
                is_committed: true,
                records_ingested: 0,
                ingest_errors: 0,
                checkpoints_saved: 0,
                checkpoint_errors: 0,
            },
            &[],
            650,
        );
        self.last_result = ApiPollingResult::SuccessHealthUpdated;
        self.phase = ApiPollingPhase::HealthyPhase;
        self.last_persistable_checkpoint_count = 0;
        self.last_batch_clean = true;
        self.last_checkpoint_save_succeeded = true;
        self.last_lease_permitted = true;
        self.last_committed = true;
        self.last_current_batch_has_errors = false;
        self.last_health_durable_checkpoint_count = before;
        self.processor_enabled = true;
        self.configured_table_count = 1;
        self.poller_is_empty = false;
        Ok(())
    }

    fn check_partial_failure_health(&mut self) -> Result {
        let before = self.begin_batch()?;
        let last_success_before = self.health.last_success_at_ms;
        let checkpoints = [
            record_checkpoint(METRIC_SOURCE_TABLE, "api-partial", "6"),
            record_checkpoint(
                METRIC_SOURCE_TABLE,
                "__iterator:api-partial",
                "iterator-token",
            ),
        ];
        self.health.apply_source_batch_health(
            SourceBatchOutcome {
                is_committed: false,
                records_ingested: 1,
                ingest_errors: 1,
                checkpoints_saved: 0,
                checkpoint_errors: 1,
            },
            &checkpoints,
            700,
        );
        if self.durable_checkpoint_count()? != before {
            return Err(anyhow::anyhow!(
                "partial failure health changed durable checkpoint state"
            ));
        }
        if self.health.last_success_at_ms != last_success_before {
            return Err(anyhow::anyhow!(
                "partial failure health changed the last successful poll timestamp"
            ));
        }
        self.last_result = ApiPollingResult::PartialFailureHealthDegraded;
        self.phase = ApiPollingPhase::DegradedPhase;
        self.last_persistable_checkpoint_count = 1;
        self.last_batch_clean = false;
        self.last_checkpoint_save_succeeded = false;
        self.last_lease_permitted = true;
        self.last_committed = false;
        self.last_current_batch_has_errors = true;
        self.last_health_durable_checkpoint_count = before;
        self.processor_enabled = true;
        self.configured_table_count = 1;
        self.poller_is_empty = false;
        Ok(())
    }

    fn check_poll_error_health(&mut self) -> Result {
        let before = self.begin_batch()?;
        apply_source_poll_error_health(&mut self.health, "poll failed".to_string(), 750);
        self.last_result = ApiPollingResult::PollErrorHealthDegraded;
        self.phase = ApiPollingPhase::DegradedPhase;
        self.last_persistable_checkpoint_count = 0;
        self.last_batch_clean = false;
        self.last_checkpoint_save_succeeded = false;
        self.last_lease_permitted = true;
        self.last_committed = false;
        self.last_current_batch_has_errors = true;
        self.last_health_durable_checkpoint_count = before;
        Ok(())
    }

    fn check_startup_disabled(&mut self) -> Result {
        if source_polling_startup(true, 0, false) != SourcePollingStartup::Disabled
            || source_polling_startup(true, 2, true) != SourcePollingStartup::Disabled
            || source_polling_startup(false, 2, false) != SourcePollingStartup::Disabled
        {
            return Err(anyhow::anyhow!(
                "disabled polling startup did not match model"
            ));
        }
        let before = self.begin_batch()?;
        self.health = SourceHealth::disabled();
        self.last_result = ApiPollingResult::StartupDisabled;
        self.phase = ApiPollingPhase::DisabledPhase;
        self.last_persistable_checkpoint_count = 0;
        self.last_batch_clean = true;
        self.last_checkpoint_save_succeeded = true;
        self.last_lease_permitted = false;
        self.last_committed = false;
        self.last_current_batch_has_errors = false;
        self.last_health_durable_checkpoint_count = before;
        self.processor_enabled = false;
        self.configured_table_count = 0;
        self.poller_is_empty = true;
        self.loaded_checkpoint_count = 0;
        Ok(())
    }

    fn check_startup_starting(&mut self) -> Result {
        if source_polling_startup(true, 2, false) != SourcePollingStartup::Starting {
            return Err(anyhow::anyhow!(
                "starting polling startup did not match model"
            ));
        }
        let before = self.begin_batch()?;
        self.health = SourceHealth::starting(1);
        self.last_result = ApiPollingResult::StartupStarting;
        self.phase = ApiPollingPhase::StartingPhase;
        self.last_persistable_checkpoint_count = 0;
        self.last_batch_clean = true;
        self.last_checkpoint_save_succeeded = true;
        self.last_lease_permitted = false;
        self.last_committed = false;
        self.last_current_batch_has_errors = false;
        self.last_health_durable_checkpoint_count = before;
        self.processor_enabled = true;
        self.configured_table_count = 1;
        self.poller_is_empty = false;
        self.loaded_checkpoint_count = 0;
        Ok(())
    }

    fn check_startup_checkpoint_mapping(&mut self) -> Result {
        let before = self.begin_batch()?;
        let storage_checkpoints = storage_checkpoints_from_engine(vec![
            EngineSourceCheckpoint {
                source_table_name: "source_users".to_string(),
                shard_id: "shard-0000".to_string(),
                position: "10".to_string(),
            },
            EngineSourceCheckpoint {
                source_table_name: "source_events".to_string(),
                shard_id: "shard-0001".to_string(),
                position: "42".to_string(),
            },
        ]);
        if storage_checkpoints.len() != 2
            || storage_checkpoints[0].source_table_name != "source_users"
            || storage_checkpoints[0].shard_id != "shard-0000"
            || storage_checkpoints[1].source_table_name != "source_events"
            || storage_checkpoints[1].shard_id != "shard-0001"
        {
            return Err(anyhow::anyhow!(
                "startup checkpoint mapping did not match production state"
            ));
        }
        self.health = SourceHealth::starting(1);
        apply_source_durable_checkpoints(&mut self.health, &storage_checkpoints);
        self.last_result = ApiPollingResult::StartupCheckpointsLoaded;
        self.phase = ApiPollingPhase::Polling;
        self.last_persistable_checkpoint_count = 0;
        self.last_batch_clean = true;
        self.last_checkpoint_save_succeeded = true;
        self.last_lease_permitted = false;
        self.last_committed = false;
        self.last_current_batch_has_errors = false;
        self.last_health_durable_checkpoint_count = before;
        self.processor_enabled = true;
        self.configured_table_count = 1;
        self.poller_is_empty = false;
        self.loaded_checkpoint_count = i64::try_from(storage_checkpoints.len())?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum BatchIntegrationResult {
    NotChecked,
    SuccessfulCommit,
    IngestFailureSkippedCheckpoints,
    IteratorCheckpointFiltered,
    CheckpointFailureStopsCommit,
    EmptyBatchCheckpointCommit,
    MalformedRecordBlocked,
    PoisonReleasedAfterRepair,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct BatchIntegrationState {
    last_result: BatchIntegrationResult,
    durable_checkpoint_count: i64,
    durable_checkpoint_before: i64,
    record_count: i64,
    ingest_error_count: i64,
    persistable_count: i64,
    iterator_checkpoint_count: i64,
    attempted_checkpoint_count: i64,
    checkpoint_error_count: i64,
    last_batch_clean: bool,
    last_committed: bool,
    last_iterator_persisted: bool,
    last_lease_permitted: bool,
    last_first_checkpoint_fails: bool,
    last_checkpoint_saves_succeeded: bool,
    malformed_record_count: i64,
    records_applied: i64,
    durable_source_cursor: i64,
    durable_source_cursor_before: i64,
    source_blocked: bool,
    retry_allowed: bool,
    failure_recorded: bool,
    last_record_position: i64,
}

impl State<BatchIntegrationDriver> for BatchIntegrationState {
    fn from_driver(driver: &BatchIntegrationDriver) -> Result<Self> {
        let checkpoints = driver.engine.load_source_checkpoints()?;
        let failures = driver.engine.load_source_batch_failures()?;
        let durable_source_cursor = durable_source_cursor(&checkpoints)?;
        let source_blocked = failures
            .iter()
            .any(|failure| failure.repaired_at_ms.is_none());
        Ok(Self {
            last_result: driver.last_result,
            durable_checkpoint_count: i64::try_from(checkpoints.len())?,
            durable_checkpoint_before: driver.durable_checkpoint_before,
            record_count: driver.record_count,
            ingest_error_count: driver.ingest_error_count,
            persistable_count: driver.persistable_count,
            iterator_checkpoint_count: driver.iterator_checkpoint_count,
            attempted_checkpoint_count: driver.attempted_checkpoint_count,
            checkpoint_error_count: driver.checkpoint_error_count,
            last_batch_clean: driver.last_batch_clean,
            last_committed: driver.last_committed,
            last_iterator_persisted: driver.last_iterator_persisted,
            last_lease_permitted: driver.last_lease_permitted,
            last_first_checkpoint_fails: driver.last_first_checkpoint_fails,
            last_checkpoint_saves_succeeded: driver.last_checkpoint_saves_succeeded,
            malformed_record_count: driver.malformed_record_count,
            records_applied: driver.records_applied,
            durable_source_cursor,
            durable_source_cursor_before: driver.durable_source_cursor_before,
            source_blocked,
            retry_allowed: !source_blocked,
            failure_recorded: driver.failure_recorded,
            last_record_position: driver.last_record_position,
        })
    }
}

struct BatchIntegrationDriver {
    engine: AnalyticsEngine,
    manifest: AnalyticsManifest,
    last_result: BatchIntegrationResult,
    durable_checkpoint_before: i64,
    record_count: i64,
    ingest_error_count: i64,
    persistable_count: i64,
    iterator_checkpoint_count: i64,
    attempted_checkpoint_count: i64,
    checkpoint_error_count: i64,
    last_batch_clean: bool,
    last_committed: bool,
    last_iterator_persisted: bool,
    last_lease_permitted: bool,
    last_first_checkpoint_fails: bool,
    last_checkpoint_saves_succeeded: bool,
    malformed_record_count: i64,
    records_applied: i64,
    durable_source_cursor_before: i64,
    failure_recorded: bool,
    last_record_position: i64,
    next_batch: u64,
    next_effect: u64,
}

impl Default for BatchIntegrationDriver {
    fn default() -> Self {
        let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect batch engine");
        let manifest = metric_points_manifest();
        engine
            .ensure_manifest(&manifest)
            .expect("initialize batch manifest");
        Self {
            engine,
            manifest,
            last_result: BatchIntegrationResult::NotChecked,
            durable_checkpoint_before: 0,
            record_count: 0,
            ingest_error_count: 0,
            persistable_count: 0,
            iterator_checkpoint_count: 0,
            attempted_checkpoint_count: 0,
            checkpoint_error_count: 0,
            last_batch_clean: true,
            last_committed: false,
            last_iterator_persisted: false,
            last_lease_permitted: true,
            last_first_checkpoint_fails: false,
            last_checkpoint_saves_succeeded: true,
            malformed_record_count: 0,
            records_applied: 0,
            durable_source_cursor_before: 0,
            failure_recorded: false,
            last_record_position: 0,
            next_batch: 0,
            next_effect: 0,
        }
    }
}

impl Driver for BatchIntegrationDriver {
    type State = BatchIntegrationState;

    #[allow(non_snake_case)]
    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            CheckAllRecordsAndCheckpointsSucceed(
                records: i64,
                recordCheckpoints: i64,
                iteratorCheckpoints: i64
            ) => self.check_all_records_and_checkpoints_succeed(
                records,
                recordCheckpoints,
                iteratorCheckpoints,
            )?,
            CheckIngestFailureSkipsCheckpointSaves => {
                self.check_ingest_failure_skips_checkpoint_saves()?;
            },
            CheckIteratorCheckpointFiltered => self.check_iterator_checkpoint_filtered()?,
            CheckCheckpointSaveFailureStopsOrdering => {
                self.check_checkpoint_save_failure_stops_ordering()?;
            },
            CheckEmptyBatchCanPersistCheckpoint => {
                self.check_empty_batch_can_persist_checkpoint()?;
            },
            CheckMalformedRecordFailsClosed => self.check_malformed_record_fails_closed()?,
            OperatorRepairsPoison => self.check_poison_released_after_repair()?,
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/source_batch_integration.qnt",
    max_samples = 50,
    max_steps = 8,
    seed = "0x9"
)]
fn given_quint_source_batch_integration_model_when_replayed_then_polling_outcome_matches()
-> impl Driver {
    BatchIntegrationDriver::default()
}

#[test]
fn given_poison_source_record_when_batch_repaired_then_failure_and_recovery_are_visible() {
    let mut driver = BatchIntegrationDriver::default();
    driver
        .check_malformed_record_fails_closed()
        .expect("malformed source adapter path");
    let blocked = BatchIntegrationState::from_driver(&driver).expect("blocked state");
    assert_eq!(
        blocked.last_result,
        BatchIntegrationResult::MalformedRecordBlocked
    );
    assert!(blocked.source_blocked);
    assert!(!blocked.retry_allowed);
    assert_eq!(blocked.records_applied, 0);
    assert_eq!(blocked.durable_checkpoint_count, 0);

    driver
        .check_poison_released_after_repair()
        .expect("poison repair adapter path");
    let repaired = BatchIntegrationState::from_driver(&driver).expect("repaired state");
    assert_eq!(
        repaired.last_result,
        BatchIntegrationResult::PoisonReleasedAfterRepair
    );
    assert!(!repaired.source_blocked);
    assert!(repaired.retry_allowed);
    assert!(!repaired.failure_recorded);
}

impl BatchIntegrationDriver {
    fn durable_checkpoint_count(&self) -> Result<i64> {
        i64::try_from(self.engine.load_source_checkpoints()?.len())
            .map_err(|error| anyhow::anyhow!("durable checkpoint count overflowed: {error}"))
    }

    fn durable_source_cursor(&self) -> Result<i64> {
        let checkpoints = self.engine.load_source_checkpoints()?;
        durable_source_cursor(&checkpoints)
    }

    fn begin_batch(&mut self) -> Result<(i64, i64)> {
        let checkpoint_count = self.durable_checkpoint_count()?;
        let cursor = self.durable_source_cursor()?;
        self.durable_checkpoint_before = checkpoint_count;
        self.durable_source_cursor_before = cursor;
        self.last_record_position = cursor;
        Ok((checkpoint_count, cursor))
    }

    fn next_batch_id(&mut self) -> u64 {
        self.next_batch = self.next_batch.saturating_add(1);
        self.next_batch
    }

    fn next_effect_authority(&mut self) -> ([u8; 16], u64) {
        self.next_effect = self.next_effect.saturating_add(1);
        (
            source_effect_execution_id(self.next_effect),
            self.next_effect,
        )
    }

    fn check_all_records_and_checkpoints_succeed(
        &mut self,
        records: i64,
        record_checkpoint_count: i64,
        iterator_checkpoint_count: i64,
    ) -> Result {
        let (before_count, before_cursor) = self.begin_batch()?;
        let records = usize::try_from(records)?;
        let record_checkpoint_count = usize::try_from(record_checkpoint_count)?;
        let iterator_checkpoint_count = usize::try_from(iterator_checkpoint_count)?;
        let batch_id = self.next_batch_id();
        let target_cursor = before_cursor.saturating_add(i64::try_from(records)?);
        let mut storage_checkpoints =
            Vec::with_capacity(record_checkpoint_count.saturating_add(iterator_checkpoint_count));
        for index in 0..record_checkpoint_count {
            let shard = format!("batch-{batch_id}-record-{index}");
            let position = target_cursor.to_string();
            storage_checkpoints.push(record_checkpoint(
                METRIC_SOURCE_TABLE,
                shard.as_str(),
                position.as_str(),
            ));
        }
        for index in 0..iterator_checkpoint_count {
            let shard = format!("__iterator:batch-{batch_id}-{index}");
            let position = format!("iterator-{batch_id}-{index}");
            storage_checkpoints.push(record_checkpoint(
                METRIC_SOURCE_TABLE,
                shard.as_str(),
                position.as_str(),
            ));
        }
        let checkpoints = record_checkpoints(&storage_checkpoints);
        if checkpoints.len() != record_checkpoint_count {
            return Err(anyhow::anyhow!(
                "iterator checkpoint crossed the durable boundary"
            ));
        }
        let records = (0..records)
            .map(|index| {
                let shard = &storage_checkpoints[index % record_checkpoint_count].shard_id;
                let event = format!("batch-{batch_id}-event-{index}");
                let key = format!("batch-{batch_id}-record-{index}");
                source_batch_item(
                    METRIC_SOURCE_TABLE,
                    shard.as_str(),
                    METRIC_ANALYTICS_TABLE,
                    key.as_str(),
                    metric_point_item(event.as_str(), "standard", (index + 1).to_string().as_str()),
                )
            })
            .collect::<Vec<_>>();
        let record_count = records.len();
        let mut outcome_count = 0_u64;
        for checkpoint in &checkpoints {
            let shard = checkpoint.shard_id();
            let shard_records = records
                .iter()
                .filter(|record| record.shard_id.as_deref() == Some(shard))
                .cloned()
                .collect::<Vec<_>>();
            let (execution_id, fence) = self.next_effect_authority();
            let summary = fenced_source_batch(FencedSourceBatchRequest {
                engine: &self.engine,
                manifest: &self.manifest,
                execution_id,
                fence,
                source_table_name: METRIC_SOURCE_TABLE,
                shard_id: shard,
                records: shard_records,
                checkpoints: vec![checkpoint.clone()],
            })?;
            outcome_count = outcome_count.saturating_add(
                summary
                    .inserted()
                    .saturating_add(summary.updated())
                    .saturating_add(summary.deleted())
                    .saturating_add(summary.skipped()),
            );
        }
        if outcome_count != u64::try_from(record_count)? {
            return Err(anyhow::anyhow!(
                "production batch returned the wrong outcome count"
            ));
        }
        let after_count = self.durable_checkpoint_count()?;
        let after_cursor = self.durable_source_cursor()?;
        if after_count != before_count + i64::try_from(record_checkpoint_count)?
            || after_cursor != target_cursor
        {
            return Err(anyhow::anyhow!(
                "successful batch did not advance durable production state"
            ));
        }
        self.last_result = BatchIntegrationResult::SuccessfulCommit;
        self.record_count = i64::try_from(record_count)?;
        self.ingest_error_count = 0;
        self.persistable_count = i64::try_from(record_checkpoint_count)?;
        self.iterator_checkpoint_count = i64::try_from(iterator_checkpoint_count)?;
        self.attempted_checkpoint_count = self.persistable_count;
        self.checkpoint_error_count = 0;
        self.last_batch_clean = true;
        self.last_committed = true;
        self.last_iterator_persisted = false;
        self.last_lease_permitted = true;
        self.last_first_checkpoint_fails = false;
        self.last_checkpoint_saves_succeeded = true;
        self.malformed_record_count = 0;
        self.records_applied = i64::try_from(record_count)?;
        self.failure_recorded = false;
        Ok(())
    }

    fn check_ingest_failure_skips_checkpoint_saves(&mut self) -> Result {
        let (before_count, before_cursor) = self.begin_batch()?;
        let visible_before = metric_row_count(&self.engine)?;
        let (execution_id, fence) = self.next_effect_authority();
        let result = fenced_source_batch(FencedSourceBatchRequest {
            engine: &self.engine,
            manifest: &self.manifest,
            execution_id,
            fence,
            source_table_name: METRIC_SOURCE_TABLE,
            shard_id: "ingest-failure-a",
            records: vec![
                source_batch_item(
                    METRIC_SOURCE_TABLE,
                    "ingest-failure-a",
                    METRIC_ANALYTICS_TABLE,
                    "ingest-failure-valid",
                    metric_point_item("ingest-failure-valid", "standard", "1"),
                ),
                source_batch_item(
                    METRIC_SOURCE_TABLE,
                    "ingest-failure-a",
                    "unregistered_table",
                    "ingest-failure-invalid",
                    metric_point_item("ingest-failure-invalid", "standard", "2"),
                ),
            ],
            checkpoints: Vec::new(),
        });
        if !result_is_engine_error(&result, |error| {
            matches!(error, AnalyticsEngineError::TableNotRegistered(_))
        }) || self.durable_checkpoint_count()? != before_count
            || self.durable_source_cursor()? != before_cursor
            || metric_row_count(&self.engine)? != visible_before
        {
            return Err(anyhow::anyhow!(
                "ingest failure did not roll back production rows and checkpoints"
            ));
        }
        self.last_result = BatchIntegrationResult::IngestFailureSkippedCheckpoints;
        self.record_count = 2;
        self.ingest_error_count = 1;
        self.persistable_count = 1;
        self.iterator_checkpoint_count = 1;
        self.attempted_checkpoint_count = 0;
        self.checkpoint_error_count = 0;
        self.last_batch_clean = false;
        self.last_committed = false;
        self.last_iterator_persisted = false;
        self.last_lease_permitted = true;
        self.last_first_checkpoint_fails = false;
        self.last_checkpoint_saves_succeeded = true;
        self.malformed_record_count = 0;
        self.records_applied = 0;
        self.failure_recorded = false;
        Ok(())
    }

    fn check_iterator_checkpoint_filtered(&mut self) -> Result {
        let (before_count, before_cursor) = self.begin_batch()?;
        let batch_id = self.next_batch_id();
        let shard = format!("iterator-record-{batch_id}");
        let iterator_shard = format!("__iterator:{shard}");
        let event = format!("iterator-record-{batch_id}");
        let record_position = before_cursor.saturating_add(1).to_string();
        let storage_checkpoints = vec![
            record_checkpoint(
                METRIC_SOURCE_TABLE,
                shard.as_str(),
                record_position.as_str(),
            ),
            record_checkpoint(
                METRIC_SOURCE_TABLE,
                iterator_shard.as_str(),
                "iterator-token",
            ),
        ];
        let checkpoints = record_checkpoints(&storage_checkpoints);
        if checkpoints.len() != 1 {
            return Err(anyhow::anyhow!("iterator checkpoint was not filtered"));
        }
        let (execution_id, fence) = self.next_effect_authority();
        let summary = fenced_source_batch(FencedSourceBatchRequest {
            engine: &self.engine,
            manifest: &self.manifest,
            execution_id,
            fence,
            source_table_name: METRIC_SOURCE_TABLE,
            shard_id: shard.as_str(),
            records: vec![source_batch_item(
                METRIC_SOURCE_TABLE,
                shard.as_str(),
                METRIC_ANALYTICS_TABLE,
                event.as_str(),
                metric_point_item(event.as_str(), "standard", "1"),
            )],
            checkpoints,
        })?;
        let after_count = self.durable_checkpoint_count()?;
        let after_cursor = self.durable_source_cursor()?;
        if summary.inserted() != 1
            || after_count != before_count + 1
            || after_cursor != before_cursor + 1
        {
            return Err(anyhow::anyhow!(
                "iterator filtering did not preserve the production checkpoint boundary: \
                 inserted={}, before_count={before_count}, after_count={after_count}, \
                 before_cursor={before_cursor}, after_cursor={after_cursor}",
                summary.inserted()
            ));
        }
        self.last_result = BatchIntegrationResult::IteratorCheckpointFiltered;
        self.record_count = 1;
        self.ingest_error_count = 0;
        self.persistable_count = 1;
        self.iterator_checkpoint_count = 1;
        self.attempted_checkpoint_count = 1;
        self.checkpoint_error_count = 0;
        self.last_batch_clean = true;
        self.last_committed = true;
        self.last_iterator_persisted = false;
        self.last_lease_permitted = true;
        self.last_first_checkpoint_fails = false;
        self.last_checkpoint_saves_succeeded = true;
        self.malformed_record_count = 0;
        self.records_applied = 1;
        self.failure_recorded = false;
        Ok(())
    }

    fn check_checkpoint_save_failure_stops_ordering(&mut self) -> Result {
        let (before_count, before_cursor) = self.begin_batch()?;
        let visible_before = metric_row_count(&self.engine)?;
        let position = before_cursor.saturating_add(1).to_string();
        let checkpoints = vec![engine_checkpoint(
            METRIC_SOURCE_TABLE,
            "checkpoint-failure-a",
            position.as_str(),
        )?];
        let existing = self.engine.load_source_checkpoints()?;
        self.engine
            .connection()
            .execute_batch(CHECKPOINT_FAILURE_TABLE_DDL)?;
        let (execution_id, fence) = self.next_effect_authority();
        let result = fenced_source_batch(FencedSourceBatchRequest {
            engine: &self.engine,
            manifest: &self.manifest,
            execution_id,
            fence,
            source_table_name: METRIC_SOURCE_TABLE,
            shard_id: "checkpoint-failure-a",
            records: vec![source_batch_item(
                METRIC_SOURCE_TABLE,
                "checkpoint-failure-a",
                METRIC_ANALYTICS_TABLE,
                "checkpoint-failure",
                metric_point_item("checkpoint-failure", "standard", "1"),
            )],
            checkpoints,
        });
        restore_checkpoint_table(&self.engine, &existing)?;
        if !result_is_engine_error(&result, |error| {
            matches!(error, AnalyticsEngineError::DuckDb(_))
        }) || self.durable_checkpoint_count()? != before_count
            || self.durable_source_cursor()? != before_cursor
            || metric_row_count(&self.engine)? != visible_before
        {
            return Err(anyhow::anyhow!(
                "checkpoint save failure did not roll back production state"
            ));
        }
        self.last_result = BatchIntegrationResult::CheckpointFailureStopsCommit;
        self.record_count = 1;
        self.ingest_error_count = 0;
        self.persistable_count = 2;
        self.iterator_checkpoint_count = 0;
        self.attempted_checkpoint_count = 1;
        self.checkpoint_error_count = 1;
        self.last_batch_clean = false;
        self.last_committed = false;
        self.last_iterator_persisted = false;
        self.last_lease_permitted = true;
        self.last_first_checkpoint_fails = true;
        self.last_checkpoint_saves_succeeded = false;
        self.malformed_record_count = 0;
        self.records_applied = 0;
        self.failure_recorded = false;
        Ok(())
    }

    fn check_empty_batch_can_persist_checkpoint(&mut self) -> Result {
        let (before_count, before_cursor) = self.begin_batch()?;
        let position = before_cursor.to_string();
        let shard = format!("empty-{before_count}");
        let checkpoint = engine_checkpoint(METRIC_SOURCE_TABLE, shard.as_str(), position.as_str())?;
        let (execution_id, fence) = self.next_effect_authority();
        fenced_source_batch(FencedSourceBatchRequest {
            engine: &self.engine,
            manifest: &self.manifest,
            execution_id,
            fence,
            source_table_name: METRIC_SOURCE_TABLE,
            shard_id: shard.as_str(),
            records: Vec::new(),
            checkpoints: vec![checkpoint],
        })?;
        if self.durable_checkpoint_count()? != before_count + 1
            || self.durable_source_cursor()? != before_cursor
        {
            return Err(anyhow::anyhow!(
                "empty batch did not persist its production checkpoint"
            ));
        }
        self.last_result = BatchIntegrationResult::EmptyBatchCheckpointCommit;
        self.record_count = 0;
        self.ingest_error_count = 0;
        self.persistable_count = 1;
        self.iterator_checkpoint_count = 0;
        self.attempted_checkpoint_count = 1;
        self.checkpoint_error_count = 0;
        self.last_batch_clean = true;
        self.last_committed = true;
        self.last_iterator_persisted = false;
        self.last_lease_permitted = true;
        self.last_first_checkpoint_fails = false;
        self.last_checkpoint_saves_succeeded = true;
        self.malformed_record_count = 0;
        self.records_applied = 0;
        self.failure_recorded = false;
        Ok(())
    }

    fn check_malformed_record_fails_closed(&mut self) -> Result {
        let (before_count, before_cursor) = self.begin_batch()?;
        let visible_before = metric_row_count(&self.engine)?;
        let next_position = before_cursor.saturating_add(1).to_string();
        let next_position_2 = before_cursor.saturating_add(2).to_string();
        let storage_checkpoints = vec![
            record_checkpoint(METRIC_SOURCE_TABLE, "shard-poison", next_position.as_str()),
            record_checkpoint(
                METRIC_SOURCE_TABLE,
                "shard-poison-next",
                next_position_2.as_str(),
            ),
            record_checkpoint(
                METRIC_SOURCE_TABLE,
                "__iterator:shard-poison",
                "iterator-token",
            ),
        ];
        let checkpoints = record_checkpoints(&storage_checkpoints);
        let mut malformed = metric_point_item("poison", "standard", "2");
        malformed.remove("tenant_id");
        let (execution_id, fence) = self.next_effect_authority();
        let result = fenced_source_batch(FencedSourceBatchRequest {
            engine: &self.engine,
            manifest: &self.manifest,
            execution_id,
            fence,
            source_table_name: METRIC_SOURCE_TABLE,
            shard_id: "shard-poison",
            records: vec![
                source_batch_item(
                    METRIC_SOURCE_TABLE,
                    "shard-poison",
                    METRIC_ANALYTICS_TABLE,
                    "valid-before",
                    metric_point_item("valid-before", "standard", "1"),
                ),
                source_batch_item(
                    METRIC_SOURCE_TABLE,
                    "shard-poison",
                    METRIC_ANALYTICS_TABLE,
                    "poison",
                    malformed,
                ),
                source_batch_item(
                    METRIC_SOURCE_TABLE,
                    "shard-poison",
                    METRIC_ANALYTICS_TABLE,
                    "valid-after",
                    metric_point_item("valid-after", "standard", "3"),
                ),
            ],
            checkpoints: vec![checkpoints[0].clone()],
        });
        if !result_is_engine_error(&result, |error| {
            matches!(error, AnalyticsEngineError::MissingAttribute(_))
        }) || self.durable_checkpoint_count()? != before_count
            || self.durable_source_cursor()? != before_cursor
            || metric_row_count(&self.engine)? != visible_before
        {
            return Err(anyhow::anyhow!(
                "malformed source record did not fail closed at the production boundary"
            ));
        }
        let failures = self.engine.load_source_batch_failures()?;
        let failure = failures
            .iter()
            .find(|failure| failure.repaired_at_ms.is_none())
            .ok_or_else(|| anyhow::anyhow!("malformed source record was not durably recorded"))?;
        if failure.source_table_name != METRIC_SOURCE_TABLE
            || failure.shard_id != "shard-poison"
            || failure.record_position != "poison"
        {
            return Err(anyhow::anyhow!(
                "malformed source failure lost exact production attribution"
            ));
        }
        self.last_result = BatchIntegrationResult::MalformedRecordBlocked;
        self.record_count = 3;
        self.ingest_error_count = 1;
        self.persistable_count = 2;
        self.iterator_checkpoint_count = 1;
        self.attempted_checkpoint_count = 0;
        self.checkpoint_error_count = 0;
        self.last_batch_clean = false;
        self.last_committed = false;
        self.last_iterator_persisted = false;
        self.last_lease_permitted = true;
        self.last_first_checkpoint_fails = false;
        self.last_checkpoint_saves_succeeded = true;
        self.malformed_record_count = 1;
        self.records_applied = 0;
        self.failure_recorded = true;
        Ok(())
    }

    fn check_poison_released_after_repair(&mut self) -> Result {
        let (before_count, before_cursor) = self.begin_batch()?;
        let failures = self.engine.load_source_batch_failures()?;
        let failure = failures
            .iter()
            .find(|failure| failure.repaired_at_ms.is_none())
            .ok_or_else(|| anyhow::anyhow!("operator repair had no unresolved poison record"))?;
        if !self.engine.repair_source_batch_failure(
            failure.source_table_name.as_str(),
            failure.shard_id.as_str(),
            failure.record_position.as_str(),
        )? {
            return Err(anyhow::anyhow!(
                "operator repair did not release the poison"
            ));
        }
        let unresolved = self
            .engine
            .load_source_batch_failures()?
            .into_iter()
            .any(|failure| failure.repaired_at_ms.is_none());
        if unresolved
            || self.durable_checkpoint_count()? != before_count
            || self.durable_source_cursor()? != before_cursor
        {
            return Err(anyhow::anyhow!(
                "poison repair changed durable state or left the source blocked"
            ));
        }
        self.last_result = BatchIntegrationResult::PoisonReleasedAfterRepair;
        self.record_count = 0;
        self.ingest_error_count = 0;
        self.persistable_count = 0;
        self.iterator_checkpoint_count = 0;
        self.attempted_checkpoint_count = 0;
        self.checkpoint_error_count = 0;
        self.last_batch_clean = true;
        self.last_committed = false;
        self.last_iterator_persisted = false;
        self.last_lease_permitted = true;
        self.last_first_checkpoint_fails = false;
        self.last_checkpoint_saves_succeeded = true;
        self.malformed_record_count = 0;
        self.records_applied = 0;
        self.durable_source_cursor_before = before_cursor;
        self.last_record_position = before_cursor;
        self.failure_recorded = false;
        Ok(())
    }
}

const CHECKPOINT_FAILURE_TABLE_DDL: &str = "DROP TABLE analytics_checkpoints; CREATE TABLE \
                                            analytics_checkpoints (source_table_name VARCHAR NOT \
                                            NULL, shard_id VARCHAR NOT NULL, position VARCHAR NOT \
                                            NULL, updated_at_ms BIGINT NOT NULL, CHECK (false));";

fn restore_checkpoint_table(
    engine: &AnalyticsEngine,
    checkpoints: &[EngineRecordCheckpoint],
) -> Result {
    engine.connection().execute_batch(
        "DROP TABLE analytics_checkpoints;
         CREATE TABLE analytics_checkpoints (
             source_table_name VARCHAR NOT NULL,
             shard_id VARCHAR NOT NULL,
             position VARCHAR NOT NULL,
             updated_at_ms BIGINT NOT NULL
         );",
    )?;
    if !checkpoints.is_empty() {
        engine.save_source_checkpoints(checkpoints)?;
    }
    Ok(())
}

fn durable_source_cursor(checkpoints: &[EngineRecordCheckpoint]) -> Result<i64> {
    let mut cursor = 0;
    for checkpoint in checkpoints {
        let position = checkpoint.position().parse::<i64>().map_err(|error| {
            anyhow::anyhow!(
                "production checkpoint position {} is not numeric: {error}",
                checkpoint.position()
            )
        })?;
        cursor = cursor.max(position);
    }
    Ok(cursor)
}

fn api_health_state(health: &SourceHealth) -> ApiHealthState {
    match health.status {
        SourceHealthStatus::Healthy => ApiHealthState::Healthy,
        SourceHealthStatus::Degraded => ApiHealthState::Degraded,
        SourceHealthStatus::Disabled => ApiHealthState::Disabled,
        SourceHealthStatus::Starting => ApiHealthState::Starting,
    }
}

fn metric_row_count(engine: &AnalyticsEngine) -> Result<i64> {
    let rows =
        engine.query_unscoped_sql_json("select count(*) as row_count from metric_points_v1")?;
    rows.first()
        .and_then(|row| row.get("row_count"))
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| anyhow::anyhow!("metric row count was not an integer"))
}

struct FencedSourceBatchRequest<'a> {
    engine: &'a AnalyticsEngine,
    manifest: &'a AnalyticsManifest,
    execution_id: [u8; 16],
    fence: u64,
    source_table_name: &'a str,
    shard_id: &'a str,
    records: Vec<StreamRecordBatchItem>,
    checkpoints: Vec<EngineRecordCheckpoint>,
}

fn fenced_source_batch(fixture: FencedSourceBatchRequest<'_>) -> Result<SourceEffectSummary> {
    let effect_request = SourceEffectRequest::try_new(SourceEffectRequestInput {
        manifest: fixture.manifest,
        manifest_digest: analytics_engine::manifest_digest(fixture.manifest),
        authority: SourceEffectAuthority::try_new(fixture.execution_id, fixture.fence)?,
        scope: SourceEffectScope::try_new(fixture.source_table_name, fixture.shard_id)?,
        records: fixture
            .records
            .into_iter()
            .map(|item| SourceBatchRecord {
                item,
                retention: None,
            })
            .collect(),
        checkpoints: fixture.checkpoints,
        privacy_policy: None,
    })?;
    match fixture.engine.ingest_fenced_source_batch(effect_request)? {
        SourceEffectDisposition::Applied { summary }
        | SourceEffectDisposition::AlreadyApplied { summary } => Ok(summary),
        SourceEffectDisposition::StaleFence => Err(anyhow::anyhow!(
            "source effect was rejected by a stale fence"
        )),
    }
}

fn result_is_engine_error<T>(
    result: &Result<T>,
    predicate: impl FnOnce(&AnalyticsEngineError) -> bool,
) -> bool {
    result
        .as_ref()
        .err()
        .and_then(|error| error.downcast_ref::<AnalyticsEngineError>())
        .is_some_and(predicate)
}

fn source_effect_execution_id(effect_id: u64) -> [u8; 16] {
    let mut execution_id = [0; 16];
    execution_id[..8].copy_from_slice(&effect_id.to_le_bytes());
    execution_id
}

fn engine_checkpoint(
    source_table_name: &str,
    shard_id: &str,
    position: &str,
) -> Result<EngineRecordCheckpoint> {
    Ok(EngineRecordCheckpoint::try_new(
        source_table_name,
        shard_id,
        position,
    )?)
}

fn record_checkpoint(source_table_name: &str, shard_id: &str, position: &str) -> SourceCheckpoint {
    SourceCheckpoint {
        source_table_name: source_table_name.to_string(),
        shard_id: shard_id.to_string(),
        position: position.to_string(),
    }
}

fn source_batch_item(
    source_table_name: &str,
    shard_id: &str,
    analytics_table_name: &str,
    record_key: &str,
    item: StorageItem,
) -> StreamRecordBatchItem {
    StreamRecordBatchItem {
        analytics_table_name: analytics_table_name.to_string(),
        source_table_name: source_table_name.to_string(),
        shard_id: Some(shard_id.to_string()),
        system_position: None,
        record_key: record_key.as_bytes().to_vec(),
        record: StorageStreamRecord {
            sequence_number: record_key.to_string(),
            keys: StorageItem::new(),
            old_image: None,
            new_image: Some(item),
        },
    }
}

fn metric_point_item(event_id: &str, model_name: &str, value: &str) -> StorageItem {
    StorageItem::from_iter([
        (
            "tenant_id".to_string(),
            StorageValue::S("tenant_01".to_string()),
        ),
        (
            "series_name".to_string(),
            StorageValue::S("ai_token_units".to_string()),
        ),
        (
            "customer_account_id".to_string(),
            StorageValue::S("bcus_123".to_string()),
        ),
        ("org_id".to_string(), StorageValue::NULL(true)),
        (
            "occurred_at_ms".to_string(),
            StorageValue::N("1782863999000".to_string()),
        ),
        (
            "ingested_at_ms".to_string(),
            StorageValue::N("1782864000000".to_string()),
        ),
        (
            "event_id".to_string(),
            StorageValue::S(event_id.to_string()),
        ),
        ("value_i64".to_string(), StorageValue::N(value.to_string())),
        ("value_decimal".to_string(), StorageValue::NULL(true)),
        (
            "dim_1".to_string(),
            StorageValue::S("eu-west-2".to_string()),
        ),
        ("dim_2".to_string(), StorageValue::S(model_name.to_string())),
        ("dim_3".to_string(), StorageValue::S("tokens".to_string())),
        ("source".to_string(), StorageValue::S("test".to_string())),
        (
            "schema_version".to_string(),
            StorageValue::N("1".to_string()),
        ),
    ])
}
