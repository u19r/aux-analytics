use analytics_api::{SourceHealth, SourceHealthStatus};
use analytics_engine::SourceCheckpoint as EngineSourceCheckpoint;
use analytics_storage::SourceCheckpoint;
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::source_polling::{
    SourcePollingStartup, apply_source_poll_error_health, apply_source_success_health,
    is_iterator_checkpoint, persistable_checkpoints, source_batch_integration_outcome,
    source_batch_outcome, source_polling_startup, storage_checkpoints_from_engine,
};

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

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct ApiPollingState {
    #[serde(rename = "lastResult")]
    last_result: ApiPollingResult,
}

impl State<ApiPollingDriver> for ApiPollingState {
    fn from_driver(driver: &ApiPollingDriver) -> Result<Self> {
        Ok(Self {
            last_result: driver.last_result,
        })
    }
}

struct ApiPollingDriver {
    last_result: ApiPollingResult,
}

impl Default for ApiPollingDriver {
    fn default() -> Self {
        Self {
            last_result: ApiPollingResult::NotChecked,
        }
    }
}

impl Driver for ApiPollingDriver {
    type State = ApiPollingState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.last_result = ApiPollingResult::NotChecked,
            CheckPersistableCheckpointsFiltered => {
                self.last_result = check_persistable_checkpoints_filtered()?;
            },
            CheckSuccessfulBatchCommits => self.last_result = check_successful_batch_commits()?,
            CheckIngestFailureBlocksCommit => {
                self.last_result = check_ingest_failure_blocks_commit()?;
            },
            CheckCheckpointFailureBlocksCommit => {
                self.last_result = check_checkpoint_failure_blocks_commit()?;
            },
            CheckSuccessHealth => self.last_result = check_success_health()?,
            CheckPartialFailureHealth => self.last_result = check_partial_failure_health()?,
            CheckPollErrorHealth => self.last_result = check_poll_error_health()?,
            CheckStartupDisabled => self.last_result = check_startup_disabled()?,
            CheckStartupStarting => self.last_result = check_startup_starting()?,
            CheckStartupCheckpointMapping => {
                self.last_result = check_startup_checkpoint_mapping()?;
            },
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/polling_api.qnt",
    max_samples = 50,
    max_steps = 8,
    seed = "0x6"
)]
fn quint_polling_api_model_matches_source_polling() -> impl Driver {
    ApiPollingDriver::default()
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
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct BatchIntegrationState {
    #[serde(rename = "lastResult")]
    last_result: BatchIntegrationResult,
}

impl State<BatchIntegrationDriver> for BatchIntegrationState {
    fn from_driver(driver: &BatchIntegrationDriver) -> Result<Self> {
        Ok(Self {
            last_result: driver.last_result,
        })
    }
}

struct BatchIntegrationDriver {
    last_result: BatchIntegrationResult,
}

impl Default for BatchIntegrationDriver {
    fn default() -> Self {
        Self {
            last_result: BatchIntegrationResult::NotChecked,
        }
    }
}

impl Driver for BatchIntegrationDriver {
    type State = BatchIntegrationState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.last_result = BatchIntegrationResult::NotChecked,
            CheckAllRecordsAndCheckpointsSucceed => {
                self.last_result = check_all_records_and_checkpoints_succeed()?;
            },
            CheckIngestFailureSkipsCheckpointSaves => {
                self.last_result = check_ingest_failure_skips_checkpoint_saves()?;
            },
            CheckIteratorCheckpointFiltered => {
                self.last_result = check_iterator_checkpoint_filtered()?;
            },
            CheckCheckpointSaveFailureStopsOrdering => {
                self.last_result = check_checkpoint_save_failure_stops_ordering()?;
            },
            CheckEmptyBatchCanPersistCheckpoint => {
                self.last_result = check_empty_batch_can_persist_checkpoint()?;
            },
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/source_batch_integration.qnt",
    max_samples = 50,
    max_steps = 8,
    seed = "0x9"
)]
fn quint_source_batch_integration_model_matches_polling_outcome() -> impl Driver {
    BatchIntegrationDriver::default()
}

fn check_persistable_checkpoints_filtered() -> Result<ApiPollingResult> {
    let checkpoints = vec![
        record_checkpoint("source_users", "shard-0001", "seq-2"),
        record_checkpoint("source_users", "__iterator:shard-0001", "next-iterator"),
    ];
    let persistable = persistable_checkpoints(&checkpoints).collect::<Vec<_>>();
    if persistable.len() == 1
        && persistable[0].shard_id == "shard-0001"
        && !is_iterator_checkpoint(persistable[0])
        && is_iterator_checkpoint(&checkpoints[1])
    {
        Ok(ApiPollingResult::PersistableCheckpointsFiltered)
    } else {
        Err(anyhow::anyhow!(
            "iterator checkpoints were not filtered before persistence"
        ))
    }
}

fn check_all_records_and_checkpoints_succeed() -> Result<BatchIntegrationResult> {
    let outcome = source_batch_integration_outcome(
        &[true, true],
        &[
            record_checkpoint("source_users", "shard-0001", "seq-2"),
            record_checkpoint("source_users", "shard-0002", "seq-4"),
        ],
        &[true, true],
    );
    if outcome.should_commit
        && outcome.all_records_ingested
        && outcome.records_ingested == 2
        && outcome.checkpoints_saved == 2
        && outcome.checkpoint_errors == 0
    {
        Ok(BatchIntegrationResult::SuccessfulCommit)
    } else {
        Err(anyhow::anyhow!(
            "successful source batch integration did not commit"
        ))
    }
}

fn check_ingest_failure_skips_checkpoint_saves() -> Result<BatchIntegrationResult> {
    let outcome = source_batch_integration_outcome(
        &[true, false],
        &[record_checkpoint("source_users", "shard-0001", "seq-2")],
        &[true],
    );
    if !outcome.should_commit
        && !outcome.all_records_ingested
        && outcome.records_ingested == 1
        && outcome.ingest_errors == 1
        && outcome.checkpoints_saved == 0
        && outcome.checkpoint_errors == 0
    {
        Ok(BatchIntegrationResult::IngestFailureSkippedCheckpoints)
    } else {
        Err(anyhow::anyhow!(
            "ingest failure did not skip checkpoint saves"
        ))
    }
}

fn check_iterator_checkpoint_filtered() -> Result<BatchIntegrationResult> {
    let outcome = source_batch_integration_outcome(
        &[true],
        &[
            record_checkpoint("source_users", "shard-0001", "seq-2"),
            record_checkpoint("source_users", "__iterator:shard-0001", "next-iterator"),
        ],
        &[true, false],
    );
    if outcome.should_commit
        && outcome.checkpoints_saved == 1
        && outcome.checkpoint_errors == 0
        && outcome.records_ingested == 1
    {
        Ok(BatchIntegrationResult::IteratorCheckpointFiltered)
    } else {
        Err(anyhow::anyhow!(
            "iterator checkpoint affected durable checkpoint persistence"
        ))
    }
}

fn check_checkpoint_save_failure_stops_ordering() -> Result<BatchIntegrationResult> {
    let outcome = source_batch_integration_outcome(
        &[true],
        &[
            record_checkpoint("source_users", "shard-0001", "seq-2"),
            record_checkpoint("source_users", "shard-0002", "seq-4"),
        ],
        &[false, true],
    );
    if !outcome.should_commit
        && !outcome.all_records_ingested
        && outcome.checkpoints_saved == 0
        && outcome.checkpoint_errors == 1
    {
        Ok(BatchIntegrationResult::CheckpointFailureStopsCommit)
    } else {
        Err(anyhow::anyhow!(
            "checkpoint failure did not stop commit ordering"
        ))
    }
}

fn check_empty_batch_can_persist_checkpoint() -> Result<BatchIntegrationResult> {
    let outcome = source_batch_integration_outcome(
        &[],
        &[record_checkpoint("source_users", "shard-0001", "seq-2")],
        &[true],
    );
    if outcome.should_commit
        && outcome.records_ingested == 0
        && outcome.ingest_errors == 0
        && outcome.checkpoints_saved == 1
    {
        Ok(BatchIntegrationResult::EmptyBatchCheckpointCommit)
    } else {
        Err(anyhow::anyhow!(
            "empty successful batch did not persist checkpoint"
        ))
    }
}

fn check_successful_batch_commits() -> Result<ApiPollingResult> {
    let outcome = source_batch_outcome(&[true, true], &[true, true]);
    if outcome.all_records_ingested
        && outcome.should_commit
        && outcome.records_ingested == 2
        && outcome.checkpoints_saved == 2
        && outcome.ingest_errors == 0
        && outcome.checkpoint_errors == 0
    {
        Ok(ApiPollingResult::SuccessfulBatchCommits)
    } else {
        Err(anyhow::anyhow!(
            "successful source batch was not committable"
        ))
    }
}

fn check_ingest_failure_blocks_commit() -> Result<ApiPollingResult> {
    let outcome = source_batch_outcome(&[true, false], &[]);
    if !outcome.all_records_ingested
        && !outcome.should_commit
        && outcome.records_ingested == 1
        && outcome.ingest_errors == 1
        && outcome.checkpoints_saved == 0
    {
        Ok(ApiPollingResult::IngestFailureBlocksCommit)
    } else {
        Err(anyhow::anyhow!(
            "ingest failure did not block checkpoint commit"
        ))
    }
}

fn check_checkpoint_failure_blocks_commit() -> Result<ApiPollingResult> {
    let outcome = source_batch_outcome(&[true], &[true, false]);
    if !outcome.all_records_ingested
        && !outcome.should_commit
        && outcome.records_ingested == 1
        && outcome.checkpoints_saved == 1
        && outcome.checkpoint_errors == 1
    {
        Ok(ApiPollingResult::CheckpointFailureBlocksCommit)
    } else {
        Err(anyhow::anyhow!(
            "checkpoint save failure did not block iterator commit"
        ))
    }
}

fn check_success_health() -> Result<ApiPollingResult> {
    let mut health = SourceHealth::starting(1);
    health.last_error = Some("previous failure".to_string());
    apply_source_success_health(
        &mut health,
        true,
        2,
        0,
        1,
        0,
        &[record_checkpoint("source_users", "shard-0001", "seq-2")],
        500,
    );
    if matches!(health.status, SourceHealthStatus::Healthy)
        && health.last_success_at_ms == Some(500)
        && health.last_error.is_none()
        && health.total_records_ingested == 2
        && health.total_checkpoints_saved == 1
        && health.checkpoints.len() == 1
        && health.checkpoints[0].position == "seq-2"
    {
        Ok(ApiPollingResult::SuccessHealthUpdated)
    } else {
        Err(anyhow::anyhow!(
            "successful poll health did not match model"
        ))
    }
}

fn check_partial_failure_health() -> Result<ApiPollingResult> {
    let mut health = SourceHealth::starting(1);
    health.last_error = Some("ingest failed".to_string());
    apply_source_success_health(
        &mut health,
        false,
        1,
        1,
        0,
        1,
        &[
            record_checkpoint("source_users", "shard-0001", "seq-2"),
            record_checkpoint("source_users", "__iterator:shard-0001", "next-iterator"),
        ],
        600,
    );
    if matches!(health.status, SourceHealthStatus::Degraded)
        && health.last_success_at_ms == Some(600)
        && health.last_error.as_deref() == Some("ingest failed")
        && health.total_records_ingested == 1
        && health.total_ingest_errors == 1
        && health.total_checkpoint_errors == 1
        && health.checkpoints.len() == 1
        && health.checkpoints[0].shard_id == "shard-0001"
    {
        Ok(ApiPollingResult::PartialFailureHealthDegraded)
    } else {
        Err(anyhow::anyhow!(
            "partial poll failure health did not match model"
        ))
    }
}

fn check_poll_error_health() -> Result<ApiPollingResult> {
    let mut health = SourceHealth::starting(1);
    health.total_poll_errors = 3;
    apply_source_poll_error_health(&mut health, "poll failed".to_string(), 700);
    if matches!(health.status, SourceHealthStatus::Degraded)
        && health.last_error_at_ms == Some(700)
        && health.last_error.as_deref() == Some("poll failed")
        && health.total_poll_errors == 4
    {
        Ok(ApiPollingResult::PollErrorHealthDegraded)
    } else {
        Err(anyhow::anyhow!("poll error health did not match model"))
    }
}

fn check_startup_disabled() -> Result<ApiPollingResult> {
    if source_polling_startup(0, false) == SourcePollingStartup::Disabled
        && source_polling_startup(2, true) == SourcePollingStartup::Disabled
    {
        Ok(ApiPollingResult::StartupDisabled)
    } else {
        Err(anyhow::anyhow!(
            "disabled polling startup did not match model"
        ))
    }
}

fn check_startup_starting() -> Result<ApiPollingResult> {
    if source_polling_startup(2, false) == SourcePollingStartup::Starting {
        Ok(ApiPollingResult::StartupStarting)
    } else {
        Err(anyhow::anyhow!(
            "starting polling startup did not match model"
        ))
    }
}

fn check_startup_checkpoint_mapping() -> Result<ApiPollingResult> {
    let storage_checkpoints = storage_checkpoints_from_engine(vec![
        EngineSourceCheckpoint {
            source_table_name: "source_users".to_string(),
            shard_id: "aux-storage".to_string(),
            position: "cursor-users".to_string(),
        },
        EngineSourceCheckpoint {
            source_table_name: "source_events".to_string(),
            shard_id: "shard-0001".to_string(),
            position: "seq-42".to_string(),
        },
    ]);
    if storage_checkpoints.len() == 2
        && storage_checkpoints[0].source_table_name == "source_users"
        && storage_checkpoints[0].shard_id == "aux-storage"
        && storage_checkpoints[0].position == "cursor-users"
        && storage_checkpoints[1].source_table_name == "source_events"
        && storage_checkpoints[1].shard_id == "shard-0001"
        && storage_checkpoints[1].position == "seq-42"
    {
        Ok(ApiPollingResult::StartupCheckpointsLoaded)
    } else {
        Err(anyhow::anyhow!(
            "startup checkpoint mapping did not match model"
        ))
    }
}

fn record_checkpoint(source_table_name: &str, shard_id: &str, position: &str) -> SourceCheckpoint {
    SourceCheckpoint {
        source_table_name: source_table_name.to_string(),
        shard_id: shard_id.to_string(),
        position: position.to_string(),
    }
}
