use std::collections::HashMap;

use analytics_contract::StorageStreamRecord;
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::{
    SourceCheckpoint,
    aws_stream::AwsShardIteratorState,
    poller::{
        apply_aws_iterator_checkpoint, aws_stream_response_batch, expand_records,
        next_aux_storage_cursor,
    },
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum StoragePollingResult {
    NotChecked,
    FanOutComplete,
    AuxResumeKeyWins,
    AuxRecordCheckpointed,
    AuxTerminalStops,
    AwsIteratorAdvanced,
    AwsRecordAndIteratorCheckpointsEmitted,
    AwsIteratorOnlyCheckpointEmitted,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct StoragePollingState {
    #[serde(rename = "lastResult")]
    last_result: StoragePollingResult,
}

impl State<StoragePollingDriver> for StoragePollingState {
    fn from_driver(driver: &StoragePollingDriver) -> Result<Self> {
        Ok(Self {
            last_result: driver.last_result,
        })
    }
}

struct StoragePollingDriver {
    last_result: StoragePollingResult,
}

impl Default for StoragePollingDriver {
    fn default() -> Self {
        Self {
            last_result: StoragePollingResult::NotChecked,
        }
    }
}

impl Driver for StoragePollingDriver {
    type State = StoragePollingState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.last_result = StoragePollingResult::NotChecked,
            CheckFanOut => self.last_result = check_fan_out()?,
            CheckAuxResumeKeyWins => self.last_result = check_aux_resume_key_wins()?,
            CheckAuxRecordCheckpoint => self.last_result = check_aux_record_checkpoint()?,
            CheckAuxTerminalStops => self.last_result = check_aux_terminal_stops()?,
            CheckAwsIteratorAdvances => self.last_result = check_aws_iterator_advances()?,
            CheckAwsBatchCheckpoints => self.last_result = check_aws_batch_checkpoints()?,
            CheckAwsIteratorOnlyCheckpoint => {
                self.last_result = check_aws_iterator_only_checkpoint()?;
            },
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/polling_storage.qnt",
    max_samples = 50,
    max_steps = 8,
    seed = "0x5"
)]
fn quint_polling_storage_model_matches_storage_polling() -> impl Driver {
    StoragePollingDriver::default()
}

fn check_fan_out() -> Result<StoragePollingResult> {
    let expanded = expand_records(
        &[
            "analytics_users".to_string(),
            "analytics_accounts".to_string(),
        ],
        [stream_record("seq-1")],
    );
    if expanded.len() == 2
        && expanded[0].analytics_table_name == "analytics_users"
        && expanded[0].record_key == "seq-1"
        && expanded[1].analytics_table_name == "analytics_accounts"
        && expanded[1].record.sequence_number == "seq-1"
    {
        Ok(StoragePollingResult::FanOutComplete)
    } else {
        Err(anyhow::anyhow!("polling fan-out did not match model"))
    }
}

fn check_aux_resume_key_wins() -> Result<StoragePollingResult> {
    let cursor = next_aux_storage_cursor(Some("next-page".to_string()), Some("seq-2".to_string()));
    if cursor.as_deref() == Some("next-page") {
        Ok(StoragePollingResult::AuxResumeKeyWins)
    } else {
        Err(anyhow::anyhow!("aux-storage resume key did not win"))
    }
}

fn check_aux_record_checkpoint() -> Result<StoragePollingResult> {
    let cursor = next_aux_storage_cursor(None, Some("seq-2".to_string()));
    if cursor.as_deref() == Some("seq-2") {
        Ok(StoragePollingResult::AuxRecordCheckpointed)
    } else {
        Err(anyhow::anyhow!(
            "terminal aux-storage record was not checkpointed"
        ))
    }
}

fn check_aux_terminal_stops() -> Result<StoragePollingResult> {
    let cursor = next_aux_storage_cursor(None, None);
    if cursor.is_none() {
        Ok(StoragePollingResult::AuxTerminalStops)
    } else {
        Err(anyhow::anyhow!(
            "empty terminal aux-storage response did not stop polling"
        ))
    }
}

fn check_aws_iterator_advances() -> Result<StoragePollingResult> {
    let mut states = vec![AwsShardIteratorState {
        shard_id: "shard-0001".to_string(),
        iterator: "old-iterator".to_string(),
    }];
    let applied = apply_aws_iterator_checkpoint(
        "source_users",
        &mut states,
        &SourceCheckpoint {
            source_table_name: "source_users".to_string(),
            shard_id: "__iterator:shard-0001".to_string(),
            position: "new-iterator".to_string(),
        },
    );
    if applied && states[0].iterator == "new-iterator" {
        Ok(StoragePollingResult::AwsIteratorAdvanced)
    } else {
        Err(anyhow::anyhow!("AWS iterator checkpoint did not advance"))
    }
}

fn check_aws_batch_checkpoints() -> Result<StoragePollingResult> {
    let batch = aws_stream_response_batch(
        "source_users",
        &["users".to_string(), "accounts".to_string()],
        "shard-0001",
        vec![stream_record("seq-1"), stream_record("seq-2")],
        Some("next-iterator".to_string()),
    );
    if batch.records.len() == 4
        && batch.checkpoints.len() == 2
        && batch.checkpoints[0].shard_id == "shard-0001"
        && batch.checkpoints[0].position == "seq-2"
        && batch.checkpoints[1].shard_id == "__iterator:shard-0001"
        && batch.checkpoints[1].position == "next-iterator"
    {
        Ok(StoragePollingResult::AwsRecordAndIteratorCheckpointsEmitted)
    } else {
        Err(anyhow::anyhow!(
            "AWS record and iterator checkpoints did not match model"
        ))
    }
}

fn check_aws_iterator_only_checkpoint() -> Result<StoragePollingResult> {
    let batch = aws_stream_response_batch(
        "source_users",
        &["users".to_string()],
        "shard-0001",
        Vec::new(),
        Some("next-iterator".to_string()),
    );
    if batch.records.is_empty()
        && batch.checkpoints.len() == 1
        && batch.checkpoints[0].shard_id == "__iterator:shard-0001"
        && batch.checkpoints[0].position == "next-iterator"
    {
        Ok(StoragePollingResult::AwsIteratorOnlyCheckpointEmitted)
    } else {
        Err(anyhow::anyhow!(
            "AWS iterator-only checkpoint did not match model"
        ))
    }
}

fn stream_record(sequence_number: &str) -> StorageStreamRecord {
    StorageStreamRecord {
        sequence_number: sequence_number.to_string(),
        keys: HashMap::new(),
        old_image: None,
        new_image: None,
    }
}
