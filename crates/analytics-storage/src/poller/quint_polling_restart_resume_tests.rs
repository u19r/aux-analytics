use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::{
    SourceCheckpoint,
    aws_stream::AwsShardIteratorState,
    poller::{apply_aws_iterator_checkpoint, checkpoint_position, iterator_checkpoint_shard_id},
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum RestartResumeResult {
    NotChecked,
    AwsRecordCheckpointSelected,
    AwsIteratorCheckpointIgnoredForSequence,
    AwsIteratorCheckpointApplied,
    AwsWrongTableIgnored,
    AwsWrongShardIgnored,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum SourceTable {
    #[serde(rename = "UsersTable")]
    Users,
    #[serde(rename = "AccountsTable")]
    Accounts,
    #[serde(rename = "UnknownTable")]
    Unknown,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum SourceShard {
    Shard1,
    Shard2,
    UnknownShard,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct RestartResumeState {
    #[serde(rename = "lastResult")]
    last_result: RestartResumeResult,
    #[serde(rename = "durableCheckpointPresent")]
    durable_checkpoint_present: bool,
    #[serde(rename = "durableTable")]
    durable_table: SourceTable,
    #[serde(rename = "durableShard")]
    durable_shard: SourceShard,
    #[serde(rename = "durablePosition")]
    durable_position: String,
    #[serde(rename = "resumedTable")]
    resumed_table: SourceTable,
    #[serde(rename = "resumedShard")]
    resumed_shard: SourceShard,
    #[serde(rename = "resumedPosition")]
    resumed_position: String,
    #[serde(rename = "iteratorPresent")]
    iterator_present: bool,
    #[serde(rename = "iteratorTable")]
    iterator_table: SourceTable,
    #[serde(rename = "iteratorShard")]
    iterator_shard: SourceShard,
    #[serde(rename = "iteratorPosition")]
    iterator_position: String,
}

impl State<RestartResumeDriver> for RestartResumeState {
    fn from_driver(driver: &RestartResumeDriver) -> Result<Self> {
        Ok(driver.state.clone())
    }
}

struct RestartResumeDriver {
    state: RestartResumeState,
    iterator_states: Vec<AwsShardIteratorState>,
}

impl Default for RestartResumeDriver {
    fn default() -> Self {
        Self {
            state: RestartResumeState {
                last_result: RestartResumeResult::NotChecked,
                durable_checkpoint_present: false,
                durable_table: SourceTable::Unknown,
                durable_shard: SourceShard::UnknownShard,
                durable_position: String::new(),
                resumed_table: SourceTable::Unknown,
                resumed_shard: SourceShard::UnknownShard,
                resumed_position: String::new(),
                iterator_present: false,
                iterator_table: SourceTable::Unknown,
                iterator_shard: SourceShard::UnknownShard,
                iterator_position: String::new(),
            },
            iterator_states: vec![AwsShardIteratorState {
                shard_id: "shard-0001".to_string(),
                iterator: "old-iterator".to_string(),
            }],
        }
    }
}

impl Driver for RestartResumeDriver {
    type State = RestartResumeState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => *self = Self::default(),
            CheckAwsRecordCheckpointSelected => check_aws_record_checkpoint_selected(self)?,
            CheckAwsIteratorCheckpointIgnoredForSequence => {
                check_aws_iterator_checkpoint_ignored_for_sequence(self)?;
            },
            CheckAwsIteratorCheckpointApplied => check_aws_iterator_checkpoint_applied(self)?,
            CheckAwsWrongTableIgnored => check_aws_wrong_table_ignored(self)?,
            CheckAwsWrongShardIgnored => check_aws_wrong_shard_ignored(self)?,
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/polling_restart_resume.qnt",
    max_samples = 50,
    max_steps = 8,
    seed = "0x515e"
)]
fn given_quint_polling_restart_resume_model_when_replayed_then_storage_rehydration_matches()
-> impl Driver {
    RestartResumeDriver::default()
}

fn check_aws_record_checkpoint_selected(driver: &mut RestartResumeDriver) -> Result<()> {
    let checkpoints = vec![record_checkpoint("source_users", "shard-0001", "seq-42")];
    let Some(position) = checkpoint_position(&checkpoints, "source_users", "shard-0001") else {
        return Err(anyhow::anyhow!(
            "AWS record checkpoint was not selected for shard resume"
        ));
    };
    if position != "seq-42" {
        return Err(anyhow::anyhow!(
            "AWS record checkpoint returned an unexpected position"
        ));
    }
    driver.state.last_result = RestartResumeResult::AwsRecordCheckpointSelected;
    driver.state.durable_checkpoint_present = true;
    driver.state.durable_table = SourceTable::Users;
    driver.state.durable_shard = SourceShard::Shard1;
    driver.state.durable_position = position;
    driver.state.resumed_table = driver.state.durable_table;
    driver.state.resumed_shard = driver.state.durable_shard;
    driver.state.resumed_position = driver.state.durable_position.clone();
    Ok(())
}

fn check_aws_iterator_checkpoint_ignored_for_sequence(
    driver: &mut RestartResumeDriver,
) -> Result<()> {
    let checkpoint = record_checkpoint("source_users", "__iterator:shard-0001", "next-iterator");
    let sequence_position = checkpoint_position(
        std::slice::from_ref(&checkpoint),
        "source_users",
        "shard-0001",
    );
    let iterator_shard_id = iterator_checkpoint_shard_id(&checkpoint);
    if sequence_position.is_some() || iterator_shard_id != Some("shard-0001") {
        return Err(anyhow::anyhow!(
            "AWS iterator checkpoint was treated as a durable sequence checkpoint"
        ));
    }
    driver.state.last_result = RestartResumeResult::AwsIteratorCheckpointIgnoredForSequence;
    Ok(())
}

fn check_aws_iterator_checkpoint_applied(driver: &mut RestartResumeDriver) -> Result<()> {
    let checkpoint = record_checkpoint("source_users", "__iterator:shard-0001", "new-iterator");
    if !apply_aws_iterator_checkpoint("source_users", &mut driver.iterator_states, &checkpoint) {
        return Err(anyhow::anyhow!(
            "AWS iterator checkpoint was not applied to the matching shard"
        ));
    }
    let Some(state) = driver
        .iterator_states
        .iter()
        .find(|state| state.shard_id == "shard-0001")
    else {
        return Err(anyhow::anyhow!(
            "AWS iterator state disappeared after application"
        ));
    };
    driver.state.last_result = RestartResumeResult::AwsIteratorCheckpointApplied;
    driver.state.iterator_present = true;
    driver.state.iterator_table = SourceTable::Users;
    driver.state.iterator_shard = SourceShard::Shard1;
    driver.state.iterator_position.clone_from(&state.iterator);
    Ok(())
}

fn check_aws_wrong_table_ignored(driver: &mut RestartResumeDriver) -> Result<()> {
    let checkpoint = record_checkpoint("source_accounts", "shard-0001", "seq-55");
    if checkpoint_position(
        std::slice::from_ref(&checkpoint),
        "source_users",
        "shard-0001",
    )
    .is_some()
    {
        return Err(anyhow::anyhow!(
            "AWS resume selected a checkpoint for the wrong source table"
        ));
    }
    driver.state.last_result = RestartResumeResult::AwsWrongTableIgnored;
    Ok(())
}

fn check_aws_wrong_shard_ignored(driver: &mut RestartResumeDriver) -> Result<()> {
    let checkpoint = record_checkpoint("source_users", "shard-0002", "seq-55");
    if checkpoint_position(
        std::slice::from_ref(&checkpoint),
        "source_users",
        "shard-0001",
    )
    .is_some()
    {
        return Err(anyhow::anyhow!(
            "AWS resume selected a checkpoint for the wrong source shard"
        ));
    }
    driver.state.last_result = RestartResumeResult::AwsWrongShardIgnored;
    Ok(())
}

fn record_checkpoint(source_table_name: &str, shard_id: &str, position: &str) -> SourceCheckpoint {
    SourceCheckpoint {
        source_table_name: source_table_name.to_string(),
        shard_id: shard_id.to_string(),
        position: position.to_string(),
    }
}
