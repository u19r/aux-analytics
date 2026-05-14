use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::{
    SourceCheckpoint,
    aws_stream::AwsShardIteratorState,
    poller::{
        AUX_STORAGE_SHARD_ID, apply_aws_iterator_checkpoint, checkpoint_position,
        iterator_checkpoint_shard_id,
    },
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum RestartResumeResult {
    NotChecked,
    AuxPersistedCheckpointSelected,
    AuxWrongTableIgnored,
    AuxWrongShardIgnored,
    AwsRecordCheckpointSelected,
    AwsIteratorCheckpointIgnoredForSequence,
    AwsIteratorCheckpointApplied,
    AwsWrongTableIgnored,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct RestartResumeState {
    #[serde(rename = "lastResult")]
    last_result: RestartResumeResult,
}

impl State<RestartResumeDriver> for RestartResumeState {
    fn from_driver(driver: &RestartResumeDriver) -> Result<Self> {
        Ok(Self {
            last_result: driver.last_result,
        })
    }
}

struct RestartResumeDriver {
    last_result: RestartResumeResult,
}

impl Default for RestartResumeDriver {
    fn default() -> Self {
        Self {
            last_result: RestartResumeResult::NotChecked,
        }
    }
}

impl Driver for RestartResumeDriver {
    type State = RestartResumeState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.last_result = RestartResumeResult::NotChecked,
            CheckAuxPersistedCheckpointSelected => {
                self.last_result = check_aux_persisted_checkpoint_selected()?;
            },
            CheckAuxWrongTableIgnored => self.last_result = check_aux_wrong_table_ignored()?,
            CheckAuxWrongShardIgnored => self.last_result = check_aux_wrong_shard_ignored()?,
            CheckAwsRecordCheckpointSelected => {
                self.last_result = check_aws_record_checkpoint_selected()?;
            },
            CheckAwsIteratorCheckpointIgnoredForSequence => {
                self.last_result = check_aws_iterator_checkpoint_ignored_for_sequence()?;
            },
            CheckAwsIteratorCheckpointApplied => {
                self.last_result = check_aws_iterator_checkpoint_applied()?;
            },
            CheckAwsWrongTableIgnored => self.last_result = check_aws_wrong_table_ignored()?,
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/polling_restart_resume.qnt",
    max_samples = 50,
    max_steps = 8,
    seed = "0x515e"
)]
fn quint_polling_restart_resume_model_matches_storage_rehydration() -> impl Driver {
    RestartResumeDriver::default()
}

fn check_aux_persisted_checkpoint_selected() -> Result<RestartResumeResult> {
    let checkpoints = vec![record_checkpoint(
        "source_users",
        AUX_STORAGE_SHARD_ID,
        "cursor-users",
    )];
    let position = checkpoint_position(&checkpoints, "source_users", AUX_STORAGE_SHARD_ID);
    if position.as_deref() == Some("cursor-users") {
        Ok(RestartResumeResult::AuxPersistedCheckpointSelected)
    } else {
        Err(anyhow::anyhow!(
            "aux-storage persisted checkpoint was not selected for resume"
        ))
    }
}

fn check_aux_wrong_table_ignored() -> Result<RestartResumeResult> {
    let checkpoints = vec![record_checkpoint(
        "source_accounts",
        AUX_STORAGE_SHARD_ID,
        "cursor-accounts",
    )];
    let position = checkpoint_position(&checkpoints, "source_users", AUX_STORAGE_SHARD_ID);
    if position.is_none() {
        Ok(RestartResumeResult::AuxWrongTableIgnored)
    } else {
        Err(anyhow::anyhow!(
            "aux-storage resume selected a checkpoint for the wrong source table"
        ))
    }
}

fn check_aux_wrong_shard_ignored() -> Result<RestartResumeResult> {
    let checkpoints = vec![record_checkpoint(
        "source_users",
        "other-shard",
        "cursor-users",
    )];
    let position = checkpoint_position(&checkpoints, "source_users", AUX_STORAGE_SHARD_ID);
    if position.is_none() {
        Ok(RestartResumeResult::AuxWrongShardIgnored)
    } else {
        Err(anyhow::anyhow!(
            "aux-storage resume selected a checkpoint for the wrong shard"
        ))
    }
}

fn check_aws_record_checkpoint_selected() -> Result<RestartResumeResult> {
    let checkpoints = vec![record_checkpoint("source_users", "shard-0001", "seq-42")];
    let position = checkpoint_position(&checkpoints, "source_users", "shard-0001");
    if position.as_deref() == Some("seq-42") {
        Ok(RestartResumeResult::AwsRecordCheckpointSelected)
    } else {
        Err(anyhow::anyhow!(
            "AWS record checkpoint was not selected for shard resume"
        ))
    }
}

fn check_aws_iterator_checkpoint_ignored_for_sequence() -> Result<RestartResumeResult> {
    let checkpoint = record_checkpoint("source_users", "__iterator:shard-0001", "next-iterator");
    let checkpoints = vec![checkpoint];
    let sequence_position = checkpoint_position(&checkpoints, "source_users", "shard-0001");
    let iterator_shard_id = iterator_checkpoint_shard_id(&checkpoints[0]);
    if sequence_position.is_none() && iterator_shard_id == Some("shard-0001") {
        Ok(RestartResumeResult::AwsIteratorCheckpointIgnoredForSequence)
    } else {
        Err(anyhow::anyhow!(
            "AWS iterator checkpoint was treated as a record sequence checkpoint"
        ))
    }
}

fn check_aws_iterator_checkpoint_applied() -> Result<RestartResumeResult> {
    let mut states = vec![AwsShardIteratorState {
        shard_id: "shard-0001".to_string(),
        iterator: "old-iterator".to_string(),
    }];
    let applied = apply_aws_iterator_checkpoint(
        "source_users",
        &mut states,
        &record_checkpoint("source_users", "__iterator:shard-0001", "new-iterator"),
    );
    if applied && states[0].iterator == "new-iterator" {
        Ok(RestartResumeResult::AwsIteratorCheckpointApplied)
    } else {
        Err(anyhow::anyhow!(
            "AWS iterator checkpoint was not applied to the matching shard"
        ))
    }
}

fn check_aws_wrong_table_ignored() -> Result<RestartResumeResult> {
    let mut states = vec![AwsShardIteratorState {
        shard_id: "shard-0001".to_string(),
        iterator: "old-iterator".to_string(),
    }];
    let applied = apply_aws_iterator_checkpoint(
        "source_users",
        &mut states,
        &record_checkpoint("source_accounts", "__iterator:shard-0001", "new-iterator"),
    );
    if !applied && states[0].iterator == "old-iterator" {
        Ok(RestartResumeResult::AwsWrongTableIgnored)
    } else {
        Err(anyhow::anyhow!(
            "AWS iterator resume applied a checkpoint for the wrong source table"
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
