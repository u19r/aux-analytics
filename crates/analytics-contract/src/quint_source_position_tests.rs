use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::{MergeDecision, SourceMutation, SourcePosition, merge_decision};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum ExistingPosition {
    NoExisting,
    StreamOld,
    StreamNew,
    Snapshot,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum IncomingPosition {
    #[serde(rename = "IncomingStreamOld")]
    StreamOld,
    #[serde(rename = "IncomingStreamNew")]
    StreamNew,
    #[serde(rename = "IncomingSnapshot")]
    Snapshot,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum ModelMutation {
    Upsert,
    Delete,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum MergeResult {
    NotChecked,
    InsertRow,
    ReplaceRow,
    DeleteRow,
    IgnoreDuplicate,
    IgnoreStale,
    AppendRow,
    RequiresValidation,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct SourcePositionMbtState {
    #[serde(rename = "existingPosition")]
    existing_position: ExistingPosition,
    #[serde(rename = "incomingPosition")]
    incoming_position: IncomingPosition,
    mutation: ModelMutation,
    #[serde(rename = "appendOnly")]
    append_only: bool,
    #[serde(rename = "lastResult")]
    last_result: MergeResult,
}

impl State<SourcePositionDriver> for SourcePositionMbtState {
    fn from_driver(driver: &SourcePositionDriver) -> Result<Self> {
        Ok(Self {
            existing_position: driver.existing_position,
            incoming_position: driver.incoming_position,
            mutation: driver.mutation,
            append_only: driver.append_only,
            last_result: driver.last_result,
        })
    }
}

struct SourcePositionDriver {
    existing_position: ExistingPosition,
    incoming_position: IncomingPosition,
    mutation: ModelMutation,
    append_only: bool,
    last_result: MergeResult,
}

impl Default for SourcePositionDriver {
    fn default() -> Self {
        Self {
            existing_position: ExistingPosition::NoExisting,
            incoming_position: IncomingPosition::StreamOld,
            mutation: ModelMutation::Upsert,
            append_only: false,
            last_result: MergeResult::NotChecked,
        }
    }
}

impl Driver for SourcePositionDriver {
    type State = SourcePositionMbtState;

    #[allow(non_snake_case)]
    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => *self = Self::default(),
            CheckMerge(
                existing: ExistingPosition,
                incoming: IncomingPosition,
                change: ModelMutation,
                isAppendOnly: bool,
            ) => {
                self.existing_position = existing;
                self.incoming_position = incoming;
                self.mutation = change;
                self.append_only = isAppendOnly;
                self.last_result = check_merge(existing, incoming, change, isAppendOnly)?;
            },
            step(
                existing: ExistingPosition?,
                incoming: IncomingPosition?,
                change: ModelMutation?,
                isAppendOnly: bool?,
            ) => {
                if let (Some(existing), Some(incoming), Some(change), Some(is_append_only)) =
                    (existing, incoming, change, isAppendOnly)
                {
                    self.existing_position = existing;
                    self.incoming_position = incoming;
                    self.mutation = change;
                    self.append_only = is_append_only;
                    self.last_result = check_merge(existing, incoming, change, is_append_only)?;
                }
            },
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/source_position_mbt.qnt",
    max_samples = 80,
    max_steps = 6,
    seed = "0x5164"
)]
fn given_quint_source_position_model_when_replayed_then_merge_decisions_matches() -> impl Driver {
    SourcePositionDriver::default()
}

fn check_merge(
    existing: ExistingPosition,
    incoming: IncomingPosition,
    mutation: ModelMutation,
    append_only: bool,
) -> Result<MergeResult> {
    let existing = match existing {
        ExistingPosition::NoExisting => None,
        ExistingPosition::StreamOld => Some(stream_position("10")),
        ExistingPosition::StreamNew => Some(stream_position("20")),
        ExistingPosition::Snapshot => Some(snapshot_position()),
    };
    let incoming = match incoming {
        IncomingPosition::StreamOld => stream_position("10"),
        IncomingPosition::StreamNew => stream_position("20"),
        IncomingPosition::Snapshot => snapshot_position(),
    };
    let mutation = match mutation {
        ModelMutation::Upsert => SourceMutation::Update,
        ModelMutation::Delete => SourceMutation::Delete,
    };
    Ok(result_from_decision(merge_decision(
        existing.as_ref(),
        &incoming,
        mutation,
        append_only,
    )?))
}

fn result_from_decision(decision: MergeDecision) -> MergeResult {
    match decision {
        MergeDecision::Insert => MergeResult::InsertRow,
        MergeDecision::Replace => MergeResult::ReplaceRow,
        MergeDecision::Delete => MergeResult::DeleteRow,
        MergeDecision::IgnoreDuplicate => MergeResult::IgnoreDuplicate,
        MergeDecision::IgnoreStale => MergeResult::IgnoreStale,
        MergeDecision::Append => MergeResult::AppendRow,
        MergeDecision::RequiresValidation => MergeResult::RequiresValidation,
    }
}

fn stream_position(sequence_number: &str) -> SourcePosition {
    SourcePosition::DynamoDbStreamSequence {
        stream_arn: "stream".to_string(),
        shard_id: "shard-1".to_string(),
        sequence_number: sequence_number.to_string(),
    }
}

fn snapshot_position() -> SourcePosition {
    SourcePosition::DynamoDbExportSnapshot {
        export_arn: "export".to_string(),
        exported_at_ms: 1,
    }
}
