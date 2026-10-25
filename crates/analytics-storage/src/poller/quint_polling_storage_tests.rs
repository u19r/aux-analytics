use std::time::Duration;

use analytics_contract::{StorageItem, StorageStreamRecord};
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;
use storage_types::{SystemStreamCheckpointToken, SystemStreamScope};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

use crate::{
    PollBatch, PolledRecord, SourceCheckpoint,
    aux_storage_client::AuxStorageStreamClient,
    aws_stream::AwsShardIteratorState,
    constants::AUX_RESPONSE_WITHOUT_SCOPE,
    planning::SourceTablePlan,
    poller::{
        AuxStorageGlobalPoller, AwsStreamResponseBatchInput, apply_aws_iterator_checkpoint,
        aws_stream_response_batch, route_records,
    },
    source_routes::CompiledSourceRoutes,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum StoragePollingResult {
    NotChecked,
    FanOutComplete,
    AuxScopeRejected,
    AwsIteratorAdvanced,
    AwsRecordAndIteratorCheckpointsEmitted,
    AwsIteratorOnlyCheckpointEmitted,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum SourceTable {
    NoSourceTable,
    UsersTable,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum SourceShard {
    NoSourceShard,
    Shard1,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum Cursor {
    CursorValue(String),
    NoCursor,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct StoragePollingState {
    #[serde(rename = "lastResult")]
    last_result: StoragePollingResult,
    #[serde(rename = "sourceTable")]
    source_table: SourceTable,
    #[serde(rename = "sourceShard")]
    source_shard: SourceShard,
    #[serde(rename = "recordCount")]
    record_count: usize,
    #[serde(rename = "analyticsTableCount")]
    analytics_table_count: usize,
    #[serde(rename = "routedRecordCount")]
    routed_record_count: usize,
    #[serde(rename = "auxScopeRejected")]
    aux_scope_rejected: bool,
    #[serde(rename = "auxCursorPreserved")]
    aux_cursor_preserved: bool,
    #[serde(rename = "auxCursorBefore")]
    aux_cursor_before: Cursor,
    #[serde(rename = "auxCursorAfter")]
    aux_cursor_after: Cursor,
    #[serde(rename = "durableCheckpointPresent")]
    durable_checkpoint_present: bool,
    #[serde(rename = "durableCheckpointCursor")]
    durable_checkpoint_cursor: Cursor,
    #[serde(rename = "durableCheckpointPresentBefore")]
    durable_checkpoint_present_before: bool,
    #[serde(rename = "durableCheckpointCursorBefore")]
    durable_checkpoint_cursor_before: Cursor,
    #[serde(rename = "awsRecordCheckpointPresent")]
    aws_record_checkpoint_present: bool,
    #[serde(rename = "recordCheckpointCount")]
    record_checkpoint_count: usize,
    #[serde(rename = "awsIteratorCheckpointPresent")]
    aws_iterator_checkpoint_present: bool,
    #[serde(rename = "iteratorCheckpointCount")]
    iterator_checkpoint_count: usize,
    #[serde(rename = "iteratorCheckpointCursor")]
    iterator_checkpoint_cursor: Cursor,
    #[serde(rename = "awsIteratorApplied")]
    aws_iterator_applied: bool,
    #[serde(rename = "iteratorCheckpointDurable")]
    iterator_checkpoint_durable: bool,
    #[serde(rename = "iteratorSourceTable")]
    iterator_source_table: SourceTable,
    #[serde(rename = "iteratorSourceShard")]
    iterator_source_shard: SourceShard,
}

impl State<StoragePollingDriver> for StoragePollingState {
    fn from_driver(driver: &StoragePollingDriver) -> Result<Self> {
        Ok(driver.state.clone())
    }
}

struct StoragePollingDriver {
    state: StoragePollingState,
}

impl Default for StoragePollingDriver {
    fn default() -> Self {
        Self {
            state: StoragePollingState {
                last_result: StoragePollingResult::NotChecked,
                source_table: SourceTable::NoSourceTable,
                source_shard: SourceShard::NoSourceShard,
                record_count: 0,
                analytics_table_count: 0,
                routed_record_count: 0,
                aux_scope_rejected: false,
                aux_cursor_preserved: false,
                aux_cursor_before: Cursor::NoCursor,
                aux_cursor_after: Cursor::NoCursor,
                durable_checkpoint_present: false,
                durable_checkpoint_cursor: Cursor::NoCursor,
                durable_checkpoint_present_before: false,
                durable_checkpoint_cursor_before: Cursor::NoCursor,
                aws_record_checkpoint_present: false,
                record_checkpoint_count: 0,
                aws_iterator_checkpoint_present: false,
                iterator_checkpoint_count: 0,
                iterator_checkpoint_cursor: Cursor::NoCursor,
                aws_iterator_applied: false,
                iterator_checkpoint_durable: false,
                iterator_source_table: SourceTable::NoSourceTable,
                iterator_source_shard: SourceShard::NoSourceShard,
            },
        }
    }
}

impl StoragePollingDriver {
    fn reset(&mut self) {
        *self = Self::default();
    }
}

impl Driver for StoragePollingDriver {
    type State = StoragePollingState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.reset(),
            CheckFanOut => check_fan_out(self)?,
            CheckAuxScopeRejected => check_aux_scope_rejected(self)?,
            CheckAwsIteratorAdvances => check_aws_iterator_advances(self)?,
            CheckAwsBatchCheckpoints => check_aws_batch_checkpoints(self)?,
            CheckAwsIteratorOnlyCheckpoint => check_aws_iterator_only_checkpoint(self)?,
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/polling_storage.qnt",
    max_samples = 50,
    max_steps = 8,
    seed = "0x5"
)]
fn given_quint_polling_storage_model_when_replayed_then_storage_polling_matches() -> impl Driver {
    StoragePollingDriver::default()
}

fn check_fan_out(driver: &mut StoragePollingDriver) -> Result<()> {
    let expanded = route_records(
        "source_users",
        &[
            "analytics_users".to_string(),
            "analytics_accounts".to_string(),
        ],
        "shard-0001",
        [stream_record("seq-1")],
    );
    let target_count = distinct_analytics_table_count(&expanded);
    if expanded.len() == 1
        && expanded[0].analytics_table_names == ["analytics_users", "analytics_accounts"]
        && expanded[0].record_key == "seq-1"
        && expanded[0].record.sequence_number == "seq-1"
        && target_count == 2
    {
        begin_observation(
            driver,
            StoragePollingResult::FanOutComplete,
            source_table(&expanded[0].source_table_name)?,
            source_shard(&expanded[0].shard_id)?,
        );
        driver.state.record_count = 1;
        driver.state.analytics_table_count = target_count;
        driver.state.routed_record_count = expanded
            .iter()
            .map(|record| record.analytics_table_names.len())
            .sum();
        Ok(())
    } else {
        Err(anyhow::anyhow!("polling fan-out did not match model"))
    }
}

fn begin_observation(
    driver: &mut StoragePollingDriver,
    result: StoragePollingResult,
    source_table: SourceTable,
    source_shard: SourceShard,
) {
    let state = &mut driver.state;
    state.durable_checkpoint_present_before = state.durable_checkpoint_present;
    state.durable_checkpoint_cursor_before = state.durable_checkpoint_cursor.clone();
    state.last_result = result;
    state.source_table = source_table;
    state.source_shard = source_shard;
    state.record_count = 0;
    state.analytics_table_count = 0;
    state.routed_record_count = 0;
    state.aux_scope_rejected = false;
    state.aux_cursor_preserved = false;
    state.aux_cursor_before = Cursor::NoCursor;
    state.aux_cursor_after = Cursor::NoCursor;
    state.aws_record_checkpoint_present = false;
    state.record_checkpoint_count = 0;
    state.aws_iterator_checkpoint_present = false;
    state.iterator_checkpoint_count = 0;
    state.iterator_checkpoint_cursor = Cursor::NoCursor;
    state.aws_iterator_applied = false;
    state.iterator_checkpoint_durable = false;
    state.iterator_source_table = SourceTable::NoSourceTable;
    state.iterator_source_shard = SourceShard::NoSourceShard;
}

fn cursor_from_option(value: Option<&str>) -> Cursor {
    value.map_or(Cursor::NoCursor, |value| {
        Cursor::CursorValue(value.to_string())
    })
}

fn source_table(value: &str) -> Result<SourceTable> {
    match value {
        "source_users" => Ok(SourceTable::UsersTable),
        _ => Err(anyhow::anyhow!("unexpected source table: {value}")),
    }
}

fn source_shard(value: &str) -> Result<SourceShard> {
    match value {
        "shard-0001" => Ok(SourceShard::Shard1),
        _ => Err(anyhow::anyhow!("unexpected source shard: {value}")),
    }
}

fn distinct_analytics_table_count(records: &[PolledRecord]) -> usize {
    let mut names = Vec::new();
    for name in records
        .iter()
        .flat_map(|record| &record.analytics_table_names)
    {
        if !names.contains(&name.as_str()) {
            names.push(name.as_str());
        }
    }
    names.len()
}

fn observe_aws_batch(
    driver: &mut StoragePollingDriver,
    batch: &PollBatch,
    result: StoragePollingResult,
) -> Result<()> {
    let scope = batch
        .source_scope()?
        .ok_or_else(|| anyhow::anyhow!("AWS batch did not expose a source scope"))?;
    let source_table = source_table(scope.source_table_name())?;
    let source_shard = source_shard(scope.shard_id())?;
    let record_checkpoint = batch
        .checkpoints
        .iter()
        .find(|checkpoint| checkpoint.is_record());
    let iterator_checkpoint = batch
        .checkpoints
        .iter()
        .find(|checkpoint| checkpoint.is_iterator());
    begin_observation(driver, result, source_table, source_shard);
    driver.state.record_count = batch.source_record_count;
    driver.state.analytics_table_count = distinct_analytics_table_count(&batch.records);
    driver.state.routed_record_count = batch
        .records
        .iter()
        .map(|record| record.analytics_table_names.len())
        .sum();
    driver.state.aws_record_checkpoint_present = record_checkpoint.is_some();
    driver.state.record_checkpoint_count = usize::from(record_checkpoint.is_some());
    if let Some(checkpoint) = record_checkpoint {
        driver.state.durable_checkpoint_present = true;
        driver.state.durable_checkpoint_cursor =
            cursor_from_option(Some(checkpoint.position.as_str()));
    }
    driver.state.aws_iterator_checkpoint_present = iterator_checkpoint.is_some();
    driver.state.iterator_checkpoint_count = usize::from(iterator_checkpoint.is_some());
    if let Some(checkpoint) = iterator_checkpoint {
        driver.state.iterator_checkpoint_cursor =
            cursor_from_option(Some(checkpoint.position.as_str()));
        driver.state.iterator_source_table = driver.state.source_table.clone();
        driver.state.iterator_source_shard = driver.state.source_shard.clone();
    }
    Ok(())
}

fn check_aux_scope_rejected(driver: &mut StoragePollingDriver) -> Result<()> {
    let prior_checkpoint = Some("v1.0123456789abcdef0123456789abcdef".to_string());
    let retained_checkpoint =
        poll_aux_storage_rejection(prior_checkpoint.clone(), AUX_RESPONSE_WITHOUT_SCOPE)?;
    if retained_checkpoint != prior_checkpoint {
        return Err(anyhow::anyhow!(
            "global AuxStorage rejection changed the durable checkpoint"
        ));
    }
    begin_observation(
        driver,
        StoragePollingResult::AuxScopeRejected,
        SourceTable::NoSourceTable,
        SourceShard::NoSourceShard,
    );
    driver.state.aux_scope_rejected = true;
    driver.state.aux_cursor_preserved = retained_checkpoint == prior_checkpoint;
    // The Quint model observes a stable symbolic cursor; the implementation
    // retains the opaque typed checkpoint above and never exposes its bytes to
    // the model state.
    driver.state.aux_cursor_before = cursor_from_option(Some("prior-page"));
    driver.state.aux_cursor_after = cursor_from_option(Some("prior-page"));
    Ok(())
}

fn poll_aux_storage_rejection(
    checkpoint: Option<String>,
    response_body: &'static str,
) -> Result<Option<String>> {
    current_thread_runtime()?.block_on(async move {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(serve_responses(listener, vec![response_body]));
        let base_url = format!("http://{address}");
        let mut poller = AuxStorageGlobalPoller {
            client: AuxStorageStreamClient::try_new(&base_url, Duration::from_secs(1))?,
            routes: test_routes()?,
            source_scope: SystemStreamScope::try_new("quint-system", 1)?,
            scan_checkpoint: checkpoint
                .as_deref()
                .map(SystemStreamCheckpointToken::try_from)
                .transpose()?,
            checkpoint: checkpoint
                .as_deref()
                .map(SystemStreamCheckpointToken::try_from)
                .transpose()?,
            max_records_per_response: 1_000,
            serialization_buffer: Vec::new(),
        };
        let error = poller
            .poll_once(1)
            .await
            .expect_err("global AuxStorage polling must fail closed");
        if error.category() != "missing_response_scope" {
            return Err(anyhow::anyhow!(
                "unexpected global scope category: {}",
                error.category()
            ));
        }
        Ok(poller
            .checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.as_str().to_string()))
    })
}

async fn serve_responses(listener: TcpListener, bodies: Vec<&'static str>) {
    for body in bodies {
        let Ok((mut stream, _)) = listener.accept().await else {
            return;
        };
        let mut request = [0_u8; 4_096];
        let _ = stream.read(&mut request).await;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: \
             {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = stream.write_all(response.as_bytes()).await;
    }
}

fn current_thread_runtime() -> Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?)
}

fn test_routes() -> Result<CompiledSourceRoutes> {
    Ok(CompiledSourceRoutes::from_plans(&[
        SourceTablePlan::aux_storage("source_users".to_string(), vec!["users".to_string()]),
    ])?)
}

#[test]
fn given_system_response_without_scope_when_polled_then_error_preserves_checkpoint() -> Result<()> {
    let checkpoint = Some("v1.0123456789abcdef0123456789abcdef".to_string());
    let retained_checkpoint =
        poll_aux_storage_rejection(checkpoint.clone(), AUX_RESPONSE_WITHOUT_SCOPE)?;
    assert_eq!(retained_checkpoint, checkpoint);
    Ok(())
}

fn check_aws_iterator_advances(driver: &mut StoragePollingDriver) -> Result<()> {
    let mut states = vec![AwsShardIteratorState {
        shard_id: "shard-0001".to_string(),
        iterator: "old-iterator".to_string(),
    }];
    let checkpoint = SourceCheckpoint {
        source_table_name: "source_users".to_string(),
        shard_id: "__iterator:shard-0001".to_string(),
        position: "new-iterator".to_string(),
    };
    let applied = apply_aws_iterator_checkpoint("source_users", &mut states, &checkpoint);
    if applied && states[0].iterator == "new-iterator" {
        begin_observation(
            driver,
            StoragePollingResult::AwsIteratorAdvanced,
            source_table(&checkpoint.source_table_name)?,
            source_shard(states[0].shard_id.as_str())?,
        );
        driver.state.aws_iterator_checkpoint_present = true;
        driver.state.iterator_checkpoint_count = 1;
        driver.state.iterator_checkpoint_cursor =
            cursor_from_option(Some(states[0].iterator.as_str()));
        driver.state.aws_iterator_applied = true;
        driver.state.iterator_source_table = driver.state.source_table.clone();
        driver.state.iterator_source_shard = driver.state.source_shard.clone();
        Ok(())
    } else {
        Err(anyhow::anyhow!("AWS iterator checkpoint did not advance"))
    }
}

fn check_aws_batch_checkpoints(driver: &mut StoragePollingDriver) -> Result<()> {
    let batch = aws_stream_response_batch(
        AwsStreamResponseBatchInput {
            source_table_name: "source_users",
            analytics_table_names: &["users".to_string(), "accounts".to_string()],
            shard_id: "shard-0001",
            source_predecessor: None,
            contract_records: vec![stream_record("seq-1"), stream_record("seq-2")],
            next_iterator: Some("next-iterator".to_string()),
        },
        &mut Vec::new(),
    )?;
    if batch.records.len() == 2
        && batch
            .records
            .iter()
            .all(|record| record.analytics_table_names == ["users", "accounts"])
        && batch.checkpoints.len() == 2
        && batch.checkpoints[0].shard_id == "shard-0001"
        && batch.checkpoints[0].position == "seq-2"
        && batch.checkpoints[1].shard_id == "__iterator:shard-0001"
        && batch.checkpoints[1].position == "next-iterator"
    {
        observe_aws_batch(
            driver,
            &batch,
            StoragePollingResult::AwsRecordAndIteratorCheckpointsEmitted,
        )
    } else {
        Err(anyhow::anyhow!(
            "AWS record and iterator checkpoints did not match model"
        ))
    }
}

fn check_aws_iterator_only_checkpoint(driver: &mut StoragePollingDriver) -> Result<()> {
    let batch = aws_stream_response_batch(
        AwsStreamResponseBatchInput {
            source_table_name: "source_users",
            analytics_table_names: &["users".to_string()],
            shard_id: "shard-0001",
            source_predecessor: None,
            contract_records: Vec::new(),
            next_iterator: Some("next-iterator".to_string()),
        },
        &mut Vec::new(),
    )?;
    if batch.records.is_empty()
        && batch.checkpoints.len() == 1
        && batch.checkpoints[0].shard_id == "__iterator:shard-0001"
        && batch.checkpoints[0].position == "next-iterator"
    {
        observe_aws_batch(
            driver,
            &batch,
            StoragePollingResult::AwsIteratorOnlyCheckpointEmitted,
        )
    } else {
        Err(anyhow::anyhow!(
            "AWS iterator-only checkpoint did not match model"
        ))
    }
}

fn stream_record(sequence_number: &str) -> StorageStreamRecord {
    StorageStreamRecord {
        sequence_number: sequence_number.to_string(),
        keys: StorageItem::new(),
        old_image: None,
        new_image: None,
    }
}
