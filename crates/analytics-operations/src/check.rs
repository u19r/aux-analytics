use std::{cmp::Ordering, vec};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    CheckBackendKind, CheckBackendSupport, CheckRowSource, OperationActor, OperationCursor,
    OperationEventKind, OperationId, OperationKind, OperationPhase, OperationRequest,
    OperationStatus, OperationStore, OperationStoreError, RateLimitPolicy,
};

#[derive(Debug, Error)]
pub enum CheckError {
    #[error(transparent)]
    Store(#[from] OperationStoreError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("check request must target at least one table")]
    EmptyTargetTables,
    #[error("check sample limit must be at least 1")]
    InvalidSampleLimit,
    #[error("source row reader failed: {0}")]
    SourceRead(String),
    #[error("destination row reader failed: {0}")]
    DestinationRead(String),
    #[error("{backend:?} check backend is unsupported: {reason}")]
    UnsupportedBackend {
        backend: CheckBackendKind,
        reason: String,
    },
    #[error("{backend:?} check backend is only partially supported: {reason}")]
    PartiallySupportedBackend {
        backend: CheckBackendKind,
        reason: String,
    },
    #[error("check backend configuration is invalid: {0}")]
    InvalidBackendConfiguration(String),
    #[error(transparent)]
    DuckDb(#[from] duckdb::Error),
}

pub type CheckResult<T> = Result<T, CheckError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckRequest {
    pub operation_id: OperationId,
    pub actor: OperationActor,
    pub target_tables: Vec<String>,
    pub strategy: CheckComparisonStrategy,
    pub sample_limit: usize,
    pub privacy_policy_version: Option<String>,
    pub output: CheckOutputFormat,
    pub rate_limit: RateLimitPolicy,
}

impl CheckRequest {
    pub fn validate(&self) -> CheckResult<()> {
        if self.target_tables.is_empty() {
            return Err(CheckError::EmptyTargetTables);
        }
        if self.sample_limit == 0 {
            return Err(CheckError::InvalidSampleLimit);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckComparisonStrategy {
    CountsOnly,
    KeySet,
    ChunkHashes,
    FullRows,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckOutputFormat {
    Json,
    Text,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckRow {
    pub table: String,
    pub key: String,
    pub row_hash: u64,
    pub source_position: u64,
    pub contains_private_data: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalCheckDataset {
    pub rows: Vec<CheckRow>,
}

impl LocalCheckDataset {
    #[must_use]
    pub fn new(rows: Vec<CheckRow>) -> Self {
        Self { rows }
    }
}

pub struct CheckRowStream {
    state: CheckRowStreamState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckRowStreamItem {
    pub partition: Option<String>,
    pub row: CheckRow,
}

enum CheckRowStreamState {
    Buffered {
        rows: vec::IntoIter<CheckRow>,
    },
    Streaming {
        source: Box<dyn CheckRowStreamSource>,
    },
}

pub trait CheckRowStreamSource {
    fn next_item(&mut self) -> CheckResult<Option<CheckRowStreamItem>>;
}

impl CheckRowStream {
    #[must_use]
    pub fn from_unsorted(mut rows: Vec<CheckRow>) -> Self {
        rows.sort_by(|left, right| row_sort_key(left).cmp(&row_sort_key(right)));
        Self::from_sorted(rows)
    }

    #[must_use]
    pub fn from_sorted(rows: Vec<CheckRow>) -> Self {
        Self {
            state: CheckRowStreamState::Buffered {
                rows: rows.into_iter(),
            },
        }
    }

    #[must_use]
    pub fn from_source(source: Box<dyn CheckRowStreamSource>) -> Self {
        Self {
            state: CheckRowStreamState::Streaming { source },
        }
    }

    pub fn next_row(&mut self) -> CheckResult<Option<CheckRow>> {
        Ok(self.next_item()?.map(|item| item.row))
    }

    fn next_item(&mut self) -> CheckResult<Option<CheckRowStreamItem>> {
        match &mut self.state {
            CheckRowStreamState::Buffered { rows } => {
                Ok(rows.next().map(CheckRowStreamItem::global))
            }
            CheckRowStreamState::Streaming { source } => source.next_item(),
        }
    }
}

impl CheckRowStreamItem {
    #[must_use]
    pub fn global(row: CheckRow) -> Self {
        Self {
            partition: None,
            row,
        }
    }

    #[must_use]
    pub fn partitioned(partition: impl Into<String>, row: CheckRow) -> Self {
        Self {
            partition: Some(partition.into()),
            row,
        }
    }
}

pub trait SourceTruthRows {
    fn source_rows(&self, target_tables: &[String]) -> CheckResult<Vec<CheckRow>>;

    fn source_row_stream(&self, target_tables: &[String]) -> CheckResult<CheckRowStream> {
        Ok(CheckRowStream::from_unsorted(
            self.source_rows(target_tables)?,
        ))
    }
}

pub trait DestinationRows {
    fn destination_rows(&self, target_tables: &[String]) -> CheckResult<Vec<CheckRow>>;

    fn destination_row_stream(&self, target_tables: &[String]) -> CheckResult<CheckRowStream> {
        Ok(CheckRowStream::from_unsorted(
            self.destination_rows(target_tables)?,
        ))
    }
}

impl SourceTruthRows for LocalCheckDataset {
    fn source_rows(&self, target_tables: &[String]) -> CheckResult<Vec<CheckRow>> {
        Ok(filter_target_tables(&self.rows, target_tables))
    }
}

impl DestinationRows for LocalCheckDataset {
    fn destination_rows(&self, target_tables: &[String]) -> CheckResult<Vec<CheckRow>> {
        Ok(filter_target_tables(&self.rows, target_tables))
    }
}

impl<T> SourceTruthRows for T
where T: CheckRowSource
{
    fn source_rows(&self, target_tables: &[String]) -> CheckResult<Vec<CheckRow>> {
        self.rows(target_tables)
            .map_err(|error| map_backend_read_error(error, self.support(), true))
    }

    fn source_row_stream(&self, target_tables: &[String]) -> CheckResult<CheckRowStream> {
        let support = self.support();
        let stream = self
            .row_stream(target_tables)
            .map_err(|error| map_backend_read_error(error, support.clone(), true))?;
        Ok(CheckRowStream::from_source(Box::new(
            BackendMappedCheckRowStream {
                stream,
                support,
                is_source: true,
            },
        )))
    }
}

impl<T> DestinationRows for T
where T: CheckRowSource
{
    fn destination_rows(&self, target_tables: &[String]) -> CheckResult<Vec<CheckRow>> {
        self.rows(target_tables)
            .map_err(|error| map_backend_read_error(error, self.support(), false))
    }

    fn destination_row_stream(&self, target_tables: &[String]) -> CheckResult<CheckRowStream> {
        let support = self.support();
        let stream = self
            .row_stream(target_tables)
            .map_err(|error| map_backend_read_error(error, support.clone(), false))?;
        Ok(CheckRowStream::from_source(Box::new(
            BackendMappedCheckRowStream {
                stream,
                support,
                is_source: false,
            },
        )))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckOutcome {
    Complete,
    Incomplete,
    Mismatched,
    Stale,
    PrivacyViolation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckReport {
    pub operation_id: OperationId,
    pub outcome: CheckOutcome,
    pub strategy: CheckComparisonStrategy,
    pub metrics: CheckMetrics,
    pub samples: CheckSamples,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckMetrics {
    pub source_rows: u64,
    pub destination_rows: u64,
    pub checked_rows: u64,
    pub missing_keys: u64,
    pub extra_keys: u64,
    pub mismatched_rows: u64,
    pub stale_rows: u64,
    pub privacy_violations: u64,
    pub deterministic_chunk_hash_mismatches: u64,
    pub unsupported_comparisons: u64,
    pub check_duration_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckSamples {
    pub missing_keys: Vec<String>,
    pub extra_keys: Vec<String>,
    pub mismatched_keys: Vec<String>,
    pub stale_keys: Vec<String>,
    pub privacy_violation_keys: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LocalCheckExecutor;

impl LocalCheckExecutor {
    pub fn execute<Source, Destination>(
        &self,
        store: &OperationStore,
        request: &CheckRequest,
        source: &Source,
        destination: &Destination,
    ) -> CheckResult<CheckReport>
    where
        Source: SourceTruthRows,
        Destination: DestinationRows,
    {
        request.validate()?;
        create_or_resume_operation(store, request)?;
        store.transition_with_counts(
            &request.operation_id,
            OperationPhase::Executing,
            OperationStatus::Running,
            Some(OperationCursor {
                label: "check".to_string(),
                position: 0,
            }),
            OperationEventKind::Started,
            serde_json::json!({ "target_table_count": request.target_tables.len() }),
            Some("check execution started"),
        )?;

        let source_rows = source.source_row_stream(&request.target_tables)?;
        let destination_rows = destination.destination_row_stream(&request.target_tables)?;
        let report = compare_row_streams(request, source_rows, destination_rows)?;
        store.save_check_report(&report.operation_id, &report)?;
        store.transition_with_counts(
            &request.operation_id,
            OperationPhase::Completed,
            status_for_outcome(report.outcome),
            Some(OperationCursor {
                label: "check".to_string(),
                position: report.metrics.checked_rows,
            }),
            event_for_outcome(report.outcome),
            serde_json::to_value(&report.metrics)?,
            Some("check execution completed"),
        )?;
        Ok(report)
    }
}

fn compare_row_streams(
    request: &CheckRequest,
    source_rows: CheckRowStream,
    destination_rows: CheckRowStream,
) -> CheckResult<CheckReport> {
    let comparison = compare_sorted_rows(request, source_rows, destination_rows)?;

    let metrics = CheckMetrics {
        source_rows: comparison.source_rows,
        destination_rows: comparison.destination_rows,
        checked_rows: comparison.checked_rows,
        missing_keys: comparison.missing_keys,
        extra_keys: comparison.extra_keys,
        mismatched_rows: comparison.mismatched_rows,
        stale_rows: comparison.stale_rows,
        privacy_violations: comparison.privacy_violations,
        deterministic_chunk_hash_mismatches: comparison.deterministic_chunk_hash_mismatches,
        unsupported_comparisons: 0,
        check_duration_ms: 0,
    };
    let outcome = outcome_for_metrics(&metrics);
    Ok(CheckReport {
        operation_id: request.operation_id.clone(),
        outcome,
        strategy: request.strategy,
        metrics,
        samples: comparison.samples,
    })
}

fn compare_sorted_rows(
    request: &CheckRequest,
    mut source_rows: CheckRowStream,
    mut destination_rows: CheckRowStream,
) -> CheckResult<SortedComparison> {
    let mut comparison = SortedComparison::default();
    let mut samples = BoundedCheckSamples::new(request.sample_limit);
    let mut source_hash = DATASET_HASH_OFFSET_BASIS;
    let mut destination_hash = DATASET_HASH_OFFSET_BASIS;
    let mut source_row = source_rows.next_item()?;
    let mut destination_row = destination_rows.next_item()?;

    while source_row.is_some() || destination_row.is_some() {
        match (source_row.as_ref(), destination_row.as_ref()) {
            (Some(source), Some(destination)) => {
                match stream_item_sort_key(source).cmp(&stream_item_sort_key(destination)) {
                    Ordering::Less => {
                        comparison.source_rows += 1;
                        source_hash = deterministic_dataset_hash_step(source_hash, &source.row);
                        if !matches!(request.strategy, CheckComparisonStrategy::CountsOnly) {
                            comparison.missing_keys += 1;
                            samples.push_missing_row(&source.row);
                        }
                        comparison.checked_rows += 1;
                        source_row = source_rows.next_item()?;
                    }
                    Ordering::Greater => {
                        comparison.destination_rows += 1;
                        destination_hash =
                            deterministic_dataset_hash_step(destination_hash, &destination.row);
                        if !matches!(request.strategy, CheckComparisonStrategy::CountsOnly) {
                            comparison.extra_keys += 1;
                            samples.push_extra_row(&destination.row);
                        }
                        comparison.checked_rows += 1;
                        destination_row = destination_rows.next_item()?;
                    }
                    Ordering::Equal => {
                        comparison.source_rows += 1;
                        comparison.destination_rows += 1;
                        source_hash = deterministic_dataset_hash_step(source_hash, &source.row);
                        destination_hash =
                            deterministic_dataset_hash_step(destination_hash, &destination.row);
                        comparison.checked_rows += 1;
                        compare_matching_rows(
                            request.strategy,
                            &source.row,
                            &destination.row,
                            &mut comparison,
                            &mut samples,
                        );
                        source_row = source_rows.next_item()?;
                        destination_row = destination_rows.next_item()?;
                    }
                }
            }
            (Some(source), None) => {
                comparison.source_rows += 1;
                source_hash = deterministic_dataset_hash_step(source_hash, &source.row);
                if !matches!(request.strategy, CheckComparisonStrategy::CountsOnly) {
                    comparison.missing_keys += 1;
                    samples.push_missing_row(&source.row);
                }
                comparison.checked_rows += 1;
                source_row = source_rows.next_item()?;
            }
            (None, Some(destination)) => {
                comparison.destination_rows += 1;
                destination_hash =
                    deterministic_dataset_hash_step(destination_hash, &destination.row);
                if !matches!(request.strategy, CheckComparisonStrategy::CountsOnly) {
                    comparison.extra_keys += 1;
                    samples.push_extra_row(&destination.row);
                }
                comparison.checked_rows += 1;
                destination_row = destination_rows.next_item()?;
            }
            (None, None) => break,
        }
    }

    if matches!(request.strategy, CheckComparisonStrategy::CountsOnly) {
        apply_count_only_differences(&mut comparison, &mut samples);
    }
    comparison.deterministic_chunk_hash_mismatches = u64::from(
        matches!(request.strategy, CheckComparisonStrategy::ChunkHashes)
            && source_hash != destination_hash,
    );
    comparison.samples = samples.into_samples();
    Ok(comparison)
}

fn compare_matching_rows(
    strategy: CheckComparisonStrategy,
    source: &CheckRow,
    destination: &CheckRow,
    comparison: &mut SortedComparison,
    samples: &mut BoundedCheckSamples,
) {
    if matches!(strategy, CheckComparisonStrategy::FullRows)
        && source.row_hash != destination.row_hash
    {
        comparison.mismatched_rows += 1;
        samples.push_mismatched_row(source);
    }
    if matches!(strategy, CheckComparisonStrategy::FullRows)
        && destination.source_position < source.source_position
    {
        comparison.stale_rows += 1;
        samples.push_stale_row(source);
    }
    if source.contains_private_data || destination.contains_private_data {
        comparison.privacy_violations += 1;
        samples.push_privacy_row(source);
    }
}

fn apply_count_only_differences(
    comparison: &mut SortedComparison,
    samples: &mut BoundedCheckSamples,
) {
    match comparison.source_rows.cmp(&comparison.destination_rows) {
        Ordering::Greater => {
            comparison.missing_keys = comparison.source_rows - comparison.destination_rows;
            for index in 0..comparison.missing_keys {
                samples.push_missing_count(index);
            }
        }
        Ordering::Less => {
            comparison.extra_keys = comparison.destination_rows - comparison.source_rows;
            for index in 0..comparison.extra_keys {
                samples.push_extra_count(index);
            }
        }
        Ordering::Equal => {}
    }
}

fn create_or_resume_operation(store: &OperationStore, request: &CheckRequest) -> CheckResult<()> {
    let operation_request = OperationRequest {
        operation_id: request.operation_id.clone(),
        kind: OperationKind::Check,
        actor: request.actor.clone(),
        target_tables: request.target_tables.clone(),
        dry_run: true,
        rate_limit: request.rate_limit.clone(),
        payload: serde_json::json!({
            "mode": "local_check_execution",
            "strategy": request.strategy,
            "sample_limit": request.sample_limit,
            "privacy_policy_version": request.privacy_policy_version,
            "output": request.output,
        }),
    };
    match store.create_operation(&operation_request) {
        Ok(()) | Err(OperationStoreError::DuplicateOperation(_)) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn filter_target_tables(rows: &[CheckRow], target_tables: &[String]) -> Vec<CheckRow> {
    if let [target_table] = target_tables {
        return rows
            .iter()
            .filter(|row| row.table == *target_table)
            .cloned()
            .collect();
    }

    rows.iter()
        .filter(|row| target_tables.contains(&row.table))
        .cloned()
        .collect()
}

fn map_backend_read_error(
    error: CheckError,
    support: CheckBackendSupport,
    is_source: bool,
) -> CheckError {
    match error {
        CheckError::UnsupportedBackend { .. }
        | CheckError::PartiallySupportedBackend { .. }
        | CheckError::InvalidBackendConfiguration(_)
        | CheckError::DuckDb(_) => error,
        other => {
            let context = match support {
                CheckBackendSupport::Supported => other.to_string(),
                CheckBackendSupport::Partial { reason }
                | CheckBackendSupport::Unsupported { reason } => reason,
            };
            if is_source {
                CheckError::SourceRead(context)
            } else {
                CheckError::DestinationRead(context)
            }
        }
    }
}

struct BackendMappedCheckRowStream {
    stream: CheckRowStream,
    support: CheckBackendSupport,
    is_source: bool,
}

impl CheckRowStreamSource for BackendMappedCheckRowStream {
    fn next_item(&mut self) -> CheckResult<Option<CheckRowStreamItem>> {
        self.stream
            .next_item()
            .map_err(|error| map_backend_read_error(error, self.support.clone(), self.is_source))
    }
}

#[derive(Debug, Default)]
struct SortedComparison {
    source_rows: u64,
    destination_rows: u64,
    checked_rows: u64,
    missing_keys: u64,
    extra_keys: u64,
    mismatched_rows: u64,
    stale_rows: u64,
    privacy_violations: u64,
    deterministic_chunk_hash_mismatches: u64,
    samples: CheckSamples,
}

struct BoundedCheckSamples {
    sample_limit: usize,
    samples: CheckSamples,
}

impl BoundedCheckSamples {
    const fn new(sample_limit: usize) -> Self {
        Self {
            sample_limit,
            samples: CheckSamples {
                missing_keys: Vec::new(),
                extra_keys: Vec::new(),
                mismatched_keys: Vec::new(),
                stale_keys: Vec::new(),
                privacy_violation_keys: Vec::new(),
            },
        }
    }

    fn push_missing_row(&mut self, row: &CheckRow) {
        push_row_sample_bounded(&mut self.samples.missing_keys, self.sample_limit, row);
    }

    fn push_extra_row(&mut self, row: &CheckRow) {
        push_row_sample_bounded(&mut self.samples.extra_keys, self.sample_limit, row);
    }

    fn push_mismatched_row(&mut self, row: &CheckRow) {
        push_row_sample_bounded(&mut self.samples.mismatched_keys, self.sample_limit, row);
    }

    fn push_stale_row(&mut self, row: &CheckRow) {
        push_row_sample_bounded(&mut self.samples.stale_keys, self.sample_limit, row);
    }

    fn push_privacy_row(&mut self, row: &CheckRow) {
        push_row_sample_bounded(
            &mut self.samples.privacy_violation_keys,
            self.sample_limit,
            row,
        );
    }

    fn push_missing_count(&mut self, index: u64) {
        push_count_sample_bounded(
            &mut self.samples.missing_keys,
            self.sample_limit,
            "count_missing:",
            index,
        );
    }

    fn push_extra_count(&mut self, index: u64) {
        push_count_sample_bounded(
            &mut self.samples.extra_keys,
            self.sample_limit,
            "count_extra:",
            index,
        );
    }

    fn into_samples(self) -> CheckSamples {
        self.samples
    }
}

fn push_row_sample_bounded(samples: &mut Vec<String>, sample_limit: usize, row: &CheckRow) {
    if samples.len() < sample_limit {
        samples.push(row_sample_key(row));
    }
}

fn push_count_sample_bounded(
    samples: &mut Vec<String>,
    sample_limit: usize,
    prefix: &str,
    index: u64,
) {
    if samples.len() < sample_limit {
        samples.push(format!("{prefix}{index}"));
    }
}

fn row_sort_key(row: &CheckRow) -> (&str, &str) {
    (row.table.as_str(), row.key.as_str())
}

fn stream_item_sort_key(item: &CheckRowStreamItem) -> (Option<&str>, &str, &str) {
    (
        item.partition.as_deref(),
        item.row.table.as_str(),
        item.row.key.as_str(),
    )
}

pub(crate) fn row_sample_key(row: &CheckRow) -> String {
    let mut key = String::with_capacity(row.table.len() + row.key.len() + 1);
    key.push_str(&row.table);
    key.push(':');
    key.push_str(&row.key);
    key
}

const DATASET_HASH_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;

pub(crate) fn deterministic_dataset_hash_step(hash: u64, row: &CheckRow) -> u64 {
    let key_hash = fnv1a(
        fnv1a(fnv1a(hash, row.table.as_bytes()), b":"),
        row.key.as_bytes(),
    );
    fnv1a(
        key_hash ^ row.row_hash ^ row.source_position,
        row.table.as_bytes(),
    )
}

fn fnv1a(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

fn outcome_for_metrics(metrics: &CheckMetrics) -> CheckOutcome {
    if metrics.privacy_violations > 0 {
        CheckOutcome::PrivacyViolation
    } else if metrics.stale_rows > 0 {
        CheckOutcome::Stale
    } else if metrics.missing_keys > 0 || metrics.extra_keys > 0 {
        CheckOutcome::Incomplete
    } else if metrics.mismatched_rows > 0 || metrics.deterministic_chunk_hash_mismatches > 0 {
        CheckOutcome::Mismatched
    } else {
        CheckOutcome::Complete
    }
}

fn status_for_outcome(outcome: CheckOutcome) -> OperationStatus {
    match outcome {
        CheckOutcome::Complete => OperationStatus::Succeeded,
        CheckOutcome::Incomplete
        | CheckOutcome::Mismatched
        | CheckOutcome::Stale
        | CheckOutcome::PrivacyViolation => OperationStatus::Failed,
    }
}

fn event_for_outcome(outcome: CheckOutcome) -> OperationEventKind {
    match outcome {
        CheckOutcome::Complete => OperationEventKind::Succeeded,
        CheckOutcome::Incomplete
        | CheckOutcome::Mismatched
        | CheckOutcome::Stale
        | CheckOutcome::PrivacyViolation => OperationEventKind::Failed,
    }
}
