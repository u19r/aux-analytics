use super::*;
use crate::constants::FNV_OFFSET_BASIS;

pub(crate) fn compare_row_streams(
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
        unsupported_comparisons: comparison.unsupported_comparisons,
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
    let mut comparison = SortedComparison {
        source_hash: FNV_OFFSET_BASIS,
        destination_hash: FNV_OFFSET_BASIS,
        sample_limit: request.sample_limit,
        ..SortedComparison::default()
    };
    let mut source_row = source_rows.next_item()?;
    let mut destination_row = destination_rows.next_item()?;
    loop {
        let order = compare_stream_heads(source_row.as_ref(), destination_row.as_ref());
        match (order, source_row.as_ref(), destination_row.as_ref()) {
            (Ordering::Less, Some(source), _) => {
                comparison.record_missing_row(request.strategy, &source.row);
                source_row = source_rows.next_item()?;
            }
            (Ordering::Greater, _, Some(destination)) => {
                comparison.record_extra_row(request.strategy, &destination.row);
                destination_row = destination_rows.next_item()?;
            }
            (Ordering::Equal, Some(source), Some(destination)) => {
                comparison.record_source_row(&source.row);
                comparison.record_destination_row(&destination.row);
                comparison.compare_matching_rows(request.strategy, &source.row, &destination.row);
                source_row = source_rows.next_item()?;
                destination_row = destination_rows.next_item()?;
            }
            _ => break,
        }
        comparison.checked_rows += 1;
    }
    Ok(comparison.finish_comparison(request.strategy))
}

fn compare_stream_heads(
    source: Option<&CheckRowStreamItem>,
    destination: Option<&CheckRowStreamItem>,
) -> Ordering {
    match (source, destination) {
        (Some(source), Some(destination)) => {
            stream_item_sort_key(source).cmp(&stream_item_sort_key(destination))
        }
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

pub(crate) fn map_backend_read_error(
    error: CheckError,
    support: CheckBackendSupport,
    is_source: bool,
) -> CheckError {
    match error {
        CheckError::UnsupportedBackend { .. }
        | CheckError::PartiallySupportedBackend { .. }
        | CheckError::InvalidBackendConfiguration(_)
        | CheckError::DuckDb(_)
        | CheckError::ScanWorkerStart(_)
        | CheckError::ScanWorkerPanicked
        | CheckError::FailurePersistence { .. } => error,
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

pub(crate) struct BackendMappedCheckRowStream {
    pub(crate) stream: CheckRowStream,
    pub(crate) support: CheckBackendSupport,
    pub(crate) is_source: bool,
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
    sample_limit: usize,
    source_hash: u64,
    destination_hash: u64,
    source_rows: u64,
    destination_rows: u64,
    checked_rows: u64,
    missing_keys: u64,
    extra_keys: u64,
    mismatched_rows: u64,
    stale_rows: u64,
    privacy_violations: u64,
    deterministic_chunk_hash_mismatches: u64,
    unsupported_comparisons: u64,
    samples: CheckSamples,
}

// Inject totals only in test builds to prove bounded work at u64::MAX without
// constructing that many rows or changing the production stream contract.
#[cfg(test)]
pub(super) fn compare_count_totals(
    source_rows: u64,
    destination_rows: u64,
    sample_limit: usize,
) -> (u64, u64, CheckSamples) {
    let comparison = SortedComparison {
        source_rows,
        destination_rows,
        sample_limit,
        ..SortedComparison::default()
    }
    .finish_comparison(CheckComparisonStrategy::CountsOnly);
    (
        comparison.missing_keys,
        comparison.extra_keys,
        comparison.samples,
    )
}

impl SortedComparison {
    fn record_source_row(&mut self, row: &CheckRow) {
        self.source_rows += 1;
        self.source_hash = deterministic_dataset_hash_step(self.source_hash, row);
    }

    fn record_destination_row(&mut self, row: &CheckRow) {
        self.destination_rows += 1;
        self.destination_hash = deterministic_dataset_hash_step(self.destination_hash, row);
    }

    fn record_missing_row(&mut self, strategy: CheckComparisonStrategy, row: &CheckRow) {
        self.record_source_row(row);
        if matches!(strategy, CheckComparisonStrategy::CountsOnly) {
            return;
        }
        self.missing_keys += 1;
        self.push_missing_row(row);
    }

    fn record_extra_row(&mut self, strategy: CheckComparisonStrategy, row: &CheckRow) {
        self.record_destination_row(row);
        if matches!(strategy, CheckComparisonStrategy::CountsOnly) {
            return;
        }
        self.extra_keys += 1;
        self.push_extra_row(row);
    }

    fn finish_comparison(mut self, strategy: CheckComparisonStrategy) -> Self {
        if matches!(strategy, CheckComparisonStrategy::CountsOnly) {
            self.apply_count_only_differences();
        }
        self.deterministic_chunk_hash_mismatches = u64::from(
            matches!(strategy, CheckComparisonStrategy::ChunkHashes)
                && self.source_hash != self.destination_hash,
        );
        self
    }

    fn compare_matching_rows(
        &mut self,
        strategy: CheckComparisonStrategy,
        source: &CheckRow,
        destination: &CheckRow,
    ) {
        if matches!(strategy, CheckComparisonStrategy::FullRows)
            && source.row_hash != destination.row_hash
        {
            self.mismatched_rows += 1;
            self.push_mismatched_row(source);
        }
        if matches!(strategy, CheckComparisonStrategy::FullRows) {
            match destination
                .source_position
                .partial_cmp_position(&source.source_position)
            {
                Some(Ordering::Less) => {
                    self.stale_rows += 1;
                    self.push_stale_row(source);
                }
                Some(Ordering::Equal | Ordering::Greater) => {}
                None => self.unsupported_comparisons += 1,
            }
        }
        if source.contains_private_data || destination.contains_private_data {
            self.privacy_violations += 1;
            self.push_privacy_row(source);
        }
    }

    fn apply_count_only_differences(&mut self) {
        match self.source_rows.cmp(&self.destination_rows) {
            Ordering::Greater => {
                self.missing_keys = self.source_rows - self.destination_rows;
                for index in 0..self.missing_keys.min(self.sample_limit as u64) {
                    self.push_missing_count(index);
                }
            }
            Ordering::Less => {
                self.extra_keys = self.destination_rows - self.source_rows;
                for index in 0..self.extra_keys.min(self.sample_limit as u64) {
                    self.push_extra_count(index);
                }
            }
            Ordering::Equal => {}
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

pub(crate) fn row_sort_key(row: &CheckRow) -> (&str, &str) {
    (row.table.as_str(), row.key.as_str())
}

pub(crate) fn stream_item_sort_key(item: &CheckRowStreamItem) -> (Option<&str>, &str, &str) {
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

pub(crate) fn deterministic_dataset_hash_step(hash: u64, row: &CheckRow) -> u64 {
    let hash = hash_string(hash, &row.table);
    let hash = hash_string(hash, &row.key);
    let hash = fnv1a(hash, &row.row_hash.to_le_bytes());
    fnv1a(hash, &row.source_position.hash_seed().to_le_bytes())
}

fn hash_string(hash: u64, value: &str) -> u64 {
    fnv1a(
        fnv1a(hash, &(value.len() as u64).to_le_bytes()),
        value.as_bytes(),
    )
}

fn fnv1a(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(crate::constants::FNV_PRIME);
    }
    hash
}

pub(crate) fn outcome_for_metrics(metrics: &CheckMetrics) -> CheckOutcome {
    if metrics.privacy_violations > 0 {
        CheckOutcome::PrivacyViolation
    } else if metrics.stale_rows > 0 {
        CheckOutcome::Stale
    } else if metrics.missing_keys > 0 || metrics.extra_keys > 0 {
        CheckOutcome::Incomplete
    } else if metrics.mismatched_rows > 0
        || metrics.deterministic_chunk_hash_mismatches > 0
        || metrics.unsupported_comparisons > 0
    {
        CheckOutcome::Mismatched
    } else {
        CheckOutcome::Complete
    }
}
