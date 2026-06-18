use analytics_api::TableLagHealth;
use analytics_contract::{StorageItem, StorageStreamRecord, StorageValue};
use analytics_storage::{IngestRateRollup, PollBatch, SourceProgress};

use crate::source_polling::metrics::*;

pub(crate) fn record_hashed_range_table_metrics(
    batch: &PollBatch,
    source_table_id: &str,
    marker_versionstamp: &str,
    progress: &SourceProgress,
    previous_progress: Option<&SourceProgress>,
) -> TableLagHealth {
    metrics::counter!(
        INGEST_TABLE_RECORDS_TOTAL_METRIC,
        "table" => source_table_id.to_string()
    )
    .increment(batch.records.len() as u64);
    let bytes_total = batch_source_bytes(batch);
    metrics::counter!(
        INGEST_TABLE_BYTES_TOTAL_METRIC,
        "table" => source_table_id.to_string()
    )
    .increment(bytes_total);
    let lag_ms = versionstamp_lag_ms(marker_versionstamp, progress.versionstamp.as_str());
    if let Some(lag_ms) = lag_ms {
        metrics::histogram!(
            INGEST_TABLE_LAG_MS_METRIC,
            "table" => source_table_id.to_string()
        )
        .record(lag_ms as f64);
    }
    let mut cursor_age_ms = None;
    if let Some(previous_progress) = previous_progress {
        let age_ms = progress
            .updated_at_ms
            .saturating_sub(previous_progress.updated_at_ms);
        cursor_age_ms = Some(age_ms as u64);
        metrics::histogram!(
            INGEST_TABLE_CURSOR_AGE_MS_METRIC,
            "table" => source_table_id.to_string()
        )
        .record(age_ms as f64);
    }
    TableLagHealth {
        source_table_name: source_table_id.to_string(),
        versionstamp: progress.versionstamp.clone(),
        lag_ms,
        cursor_age_ms,
        updated_at_ms: Some(progress.updated_at_ms as u128),
    }
}

pub(crate) fn upsert_table_lag_health(table_lag: &mut Vec<TableLagHealth>, next: TableLagHealth) {
    if let Some(existing) = table_lag
        .iter_mut()
        .find(|lag| lag.source_table_name == next.source_table_name)
    {
        *existing = next;
    } else {
        table_lag.push(next);
    }
}

pub(crate) fn ingest_rate_rollup_from_batch(
    batch: &PollBatch,
    source_table_id: &str,
    marker_versionstamp: &str,
    progress: &SourceProgress,
    now_ms: i64,
) -> Option<IngestRateRollup> {
    if batch.records.is_empty() {
        return None;
    }
    Some(IngestRateRollup {
        source_table_id: source_table_id.to_string(),
        tenant_id: batch_tenant_id(batch),
        window_started_at_ms: now_ms - now_ms.rem_euclid(INGEST_RATE_ROLLUP_WINDOW_MS),
        updated_at_ms: now_ms,
        records_total: batch.records.len() as u64,
        bytes_total: batch_source_bytes(batch),
        cursor: progress.cursor.clone(),
        versionstamp: marker_versionstamp.to_string(),
    })
}

pub(crate) fn batch_source_bytes(batch: &PollBatch) -> u64 {
    batch
        .records
        .iter()
        .map(|record| serde_json::to_vec(&record.record).map_or(0, |bytes| bytes.len() as u64))
        .sum()
}

pub(crate) fn batch_tenant_id(batch: &PollBatch) -> String {
    batch
        .records
        .iter()
        .find_map(|record| tenant_id_from_record(&record.record))
        .unwrap_or_else(|| "unknown".to_string())
}

fn tenant_id_from_record(record: &StorageStreamRecord) -> Option<String> {
    tenant_id_from_item(record.new_image.as_ref())
        .or_else(|| tenant_id_from_item(Some(&record.keys)))
        .or_else(|| tenant_id_from_item(record.old_image.as_ref()))
}

fn tenant_id_from_item(item: Option<&StorageItem>) -> Option<String> {
    item.and_then(|item| match item.get("tenant_id") {
        Some(StorageValue::S(value)) if !value.trim().is_empty() => Some(value.clone()),
        _ => None,
    })
}

pub(crate) fn versionstamp_lag_ms(
    marker_versionstamp: &str,
    progress_versionstamp: &str,
) -> Option<u64> {
    let marker = marker_versionstamp.parse::<u128>().ok()?;
    let progress = progress_versionstamp.parse::<u128>().ok()?;
    Some(progress.saturating_sub(marker).min(u128::from(u64::MAX)) as u64)
}
