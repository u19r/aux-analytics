use super::{
    TableFixExecutor,
    contracts::{
        TableFixError, TableFixReport, TableFixRequest, TableFixResult, TableFixSourceSnapshot,
        TableFixTruthRecord, VerifiedTableFixDestination,
    },
    verification::{
        ValidatedTableFixSnapshot, preflight_rate_limit, raw_backup_object_ids, selected_key_set,
        selected_row_keys, validate_selected_row_limit,
    },
};
use crate::{
    OperationRateLimiter, RateLimitClock, RateLimitUsage, RawBackupObjectIndex,
    RawBackupObjectStore, enforce_rate_limit_with_limiter,
};

impl<'a, Store: ?Sized> TableFixExecutor<'a, Store> {
    pub(crate) fn execute_raw_backup_verified_with_limiter_at<Destination, Clock: RateLimitClock>(
        &self,
        request: &TableFixRequest,
        indexes: &[RawBackupObjectIndex],
        destination: &mut Destination,
        limiter: &OperationRateLimiter,
        clock: &Clock,
    ) -> TableFixResult<TableFixReport>
    where
        Store: RawBackupObjectStore,
        Destination: VerifiedTableFixDestination,
    {
        request.validate()?;
        preflight_rate_limit(request, limiter, clock)?;
        let selected_keys = selected_row_keys(&request.selection)?;
        let selected_key_set = selected_key_set(selected_keys);
        let selected_rows = u64::try_from(selected_key_set.len()).unwrap_or(u64::MAX);
        validate_selected_row_limit(&request.validation, selected_rows)?;
        let mut records = Vec::new();
        let mut revision_parts = Vec::new();
        for object_id in raw_backup_object_ids(&request.source)? {
            let index = indexes
                .iter()
                .find(|index| &index.object_id == object_id)
                .ok_or_else(|| TableFixError::MissingRawBackupIndex(object_id.clone()))?;
            revision_parts.push(format!(
                "{}:{}:{}:{}:{}",
                index.object_id,
                index.checksum,
                index.record_count,
                index.byte_count,
                index.source_identity,
            ));
            enforce_rate_limit_with_limiter(
                limiter,
                RateLimitUsage {
                    source_requests: 1,
                    ..RateLimitUsage::default()
                },
                clock,
            )?;
            let replay = self.raw_backup_store.replay_object_report(index)?;
            records.extend(replay.records.into_iter().filter(|record| {
                record.table_name == request.table_name
                    && selected_key_set.contains(record.record_key.as_slice())
            }));
        }
        revision_parts.sort_unstable();
        let source_revision = revision_parts.join("|");
        ValidatedTableFixSnapshot::try_new(
            request,
            selected_keys,
            TableFixSourceSnapshot {
                source_revision,
                records: records
                    .into_iter()
                    .map(|record| TableFixTruthRecord {
                        table_name: record.table_name,
                        record_key: record.record_key,
                        record: record.record,
                        stale: false,
                    })
                    .collect(),
            },
        )?
        .execute(destination, &self.authority, limiter, clock)
    }
}
