use analytics_engine::StreamRecordBatchItem;
use analytics_storage::PolledRecord;

use super::observation::SourceJobShared;

impl SourceJobShared {
    /// Defers destination ownership until native intake and moves the final
    /// item.
    pub(super) fn into_destination_records(
        record: PolledRecord,
    ) -> impl Iterator<Item = StreamRecordBatchItem> {
        let mut destinations = record.analytics_table_names.into_iter();
        let mut final_item = Some(StreamRecordBatchItem {
            source_table_name: record.source_table_name,
            analytics_table_name: String::new(),
            shard_id: Some(record.shard_id),
            record_key: record.record_key.into_bytes(),
            record: record.record,
            system_position: record.system_position,
        });
        std::iter::from_fn(move || {
            let analytics_table_name = destinations.next()?;
            let mut item = if destinations.len() == 0 {
                final_item.take()?
            } else {
                final_item.as_ref()?.clone()
            };
            item.analytics_table_name = analytics_table_name;
            Some(item)
        })
    }
}
