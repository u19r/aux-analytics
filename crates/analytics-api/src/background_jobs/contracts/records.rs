use super::{PolledRecord, SourceJobRecord};

pub(super) fn coalesce_records(records: Vec<PolledRecord>) -> Vec<SourceJobRecord> {
    let mut grouped: Vec<SourceJobRecord> = Vec::new();
    for record in records {
        if let Some(previous) = grouped.last_mut().filter(|previous| {
            previous.source_table_name == record.source_table_name
                && previous.record_key == record.record_key.as_bytes()
                && previous.system_position == record.system_position
                && previous.record == record.record
        }) {
            previous
                .analytics_table_names
                .extend(record.analytics_table_names);
            continue;
        }
        grouped.push(SourceJobRecord {
            source_table_name: record.source_table_name,
            analytics_table_names: record.analytics_table_names,
            record_key: record.record_key.into_bytes(),
            record: record.record,
            system_position: record.system_position,
        });
    }
    grouped
}

impl SourceJobRecord {
    pub(super) fn into_polled_record(self, table_scope: Option<(&str, &str)>) -> PolledRecord {
        let shard_id = self
            .system_position
            .as_ref()
            .map(|position| position.shard_id().as_str().to_owned())
            .or_else(|| table_scope.map(|(_, shard)| shard.to_owned()))
            .expect("validated source scope");
        PolledRecord {
            source_table_name: self.source_table_name,
            shard_id,
            analytics_table_names: self.analytics_table_names,
            record_key: String::from_utf8(self.record_key).expect("validated UTF-8 key"),
            record: self.record,
            system_position: self.system_position,
        }
    }
}
