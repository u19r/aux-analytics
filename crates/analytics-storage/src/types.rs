use analytics_contract::StorageStreamRecord;

#[derive(Debug, Clone)]
pub struct PolledRecord {
    pub analytics_table_name: String,
    pub record_key: String,
    pub record: StorageStreamRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceCheckpoint {
    pub source_table_name: String,
    pub shard_id: String,
    pub position: String,
}

#[derive(Debug, Clone)]
pub struct PollBatch {
    pub records: Vec<PolledRecord>,
    pub checkpoints: Vec<SourceCheckpoint>,
}
