use analytics_contract::StorageStreamRecord;

#[derive(Debug, Clone)]
pub struct PolledRecord {
    pub source_table_name: String,
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
    pub source_response_count: usize,
    pub source_nonempty_response_count: usize,
    pub source_record_count: usize,
    pub source_encoded_bytes: usize,
    pub records: Vec<PolledRecord>,
    pub checkpoints: Vec<SourceCheckpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotChunkCheckpoint {
    pub source_table_name: String,
    pub chunk_id: String,
}

#[derive(Debug, Clone)]
pub struct SnapshotChunk {
    pub checkpoint: SnapshotChunkCheckpoint,
    pub records: Vec<PolledRecord>,
}
