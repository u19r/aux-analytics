use std::time::Duration;

// Retention remote lookup retry policy.
pub(crate) const AUX_STORAGE_POLICY_ATTEMPTS: u32 = 3;
pub(crate) const BASE_BACKOFF_MS: u64 = 50;

// Provider pagination bounds and retention lookup timeout.
pub(crate) const AWS_GET_RECORDS_MAX_RECORDS: u32 = 1_000;
pub(crate) const AWS_DESCRIBE_STREAM_MAX_SHARDS: usize = 100;
pub(crate) const RETENTION_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

// AWS stream discovery retry schedule.
pub(crate) const DISCOVERY_RETRY_INITIAL_DELAY: Duration = Duration::from_secs(1);
pub(crate) const DISCOVERY_RETRY_MAX_DELAY: Duration = Duration::from_secs(30);

// Conversion performance test fixtures and measurement bounds.
#[cfg(test)]
pub(crate) const RECORD_COUNT: usize = 100;
#[cfg(test)]
pub(crate) const PAIRED_SAMPLES: usize = 3;
#[cfg(test)]
pub(crate) const WARMUP_RUNS: usize = 2;
#[cfg(test)]
pub(crate) const REPETITIONS: usize = 16;
#[cfg(test)]
pub(crate) const CURRENT_ROW_PAGE_FIELDS: [&str; 5] =
    ["id", "tenant", "source_position", "private", "payload"];

// Logical record key performance test fixtures and measurement bounds.
#[cfg(test)]
pub(crate) const LOGICAL_KEY_BYTES: usize = 100;
#[cfg(test)]
pub(crate) const LOGICAL_KEY_REPETITIONS: usize = 32;

// Aux storage client test fixtures and measurement bounds.
#[cfg(test)]
pub(crate) const CHECKPOINT: &str = "v1.0123456789abcdef0123456789abcdef";

// Quint polling storage test fixtures and measurement bounds.
#[cfg(test)]
pub(crate) const AUX_RESPONSE_WITHOUT_SCOPE: &str = r#"{"Records":[]}"#;

// Poller system stream test fixtures and measurement bounds.
#[cfg(test)]
pub(crate) const DESCRIBE: &str =
    r#"{"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7}}"#;
#[cfg(test)]
pub(crate) const JOB_PAGE: &str = r#"{"Records":[{"SourceTableName":"job","Keys":{},"SequenceNumber":"job-1","SystemPosition":{"ShardId":"shard-a","LineageId":"job-lineage","Offset":"1"}}],"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7},"Checkpoint":"v1.11111111111111111111111111111111"}"#;
#[cfg(test)]
pub(crate) const SELECTED_PAGE: &str = r#"{"Records":[{"SourceTableName":"source_users","Keys":{},"SequenceNumber":"user-1","SystemPosition":{"ShardId":"shard-a","LineageId":"user-lineage","Offset":"2"}}],"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7},"Checkpoint":"v1.22222222222222222222222222222222"}"#;
#[cfg(test)]
pub(crate) const EMPTY_SELECTED_PAGE: &str = r#"{"Records":[],"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7},"Checkpoint":"v1.22222222222222222222222222222222"}"#;
#[cfg(test)]
pub(crate) const MALFORMED_JOB_PAGE: &str = r#"{"Records":[{"SourceTableName":"job","Keys":{},"SequenceNumber":"job-2"}],"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7},"Checkpoint":"v1.33333333333333333333333333333333"}"#;
#[cfg(test)]
pub(crate) const EMPTY_PAGE: &str = r#"{"Records":[],"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7},"Checkpoint":"v1.11111111111111111111111111111111"}"#;

// Retention performance test fixtures and measurement bounds.
#[cfg(test)]
pub(crate) const RETENTION_PERFORMANCE_REPETITIONS: usize = 32;

// Logical record-key probe selection and calibration.
#[cfg(test)]
pub(crate) const LOGICAL_RECORD_COUNT: usize = 100;
#[cfg(test)]
pub(crate) const LOGICAL_KEY_PAIRED_SAMPLES: usize = 3;
#[cfg(test)]
pub(crate) const LOGICAL_KEY_WARMUP_RUNS: usize = 2;

// Live indexed dataset admission bounds and source descriptor version.
pub(crate) const MAX_LIVE_DATASET_EXAMINED_ROWS: u32 = 10_000;
pub(crate) const MAX_LIVE_DATASET_WINDOW_MS: u64 = 31 * 24 * 60 * 60 * 1000;
pub(crate) const APPLICATION_DATASET_SOURCE_INDEX: &str = "primary_application_status_time_v1";
