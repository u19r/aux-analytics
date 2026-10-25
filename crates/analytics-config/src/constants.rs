// Runtime and ingestion defaults.
pub(crate) const DEFAULT_CONFIG_VERSION: &str = "1";
pub(crate) const DEFAULT_HTTP_BIND_ADDR: &str = "0.0.0.0:8080";
pub(crate) const DEFAULT_LOG_LEVEL: &str = "warn";
pub(crate) const DEFAULT_OBJECT_STORAGE_SCHEME: &str = "s3";
pub(crate) const DEFAULT_POLL_INTERVAL_MS: u64 = 100;
pub(crate) const DEFAULT_POLL_MAX_SHARDS: usize = 16;
pub(crate) const DEFAULT_POLL_MAX_RESPONSES_PER_INTERVAL: usize = 4;
pub(crate) const DEFAULT_POLL_MAX_RECORDS_PER_RESPONSE: u32 = 8_192;
pub(crate) const DEFAULT_POLL_REQUEST_TIMEOUT_MS: u64 = 5_000;
pub(crate) const DEFAULT_JOB_STORE_REQUEST_TIMEOUT_MS: u64 = 5_000;
pub(crate) const DEFAULT_INGEST_PROCESSOR_ENABLED: bool = true;
/// Maximum retention delete batch accepted by configuration and runtime
/// consumers.
///
/// Runtime consumers should import this value instead of copying the bound.
pub const MAX_RETENTION_DELETE_BATCH_SIZE: u64 = 10_000;
pub const DEFAULT_QUERY_MAX_READ_CONNECTIONS: usize = 64;

// Retention sweep and cache defaults.
pub(crate) const DEFAULT_RETENTION_SWEEP_INTERVAL_MS: u64 = 60_000;
pub(crate) const DEFAULT_RETENTION_DELETE_BATCH_SIZE: u64 = 500;
pub(crate) const DEFAULT_RETENTION_CACHE_TTL_MS: u64 = 300_000;

// Environment configuration routes.
pub(crate) const ENV_CONFIG_PATH: &str = "AUX_ANALYTICS_CONFIG";
pub(crate) const ENV_MANIFEST_PATH: &str = "AUX_ANALYTICS_MANIFEST";
pub(crate) const ENV_DUCKDB: &str = "AUX_ANALYTICS_DUCKDB";
pub(crate) const ENV_DUCKLAKE_SQLITE_CATALOG: &str = "AUX_ANALYTICS_DUCKLAKE_SQLITE_CATALOG";
pub(crate) const ENV_DUCKLAKE_AUX_CATALOG: &str = "AUX_ANALYTICS_DUCKLAKE_AUX_CATALOG";
pub(crate) const ENV_DUCKLAKE_DATA_PATH: &str = "AUX_ANALYTICS_DUCKLAKE_DATA_PATH";
pub(crate) const ENV_CATALOG_BACKEND: &str = "AUX_ANALYTICS_CATALOG_BACKEND";
pub(crate) const ENV_S3_BUCKET: &str = "AUX_ANALYTICS_S3_BUCKET";
pub(crate) const ENV_S3_PREFIX: &str = "AUX_ANALYTICS_S3_PREFIX";
pub(crate) const ENV_OBJECT_REGION: &str = "AUX_ANALYTICS_OBJECT_REGION";
pub(crate) const ENV_CATALOG_DUCKDB_THREADS: &str = "AUX_ANALYTICS_CATALOG_DUCKDB_THREADS";
pub(crate) const ENV_QUERY_MAX_READ_CONNECTIONS: &str = "AUX_ANALYTICS_QUERY_MAX_READ_CONNECTIONS";
pub(crate) const ENV_QUERY_MAX_RESPONSE_SIZE_KB: &str = "AUX_ANALYTICS_QUERY_MAX_RESPONSE_SIZE_KB";
pub(crate) const ENV_QUERY_LAMBDA_RESPONSE_COMPRESSION: &str =
    "AUX_ANALYTICS_QUERY_LAMBDA_RESPONSE_COMPRESSION";
pub(crate) const ENV_QUERY_LAMBDA_QUERY_ONLY: &str = "AUX_ANALYTICS_QUERY_LAMBDA_QUERY_ONLY";
pub(crate) const ENV_QUERY_DISABLE_QUERY_DEADLINE: &str =
    "AUX_ANALYTICS_QUERY_DISABLE_QUERY_DEADLINE";
