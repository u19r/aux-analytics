use std::{fs, io::Write, path::PathBuf, sync::Arc, time::Instant};

use analytics_api::{
    AnalyticsEngineAccess, IngestStreamRecordRequest, QueryBatchResult, QueryResponseBuildError,
    build_query_batch_result, build_query_response,
};
use analytics_contract::{
    AnalyticsManifest, PrivacyPolicy, StructuredQuery, TenantRangePurgeRequest,
};
use analytics_engine::{
    AnalyticsEngine, AnalyticsEngineError, CatalogType, IngestOutcome, StorageBackend,
    prepare_tenant_structured_query, prepare_unscoped_structured_query,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use config::{
    AnalyticsCatalogBackend, AnalyticsLambdaResponseCompression, RootConfig,
    load_optional_with_overrides,
};
use flate2::{Compression, write::GzEncoder};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::task::JoinSet;

const ENV_CONFIG_PATH: &str = "AUX_ANALYTICS_CONFIG";
const ENV_MANIFEST_PATH: &str = "AUX_ANALYTICS_MANIFEST";
const ENV_DUCKDB: &str = "AUX_ANALYTICS_DUCKDB";
const ENV_DUCKLAKE_SQLITE_CATALOG: &str = "AUX_ANALYTICS_DUCKLAKE_SQLITE_CATALOG";
const ENV_DUCKLAKE_AUX_CATALOG: &str = "AUX_ANALYTICS_DUCKLAKE_AUX_CATALOG";
const ENV_DUCKLAKE_DATA_PATH: &str = "AUX_ANALYTICS_DUCKLAKE_DATA_PATH";
const ENV_CATALOG_BACKEND: &str = "AUX_ANALYTICS_CATALOG_BACKEND";
const ENV_S3_BUCKET: &str = "AUX_ANALYTICS_S3_BUCKET";
const ENV_S3_PREFIX: &str = "AUX_ANALYTICS_S3_PREFIX";
const ENV_OBJECT_REGION: &str = "AUX_ANALYTICS_OBJECT_REGION";
const ENV_CATALOG_DUCKDB_THREADS: &str = "AUX_ANALYTICS_CATALOG_DUCKDB_THREADS";
const ENV_QUERY_MAX_READ_CONNECTIONS: &str = "AUX_ANALYTICS_QUERY_MAX_READ_CONNECTIONS";
const ENV_QUERY_MAX_RESPONSE_SIZE_KB: &str = "AUX_ANALYTICS_QUERY_MAX_RESPONSE_SIZE_KB";
const ENV_QUERY_LAMBDA_RESPONSE_COMPRESSION: &str =
    "AUX_ANALYTICS_QUERY_LAMBDA_RESPONSE_COMPRESSION";
const ENV_QUERY_LAMBDA_QUERY_ONLY: &str = "AUX_ANALYTICS_QUERY_LAMBDA_QUERY_ONLY";
const ENV_QUERY_DISABLE_DUCKDB_INTERRUPT: &str = "AUX_ANALYTICS_QUERY_DISABLE_DUCKDB_INTERRUPT";

#[derive(Debug, Error)]
pub enum AnalyticsLambdaError {
    #[error("analytics engine error: {0}")]
    Engine(#[from] analytics_engine::AnalyticsEngineError),
    #[error("analytics engine access error: {0}")]
    EngineAccess(#[from] analytics_api::AnalyticsEngineAccessError),
    #[error("config error: {0}")]
    Config(#[from] config::ConfigError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("query response metadata error: {0}")]
    QueryResponse(#[from] QueryResponseBuildError),
    #[error(
        "analytics_response_too_large: serialized response is {actual_bytes} bytes, limit is \
         {limit_bytes} bytes"
    )]
    ResponseTooLarge {
        actual_bytes: usize,
        limit_bytes: usize,
    },
    #[error("schema error: {0}")]
    Schema(String),
    #[error("privacy policy error: {0}")]
    PrivacyPolicy(#[from] analytics_contract::PrivacyPolicyError),
    #[error("query task failed: {0}")]
    QueryTaskJoin(#[from] tokio::task::JoinError),
    #[error("validation error: {0}")]
    Validation(String),
}

impl AnalyticsLambdaError {
    fn schema(message: impl Into<String>) -> Self {
        Self::Schema(message.into())
    }

    fn validation(message: impl Into<String>) -> Self {
        Self::Validation(message.into())
    }
}

pub struct AnalyticsLambdaHandler {
    manifest: Arc<AnalyticsManifest>,
    engine: AnalyticsEngineAccess,
    privacy_policy: Option<Arc<PrivacyPolicy>>,
    response_config: LambdaResponseConfig,
}

#[derive(Debug, Clone, Copy)]
pub struct LambdaResponseConfig {
    max_response_size_kb: Option<usize>,
    response_compression: AnalyticsLambdaResponseCompression,
    query_only: bool,
    disable_duckdb_interrupt: bool,
}

impl Default for LambdaResponseConfig {
    fn default() -> Self {
        Self {
            max_response_size_kb: None,
            response_compression: AnalyticsLambdaResponseCompression::None,
            query_only: false,
            disable_duckdb_interrupt: false,
        }
    }
}

impl From<&config::AnalyticsQueryConfig> for LambdaResponseConfig {
    fn from(query: &config::AnalyticsQueryConfig) -> Self {
        Self {
            max_response_size_kb: query.max_response_size_kb,
            response_compression: query.lambda_response_compression,
            query_only: query.lambda_query_only,
            disable_duckdb_interrupt: query.disable_duckdb_interrupt,
        }
    }
}

impl AnalyticsLambdaHandler {
    pub async fn from_env() -> Result<Self, AnalyticsLambdaError> {
        let config_path = optional_env_path(ENV_CONFIG_PATH);
        let config = load_optional_with_overrides(config_path.as_deref(), &[])?;
        let mut root = config.root.clone();
        apply_lambda_env_config(&mut root, optional_env)?;
        let manifest_path =
            resolve_manifest_path(optional_env(ENV_MANIFEST_PATH).as_deref(), &root)?;
        let manifest = read_manifest(manifest_path.as_str())?;
        let privacy_policy = load_privacy_policy(&root)?.map(Arc::new);
        let backend = resolve_storage_backend_from_env(&root)?;
        Self::new_with_privacy_policy_max_read_connections_and_response_config(
            manifest,
            &backend,
            privacy_policy,
            root.analytics.query.max_read_connections,
            LambdaResponseConfig::from(&root.analytics.query),
        )
    }

    pub fn new(
        manifest: AnalyticsManifest,
        backend: &StorageBackend,
    ) -> Result<Self, AnalyticsLambdaError> {
        Self::new_with_privacy_policy_and_max_read_connections(
            manifest,
            backend,
            None,
            config::DEFAULT_QUERY_MAX_READ_CONNECTIONS,
        )
    }

    pub fn new_with_privacy_policy(
        manifest: AnalyticsManifest,
        backend: &StorageBackend,
        privacy_policy: Option<Arc<PrivacyPolicy>>,
    ) -> Result<Self, AnalyticsLambdaError> {
        Self::new_with_privacy_policy_and_max_read_connections(
            manifest,
            backend,
            privacy_policy,
            config::DEFAULT_QUERY_MAX_READ_CONNECTIONS,
        )
    }

    pub fn new_with_privacy_policy_and_max_read_connections(
        manifest: AnalyticsManifest,
        backend: &StorageBackend,
        privacy_policy: Option<Arc<PrivacyPolicy>>,
        max_read_connections: usize,
    ) -> Result<Self, AnalyticsLambdaError> {
        Self::new_with_privacy_policy_max_read_connections_and_response_config(
            manifest,
            backend,
            privacy_policy,
            max_read_connections,
            LambdaResponseConfig::default(),
        )
    }

    pub fn new_with_privacy_policy_max_read_connections_and_response_config(
        manifest: AnalyticsManifest,
        backend: &StorageBackend,
        privacy_policy: Option<Arc<PrivacyPolicy>>,
        max_read_connections: usize,
        response_config: LambdaResponseConfig,
    ) -> Result<Self, AnalyticsLambdaError> {
        let engine = AnalyticsEngine::connect(backend)?;
        if !response_config.query_only {
            engine.ensure_manifest(&manifest)?;
        }
        let engine = if response_config.query_only {
            AnalyticsEngineAccess::shared(engine)
        } else {
            AnalyticsEngineAccess::backend_aware_with_max_read_connections(
                engine,
                backend.clone(),
                max_read_connections,
            )
        };
        Ok(Self {
            manifest: Arc::new(manifest),
            engine,
            privacy_policy,
            response_config,
        })
    }

    pub async fn handle_event(&self, payload: Value) -> Result<Value, AnalyticsLambdaError> {
        let event = validate_json::<AnalyticsLambdaEvent>(payload)?;
        match event {
            AnalyticsLambdaEvent::Init => Ok(json!(InitResponse {
                initialized: true,
                table_count: self.manifest.tables.len(),
            })),
            AnalyticsLambdaEvent::UnscopedSqlQuery { sql } => {
                let rows = self
                    .engine
                    .with_read(|engine| engine.query_unscoped_sql_json(sql.as_str()))
                    .await??;
                Ok(json!(QueryLambdaResponse { rows }))
            }
            AnalyticsLambdaEvent::UnscopedStructuredQuery { query } => {
                let started = Instant::now();
                let (rows, prepared) = self
                    .engine
                    .with_read(|engine| {
                        let prepared =
                            prepare_unscoped_structured_query(self.manifest.as_ref(), &query)?;
                        let rows = engine.query_prepared_structured_json(&prepared)?;
                        Ok::<_, AnalyticsEngineError>((rows, prepared))
                    })
                    .await??;
                self.query_response(build_query_response(rows, prepared.metadata(), started)?)
            }
            AnalyticsLambdaEvent::TenantQuery {
                target_tenant_id,
                query,
            } => {
                let started = Instant::now();
                let response_config = self.response_config;
                let (rows, prepared) = self
                    .engine
                    .with_read(|engine| {
                        let prepared = prepare_tenant_structured_query(
                            self.manifest.as_ref(),
                            &query,
                            target_tenant_id.as_str(),
                        )?;
                        let rows = if response_config.disable_duckdb_interrupt {
                            engine.query_prepared_structured_json_without_timeout(&prepared)?
                        } else {
                            engine.query_prepared_structured_json(&prepared)?
                        };
                        Ok::<_, AnalyticsEngineError>((rows, prepared))
                    })
                    .await??;
                self.query_response(build_query_response(rows, prepared.metadata(), started)?)
            }
            AnalyticsLambdaEvent::TenantQueryBatch {
                target_tenant_id,
                queries,
            } => {
                let results = self.query_tenant_batch(target_tenant_id, queries).await?;
                self.query_response(QueryBatchLambdaResponse { results })
            }
            AnalyticsLambdaEvent::TenantRangePurge { request } => {
                self.ensure_ingest_allowed()?;
                let response = self
                    .engine
                    .with_write(|engine| engine.purge_tenant_range(&request))
                    .await?;
                Ok(json!(response))
            }
            AnalyticsLambdaEvent::Ingest {
                analytics_table_name,
                request,
            } => {
                self.ensure_ingest_allowed()?;
                let outcome = self
                    .ingest_one(analytics_table_name.as_str(), *request)
                    .await?;
                Ok(json!(IngestLambdaResponse {
                    outcome: ingest_outcome_name(outcome),
                }))
            }
            AnalyticsLambdaEvent::IngestBatch {
                analytics_table_name,
                records,
            } => {
                self.ensure_ingest_allowed()?;
                let mut outcomes = Vec::with_capacity(records.len());
                for request in records {
                    let outcome = self
                        .ingest_one(analytics_table_name.as_str(), request)
                        .await?;
                    outcomes.push(ingest_outcome_name(outcome).to_string());
                }
                Ok(json!(IngestBatchLambdaResponse {
                    processed: outcomes.len(),
                    outcomes,
                }))
            }
        }
    }

    async fn ingest_one(
        &self,
        analytics_table_name: &str,
        request: IngestStreamRecordRequest,
    ) -> Result<IngestOutcome, AnalyticsLambdaError> {
        let (record_key, record) = request.into_contract_record();
        if let Some(policy) = self.privacy_policy.as_ref() {
            return Ok(self
                .engine
                .with_write(|engine| {
                    engine.ingest_stream_record_with_privacy_policy(
                        self.manifest.as_ref(),
                        analytics_table_name,
                        record_key.as_bytes(),
                        record,
                        policy,
                    )
                })
                .await?
                .outcome);
        }
        Ok(self
            .engine
            .with_write(|engine| {
                engine.ingest_stream_record(
                    self.manifest.as_ref(),
                    analytics_table_name,
                    record_key.as_bytes(),
                    record,
                )
            })
            .await?)
    }

    async fn query_tenant_batch(
        &self,
        target_tenant_id: String,
        queries: Vec<TenantQueryBatchItem>,
    ) -> Result<Vec<QueryBatchResult>, AnalyticsLambdaError> {
        let mut tasks = JoinSet::new();
        for (index, query) in queries.into_iter().enumerate() {
            let engine = self.engine.clone();
            let manifest = Arc::clone(&self.manifest);
            let target_tenant_id = target_tenant_id.clone();
            let response_config = self.response_config;
            tasks.spawn(async move {
                let started = Instant::now();
                let (rows, prepared) = engine
                    .with_read(|engine| {
                        let prepared = prepare_tenant_structured_query(
                            manifest.as_ref(),
                            &query.query,
                            target_tenant_id.as_str(),
                        )?;
                        let rows = if response_config.disable_duckdb_interrupt {
                            engine.query_prepared_structured_json_without_timeout(&prepared)?
                        } else {
                            engine.query_prepared_structured_json(&prepared)?
                        };
                        Ok::<_, AnalyticsEngineError>((rows, prepared))
                    })
                    .await??;
                let response =
                    build_query_batch_result(query.name, rows, prepared.metadata(), started)?;
                Ok::<_, AnalyticsLambdaError>((index, response))
            });
        }

        let mut results = Vec::with_capacity(tasks.len());
        while let Some(joined) = tasks.join_next().await {
            results.push(joined??);
        }
        results.sort_by_key(|(index, _)| *index);
        Ok(results.into_iter().map(|(_, result)| result).collect())
    }

    fn ensure_ingest_allowed(&self) -> Result<(), AnalyticsLambdaError> {
        if self.response_config.query_only {
            return Err(AnalyticsLambdaError::validation(
                "ingest operations are disabled when analytics.query.lambda_query_only is true",
            ));
        }
        Ok(())
    }

    fn query_response<T: Serialize>(&self, response: T) -> Result<Value, AnalyticsLambdaError> {
        let value = serde_json::to_value(response)?;
        encode_limited_query_response(value, self.response_config)
    }
}

fn encode_limited_query_response(
    value: Value,
    config: LambdaResponseConfig,
) -> Result<Value, AnalyticsLambdaError> {
    let Some(limit_bytes) = config.max_response_size_kb.map(kib_to_bytes) else {
        return Ok(value);
    };
    let raw = serde_json::to_vec(&value)?;
    if raw.len() <= limit_bytes {
        return Ok(value);
    }
    if config.response_compression != AnalyticsLambdaResponseCompression::GzipBase64 {
        return Err(AnalyticsLambdaError::ResponseTooLarge {
            actual_bytes: raw.len(),
            limit_bytes,
        });
    }
    let compressed = gzip_bytes(raw.as_slice())?;
    let payload = BASE64.encode(compressed);
    let envelope = json!({
        "encoding": "gzip+base64",
        "payload": payload,
    });
    let envelope_size = serde_json::to_vec(&envelope)?.len();
    if envelope_size > limit_bytes {
        return Err(AnalyticsLambdaError::ResponseTooLarge {
            actual_bytes: envelope_size,
            limit_bytes,
        });
    }
    Ok(envelope)
}

fn gzip_bytes(bytes: &[u8]) -> Result<Vec<u8>, AnalyticsLambdaError> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(bytes)?;
    Ok(encoder.finish()?)
}

fn kib_to_bytes(kib: usize) -> usize {
    kib.saturating_mul(1024)
}

fn load_privacy_policy(root: &RootConfig) -> Result<Option<PrivacyPolicy>, AnalyticsLambdaError> {
    let Some(path) = root.analytics.privacy.policy_path.as_deref() else {
        return Ok(None);
    };
    let policy: PrivacyPolicy = serde_json::from_str(fs::read_to_string(path)?.as_str())?;
    policy.validate()?;
    Ok(Some(policy))
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum AnalyticsLambdaEvent {
    Init,
    UnscopedSqlQuery {
        sql: String,
    },
    UnscopedStructuredQuery {
        query: StructuredQuery,
    },
    TenantQuery {
        target_tenant_id: String,
        query: StructuredQuery,
    },
    TenantQueryBatch {
        target_tenant_id: String,
        queries: Vec<TenantQueryBatchItem>,
    },
    TenantRangePurge {
        #[serde(flatten)]
        request: TenantRangePurgeRequest,
    },
    Ingest {
        analytics_table_name: String,
        #[serde(flatten)]
        request: Box<IngestStreamRecordRequest>,
    },
    IngestBatch {
        analytics_table_name: String,
        records: Vec<IngestStreamRecordRequest>,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct InitResponse {
    pub initialized: bool,
    pub table_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryLambdaResponse {
    pub rows: Vec<Value>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
pub struct TenantQueryBatchItem {
    pub name: String,
    pub query: StructuredQuery,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryBatchLambdaResponse {
    pub results: Vec<QueryBatchResult>,
}

#[derive(Debug, Clone, Serialize)]
pub struct IngestLambdaResponse {
    pub outcome: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct IngestBatchLambdaResponse {
    pub processed: usize,
    pub outcomes: Vec<String>,
}

fn validate_json<T>(payload: Value) -> Result<T, AnalyticsLambdaError>
where T: DeserializeOwned + JsonSchema {
    let schema = schemars::schema_for!(T);
    let schema_value = serde_json::to_value(schema)?;
    let validator = jsonschema::validator_for(&schema_value)
        .map_err(|err| AnalyticsLambdaError::schema(err.to_string()))?;
    let errors = validator
        .iter_errors(&payload)
        .map(|error| error.to_string())
        .collect::<Vec<_>>();
    if !errors.is_empty() {
        return Err(AnalyticsLambdaError::validation(format!(
            "lambda event failed schema validation: {}",
            errors.join("; ")
        )));
    }
    Ok(serde_json::from_value(payload)?)
}

fn read_manifest(path: &str) -> Result<AnalyticsManifest, AnalyticsLambdaError> {
    let contents = fs::read_to_string(path)?;
    let value = serde_json::from_str::<Value>(contents.as_str())?;
    validate_json::<AnalyticsManifest>(value.clone())?;
    let manifest = serde_json::from_value::<AnalyticsManifest>(value)?;
    manifest
        .validate()
        .map_err(|err| AnalyticsLambdaError::validation(err.to_string()))?;
    Ok(manifest)
}

fn resolve_manifest_path(
    manifest_env: Option<&str>,
    root: &RootConfig,
) -> Result<String, AnalyticsLambdaError> {
    manifest_env
        .map(ToOwned::to_owned)
        .or_else(|| root.analytics.manifest_path.clone())
        .ok_or_else(|| {
            AnalyticsLambdaError::validation(
                "analytics manifest path is required: set AUX_ANALYTICS_MANIFEST or \
                 analytics.manifest_path",
            )
        })
}

fn resolve_storage_backend_from_env(
    root: &RootConfig,
) -> Result<StorageBackend, AnalyticsLambdaError> {
    resolve_storage_backend_from_env_with(root, optional_env)
}

fn resolve_storage_backend_from_env_with(
    root: &RootConfig,
    env: impl Fn(&str) -> Option<String>,
) -> Result<StorageBackend, AnalyticsLambdaError> {
    if let Some(path) = env(ENV_DUCKDB) {
        return Ok(StorageBackend::DuckDb { path });
    }
    if let Some(catalog_path) = env(ENV_DUCKLAKE_SQLITE_CATALOG) {
        return Ok(StorageBackend::DuckLake {
            catalog: CatalogType::Sqlite,
            catalog_path,
            data_path: required_env_with(ENV_DUCKLAKE_DATA_PATH, &env)?,
            object_storage: Some(Box::new(root.analytics.object_storage.clone())),
            catalog_settings: (&root.analytics.catalog).into(),
        });
    }
    if let Some(catalog_path) = env(ENV_DUCKLAKE_AUX_CATALOG) {
        return Ok(StorageBackend::DuckLake {
            catalog: CatalogType::AuxCatalog,
            catalog_path,
            data_path: required_env_with(ENV_DUCKLAKE_DATA_PATH, &env)?,
            object_storage: Some(Box::new(root.analytics.object_storage.clone())),
            catalog_settings: (&root.analytics.catalog).into(),
        });
    }
    resolve_storage_backend_from_config(root)
}

fn apply_lambda_env_config(
    root: &mut RootConfig,
    env: impl Fn(&str) -> Option<String>,
) -> Result<(), AnalyticsLambdaError> {
    if let Some(backend) = env(ENV_CATALOG_BACKEND) {
        root.analytics.catalog.backend = Some(parse_catalog_backend(backend.as_str())?);
    }
    if let Some(data_path) = env(ENV_DUCKLAKE_DATA_PATH) {
        apply_ducklake_data_path(root, data_path.as_str());
    }
    if let Some(bucket) = env(ENV_S3_BUCKET) {
        root.analytics.object_storage.bucket = Some(bucket);
    }
    if let Some(prefix) = env(ENV_S3_PREFIX) {
        root.analytics.object_storage.path = Some(prefix);
    }
    if let Some(region) = env(ENV_OBJECT_REGION) {
        let endpoint = format!("s3.{region}.amazonaws.com");
        root.analytics.object_storage.region = Some(region);
        if root.analytics.object_storage.endpoint_url.is_none() {
            root.analytics.object_storage.endpoint_url = Some(endpoint);
        }
    }
    if let Some(threads) = env(ENV_CATALOG_DUCKDB_THREADS) {
        root.analytics.catalog.duckdb_threads = Some(parse_usize_env(
            ENV_CATALOG_DUCKDB_THREADS,
            threads.as_str(),
        )?);
    }
    if let Some(max_read_connections) = env(ENV_QUERY_MAX_READ_CONNECTIONS) {
        root.analytics.query.max_read_connections = parse_usize_env(
            ENV_QUERY_MAX_READ_CONNECTIONS,
            max_read_connections.as_str(),
        )?;
    }
    if let Some(limit) = env(ENV_QUERY_MAX_RESPONSE_SIZE_KB) {
        root.analytics.query.max_response_size_kb = Some(parse_usize_env(
            ENV_QUERY_MAX_RESPONSE_SIZE_KB,
            limit.as_str(),
        )?);
    }
    if let Some(compression) = env(ENV_QUERY_LAMBDA_RESPONSE_COMPRESSION) {
        root.analytics.query.lambda_response_compression =
            parse_lambda_response_compression(compression.as_str())?;
    }
    if let Some(query_only) = env(ENV_QUERY_LAMBDA_QUERY_ONLY) {
        root.analytics.query.lambda_query_only =
            parse_bool_env(ENV_QUERY_LAMBDA_QUERY_ONLY, query_only.as_str())?;
    }
    if let Some(disable_interrupt) = env(ENV_QUERY_DISABLE_DUCKDB_INTERRUPT) {
        root.analytics.query.disable_duckdb_interrupt = parse_bool_env(
            ENV_QUERY_DISABLE_DUCKDB_INTERRUPT,
            disable_interrupt.as_str(),
        )?;
    }
    Ok(())
}

fn parse_catalog_backend(value: &str) -> Result<AnalyticsCatalogBackend, AnalyticsLambdaError> {
    match value.trim() {
        "duckdb" => Ok(AnalyticsCatalogBackend::Duckdb),
        "ducklake_sqlite" => Ok(AnalyticsCatalogBackend::DucklakeSqlite),
        "ducklake_aux_catalog" => Ok(AnalyticsCatalogBackend::DucklakeAuxCatalog),
        "ducklake_motherduck" => Ok(AnalyticsCatalogBackend::DucklakeMotherduck),
        other => Err(AnalyticsLambdaError::validation(format!(
            "{ENV_CATALOG_BACKEND} must be one of duckdb, ducklake_sqlite, ducklake_aux_catalog, \
             or ducklake_motherduck; got {other}"
        ))),
    }
}

fn parse_lambda_response_compression(
    value: &str,
) -> Result<AnalyticsLambdaResponseCompression, AnalyticsLambdaError> {
    match value.trim() {
        "none" => Ok(AnalyticsLambdaResponseCompression::None),
        "gzip_base64" => Ok(AnalyticsLambdaResponseCompression::GzipBase64),
        other => Err(AnalyticsLambdaError::validation(format!(
            "{ENV_QUERY_LAMBDA_RESPONSE_COMPRESSION} must be one of none or gzip_base64; got \
             {other}"
        ))),
    }
}

fn parse_usize_env(name: &str, value: &str) -> Result<usize, AnalyticsLambdaError> {
    value.trim().parse::<usize>().map_err(|source| {
        AnalyticsLambdaError::validation(format!("{name} must be a positive integer: {source}"))
    })
}

fn parse_bool_env(name: &str, value: &str) -> Result<bool, AnalyticsLambdaError> {
    match value.trim() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        other => Err(AnalyticsLambdaError::validation(format!(
            "{name} must be true, false, 1, or 0; got {other}"
        ))),
    }
}

fn apply_ducklake_data_path(root: &mut RootConfig, data_path: &str) {
    let trimmed = data_path.trim();
    let Some(rest) = trimmed.strip_prefix("s3://") else {
        root.analytics.object_storage.path = Some(trimmed.to_string());
        return;
    };
    if let Some((bucket, prefix)) = rest.split_once('/') {
        root.analytics.object_storage.bucket = Some(bucket.to_string());
        root.analytics.object_storage.path = Some(prefix.to_string());
    } else if !rest.is_empty() {
        root.analytics.object_storage.bucket = Some(rest.to_string());
    }
}

fn resolve_storage_backend_from_config(
    root: &RootConfig,
) -> Result<StorageBackend, AnalyticsLambdaError> {
    let catalog = &root.analytics.catalog;
    let Some(catalog_backend) = catalog.backend else {
        return Err(AnalyticsLambdaError::validation(
            "analytics catalog backend is required: set an AUX_ANALYTICS_* backend env var or \
             analytics.catalog.backend",
        ));
    };
    let connection_string = catalog.connection_string.as_deref().ok_or_else(|| {
        AnalyticsLambdaError::validation(
            "analytics catalog connection string is required: set an AUX_ANALYTICS_* backend env \
             var or analytics.catalog.connection_string",
        )
    })?;

    match catalog_backend {
        AnalyticsCatalogBackend::Duckdb => Ok(StorageBackend::DuckDb {
            path: connection_string.to_string(),
        }),
        AnalyticsCatalogBackend::DucklakeSqlite => Ok(StorageBackend::DuckLake {
            catalog: CatalogType::Sqlite,
            catalog_path: connection_string.to_string(),
            data_path: ducklake_data_path(root)?,
            object_storage: Some(Box::new(root.analytics.object_storage.clone())),
            catalog_settings: (&root.analytics.catalog).into(),
        }),
        AnalyticsCatalogBackend::DucklakeAuxCatalog => Ok(StorageBackend::DuckLake {
            catalog: CatalogType::AuxCatalog,
            catalog_path: connection_string.to_string(),
            data_path: ducklake_data_path(root)?,
            object_storage: Some(Box::new(root.analytics.object_storage.clone())),
            catalog_settings: (&root.analytics.catalog).into(),
        }),
        AnalyticsCatalogBackend::DucklakeMotherduck => Ok(StorageBackend::DuckLake {
            catalog: CatalogType::MotherDuck,
            catalog_path: connection_string.to_string(),
            data_path: ducklake_data_path(root)?,
            object_storage: Some(Box::new(root.analytics.object_storage.clone())),
            catalog_settings: (&root.analytics.catalog).into(),
        }),
    }
}

fn ducklake_data_path(root: &RootConfig) -> Result<String, AnalyticsLambdaError> {
    let path = root
        .analytics
        .object_storage
        .path
        .as_deref()
        .ok_or_else(|| {
            AnalyticsLambdaError::validation(
                "analytics.object_storage.path is required for DuckLake backends",
            )
        })?;
    if let Some(bucket) = root.analytics.object_storage.bucket.as_deref() {
        return Ok(format!(
            "{}://{}/{}",
            root.analytics
                .object_storage
                .scheme
                .trim_end_matches("://")
                .trim_matches('/'),
            bucket.trim_matches('/'),
            path.trim_start_matches('/')
        ));
    }
    Ok(path.to_string())
}

fn optional_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn optional_env_path(key: &str) -> Option<PathBuf> {
    optional_env(key).map(PathBuf::from)
}

fn required_env_with(
    key: &str,
    env: impl Fn(&str) -> Option<String>,
) -> Result<String, AnalyticsLambdaError> {
    env(key).ok_or_else(|| {
        AnalyticsLambdaError::validation(format!("{key} is required for the selected backend"))
    })
}

fn ingest_outcome_name(outcome: IngestOutcome) -> &'static str {
    match outcome {
        IngestOutcome::Inserted => "inserted",
        IngestOutcome::Updated => "updated",
        IngestOutcome::Deleted => "deleted",
        IngestOutcome::Skipped => "skipped",
    }
}

#[cfg(test)]
#[path = "lambda_tests.rs"]
mod lambda_tests;
