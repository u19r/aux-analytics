use std::{fs, path::PathBuf, sync::Arc};

use analytics_api::IngestStreamRecordRequest;
use analytics_contract::{AnalyticsManifest, StructuredQuery};
use analytics_engine::{AnalyticsEngine, CatalogType, IngestOutcome, StorageBackend};
use config::{AnalyticsCatalogBackend, RootConfig, load_optional_with_overrides};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::Mutex;

const ENV_CONFIG_PATH: &str = "AUX_ANALYTICS_CONFIG";
const ENV_MANIFEST_PATH: &str = "AUX_ANALYTICS_MANIFEST";
const ENV_DUCKDB: &str = "AUX_ANALYTICS_DUCKDB";
const ENV_DUCKLAKE_SQLITE_CATALOG: &str = "AUX_ANALYTICS_DUCKLAKE_SQLITE_CATALOG";
const ENV_DUCKLAKE_POSTGRES_CATALOG: &str = "AUX_ANALYTICS_DUCKLAKE_POSTGRES_CATALOG";
const ENV_DUCKLAKE_DATA_PATH: &str = "AUX_ANALYTICS_DUCKLAKE_DATA_PATH";

#[derive(Debug, Error)]
pub enum AnalyticsLambdaError {
    #[error("analytics engine error: {0}")]
    Engine(#[from] analytics_engine::AnalyticsEngineError),
    #[error("config error: {0}")]
    Config(#[from] config::ConfigError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("schema error: {0}")]
    Schema(String),
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
    engine: Arc<Mutex<AnalyticsEngine>>,
}

impl AnalyticsLambdaHandler {
    pub fn from_env() -> Result<Self, AnalyticsLambdaError> {
        let config_path = optional_env_path(ENV_CONFIG_PATH);
        let config = load_optional_with_overrides(config_path.as_deref(), &[])?;
        let root = config.root.clone();
        let manifest_path =
            resolve_manifest_path(optional_env(ENV_MANIFEST_PATH).as_deref(), &root)?;
        let manifest = read_manifest(manifest_path.as_str())?;
        let backend = resolve_storage_backend_from_env(&root)?;
        Self::new(manifest, &backend)
    }

    pub fn new(
        manifest: AnalyticsManifest,
        backend: &StorageBackend,
    ) -> Result<Self, AnalyticsLambdaError> {
        let engine = AnalyticsEngine::connect(backend)?;
        engine.ensure_manifest(&manifest)?;
        Ok(Self {
            manifest: Arc::new(manifest),
            engine: Arc::new(Mutex::new(engine)),
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
                let engine = self.engine.lock().await;
                let rows = engine.query_unscoped_sql_json(sql.as_str())?;
                Ok(json!(QueryLambdaResponse { rows }))
            }
            AnalyticsLambdaEvent::UnscopedStructuredQuery { query } => {
                let engine = self.engine.lock().await;
                let rows = engine.query_unscoped_structured_json(self.manifest.as_ref(), &query)?;
                Ok(json!(QueryLambdaResponse { rows }))
            }
            AnalyticsLambdaEvent::TenantQuery {
                target_tenant_id,
                query,
            } => {
                let engine = self.engine.lock().await;
                let rows = engine.query_tenant_structured_json(
                    self.manifest.as_ref(),
                    &query,
                    target_tenant_id.as_str(),
                )?;
                Ok(json!(QueryLambdaResponse { rows }))
            }
            AnalyticsLambdaEvent::Ingest {
                analytics_table_name,
                request,
            } => {
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
        let engine = self.engine.lock().await;
        Ok(engine.ingest_stream_record(
            self.manifest.as_ref(),
            analytics_table_name,
            record_key.as_bytes(),
            record,
        )?)
    }
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
    if let Some(path) = optional_env(ENV_DUCKDB) {
        return Ok(StorageBackend::DuckDb { path });
    }
    if let Some(catalog_path) = optional_env(ENV_DUCKLAKE_SQLITE_CATALOG) {
        return Ok(StorageBackend::DuckLake {
            catalog: CatalogType::Sqlite,
            catalog_path,
            data_path: required_env(ENV_DUCKLAKE_DATA_PATH)?,
            object_storage: None,
        });
    }
    if let Some(catalog_path) = optional_env(ENV_DUCKLAKE_POSTGRES_CATALOG) {
        return Ok(StorageBackend::DuckLake {
            catalog: CatalogType::Postgres,
            catalog_path,
            data_path: required_env(ENV_DUCKLAKE_DATA_PATH)?,
            object_storage: None,
        });
    }
    resolve_storage_backend_from_config(root)
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
        }),
        AnalyticsCatalogBackend::DucklakePostgres => Ok(StorageBackend::DuckLake {
            catalog: CatalogType::Postgres,
            catalog_path: connection_string.to_string(),
            data_path: ducklake_data_path(root)?,
            object_storage: Some(Box::new(root.analytics.object_storage.clone())),
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

fn required_env(key: &str) -> Result<String, AnalyticsLambdaError> {
    optional_env(key).ok_or_else(|| {
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
