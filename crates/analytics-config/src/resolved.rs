use std::path::Path;

use crate::{
    AnalyticsCatalogBackend, AnalyticsObjectStorageConfig, AnalyticsRetentionConfig,
    AnalyticsSourceConfig, ConfigError, ConfigErrorDebug, ConfigErrorKind, RootConfig,
    TenantRetentionPolicyRequest, TenantRetentionPolicySource, load_optional_with_overrides,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BackendOverride {
    pub duckdb: Option<String>,
    pub ducklake_sqlite_catalog: Option<String>,
    pub ducklake_postgres_catalog: Option<String>,
    pub ducklake_data_path: Option<String>,
}

impl BackendOverride {
    #[must_use]
    pub fn has_backend(&self) -> bool {
        self.duckdb.is_some()
            || self.ducklake_sqlite_catalog.is_some()
            || self.ducklake_postgres_catalog.is_some()
    }

    fn storage_backend(&self) -> Result<StorageBackend, ConfigError> {
        if let Some(path) = self.duckdb.as_ref() {
            return Ok(StorageBackend::DuckDb { path: path.clone() });
        }
        if let Some(catalog_path) = self.ducklake_sqlite_catalog.as_ref() {
            return Ok(StorageBackend::DuckLake {
                catalog: CatalogType::Sqlite,
                catalog_path: catalog_path.clone(),
                data_path: self.required_ducklake_data_path()?,
                object_storage: None,
            });
        }
        if let Some(catalog_path) = self.ducklake_postgres_catalog.as_ref() {
            return Ok(StorageBackend::DuckLake {
                catalog: CatalogType::Postgres,
                catalog_path: catalog_path.clone(),
                data_path: self.required_ducklake_data_path()?,
                object_storage: None,
            });
        }
        Err(ConfigError::new(ConfigErrorKind::MissingBackend))
    }

    fn required_ducklake_data_path(&self) -> Result<String, ConfigError> {
        self.ducklake_data_path
            .clone()
            .ok_or_else(|| ConfigError::new(ConfigErrorKind::MissingDucklakeDataPath))
    }
}

#[derive(Debug, Clone)]
pub enum StorageBackend {
    DuckDb {
        path: String,
    },
    DuckLake {
        catalog: CatalogType,
        catalog_path: String,
        data_path: String,
        object_storage: Option<Box<AnalyticsObjectStorageConfig>>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogType {
    Sqlite,
    Postgres,
}

pub fn resolve_storage_backend(
    backend_override: &BackendOverride,
    root: &RootConfig,
) -> Result<StorageBackend, ConfigError> {
    if backend_override.has_backend() {
        return backend_override.storage_backend();
    }

    let catalog = &root.analytics.catalog;
    let Some(catalog_backend) = catalog.backend else {
        return Err(ConfigError::new(ConfigErrorKind::MissingCatalogBackend));
    };
    let connection_string = catalog
        .connection_string
        .as_deref()
        .ok_or_else(|| ConfigError::new(ConfigErrorKind::MissingCatalogConnectionString))?;

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

fn ducklake_data_path(root: &RootConfig) -> Result<String, ConfigError> {
    let path = root
        .analytics
        .object_storage
        .path
        .as_deref()
        .ok_or_else(|| ConfigError::new(ConfigErrorKind::MissingDucklakeObjectStoragePath))?;
    if let Some(bucket) = root.analytics.object_storage.bucket.as_deref() {
        return Ok(format!(
            "{}://{}/{}",
            root.analytics.object_storage.scheme.trim_matches('/'),
            bucket.trim_matches('/'),
            path.trim_start_matches('/')
        ));
    }
    Ok(path.to_string())
}

pub fn load_with_override_args(
    config_path: Option<&Path>,
    override_args: &[String],
) -> Result<std::sync::Arc<crate::Config>, ConfigError> {
    let overrides = parse_override_args(override_args)?;
    load_optional_with_overrides(config_path, overrides.as_slice())
}

pub fn parse_override_args(values: &[String]) -> Result<Vec<(String, String)>, ConfigError> {
    values
        .iter()
        .map(|value| {
            let Some((path, raw)) = value.split_once('=') else {
                return Err(ConfigError::argument(format!(
                    "invalid override '{value}', expected PATH=VALUE"
                )));
            };
            if path.trim().is_empty() {
                return Err(ConfigError::argument(format!(
                    "invalid override '{value}', path is empty"
                )));
            }
            Ok((path.to_string(), raw.to_string()))
        })
        .collect()
}

pub fn resolve_manifest_path(
    manifest_arg: Option<&str>,
    root: &RootConfig,
) -> Result<String, ConfigError> {
    manifest_arg
        .map(ToOwned::to_owned)
        .or_else(|| root.analytics.manifest_path.clone())
        .ok_or_else(|| ConfigError::new(ConfigErrorKind::MissingManifestPath))
}

pub fn validate_source_config(source: &AnalyticsSourceConfig) -> Result<(), ConfigError> {
    for table in &source.tables {
        if table.stream_type.or(source.stream_type).is_none() {
            return Err(ConfigError::with_debug(
                ConfigErrorKind::InvalidSourceTableStreamType,
                ConfigErrorDebug::SourceTableName(table.table_name.clone()),
            ));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
pub fn validate_retention_config(retention: &AnalyticsRetentionConfig) -> Result<(), ConfigError> {
    if !retention.enabled {
        return Ok(());
    }
    if retention.sweep_interval_ms == 0
        || retention.delete_batch_size == 0
        || retention.delete_batch_pause_ms == 0
    {
        return Err(ConfigError::with_debug(
            ConfigErrorKind::InvalidRetentionConfig,
            ConfigErrorDebug::Message(
                "sweep_interval_ms, delete_batch_size, and delete_batch_pause_ms must be greater \
                 than zero"
                    .to_string(),
            ),
        ));
    }
    for table in &retention.tables {
        validate_non_empty_retention_field(
            table.analytics_table_name.as_str(),
            "analytics_table_name",
            table.analytics_table_name.as_str(),
        )?;
        if table.default_period_ms == 0 {
            return Err(retention_table_error(
                table.analytics_table_name.as_str(),
                "default_period_ms must be greater than zero",
            ));
        }
        match &table.timestamp {
            crate::RetentionTimestampConfig::Attribute { attribute_path } => {
                validate_non_empty_retention_field(
                    table.analytics_table_name.as_str(),
                    "timestamp.attribute_path",
                    attribute_path.as_str(),
                )?;
            }
            crate::RetentionTimestampConfig::IngestedAt => {}
        }
        validate_non_empty_retention_field(
            table.analytics_table_name.as_str(),
            "duration_selector.attribute_path",
            table
                .tenant_policy
                .duration_selector
                .attribute_path
                .as_str(),
        )?;
        match table.tenant_policy.source {
            TenantRetentionPolicySource::AuxStorage => {
                if table
                    .tenant_policy
                    .endpoint_url
                    .as_deref()
                    .is_none_or(str::is_empty)
                {
                    return Err(retention_table_error(
                        table.analytics_table_name.as_str(),
                        "tenant_policy.endpoint_url is required for aux_storage",
                    ));
                }
            }
            TenantRetentionPolicySource::DynamoDb => {}
        }
        match &table.tenant_policy.request {
            TenantRetentionPolicyRequest::GetItem(request) => {
                validate_non_empty_retention_field(
                    table.analytics_table_name.as_str(),
                    "tenant_policy.request.get_item.table_name",
                    request.table_name.as_str(),
                )?;
                if request.key.is_empty() {
                    return Err(retention_table_error(
                        table.analytics_table_name.as_str(),
                        "tenant_policy.request.get_item.key must not be empty",
                    ));
                }
            }
            TenantRetentionPolicyRequest::QueryTable(request) => {
                validate_non_empty_retention_field(
                    table.analytics_table_name.as_str(),
                    "tenant_policy.request.query_table.table_name",
                    request.table_name.as_str(),
                )?;
                validate_non_empty_retention_field(
                    table.analytics_table_name.as_str(),
                    "tenant_policy.request.query_table.key_condition_expression",
                    request.key_condition_expression.as_str(),
                )?;
                if request.expression_attribute_values.is_empty() {
                    return Err(retention_table_error(
                        table.analytics_table_name.as_str(),
                        "tenant_policy.request.query_table.expression_attribute_values must not \
                         be empty",
                    ));
                }
                if request.limit != 1 {
                    return Err(retention_table_error(
                        table.analytics_table_name.as_str(),
                        "tenant_policy.request.query_table.limit must equal 1",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn validate_non_empty_retention_field(
    table_name: &str,
    field: &str,
    value: &str,
) -> Result<(), ConfigError> {
    if value.trim().is_empty() {
        return Err(retention_table_error(
            table_name,
            format!("{field} must not be empty"),
        ));
    }
    Ok(())
}

fn retention_table_error(table_name: &str, message: impl Into<String>) -> ConfigError {
    ConfigError::with_debug(
        ConfigErrorKind::InvalidRetentionConfig,
        ConfigErrorDebug::Message(format!(
            "{}: analytics_table_name={table_name}",
            message.into()
        )),
    )
}
