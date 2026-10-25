use std::path::Path;

use crate::{
    AnalyticsCatalogBackend, AnalyticsCatalogConfig, AnalyticsObjectStorageConfig,
    AnalyticsRetentionConfig, AnalyticsSourceConfig, ConfigError, ConfigErrorDebug,
    ConfigErrorKind, RootConfig, TenantRetentionPolicyRequest, TenantRetentionPolicySource,
    constants::MAX_RETENTION_DELETE_BATCH_SIZE, load_optional_with_overrides,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BackendOverride {
    pub duckdb: Option<String>,
    pub ducklake_sqlite_catalog: Option<String>,
    pub ducklake_aux_catalog: Option<String>,
    pub ducklake_data_path: Option<String>,
}

impl BackendOverride {
    #[must_use]
    pub fn has_backend(&self) -> bool {
        self.duckdb.is_some()
            || self.ducklake_sqlite_catalog.is_some()
            || self.ducklake_aux_catalog.is_some()
    }

    fn resolve_storage_backend(
        &self,
        root: Option<&RootConfig>,
    ) -> Result<StorageBackend, ConfigError> {
        if let Some(path) = self.duckdb.as_ref() {
            if path.trim().is_empty() {
                return Err(ConfigError::new(ConfigErrorKind::MissingBackend));
            }
            return Ok(StorageBackend::DuckDb { path: path.clone() });
        }
        let selection = self
            .ducklake_sqlite_catalog
            .as_ref()
            .map(|path| (CatalogType::Sqlite, path))
            .or_else(|| {
                self.ducklake_aux_catalog
                    .as_ref()
                    .map(|path| (CatalogType::AuxCatalog, path))
            });
        let Some((catalog, catalog_path)) = selection else {
            return Err(ConfigError::new(ConfigErrorKind::MissingBackend));
        };
        if catalog_path.trim().is_empty() {
            return Err(ConfigError::new(ConfigErrorKind::MissingBackend));
        }
        Ok(StorageBackend::new_ducklake(
            catalog,
            catalog_path.clone(),
            self.get_ducklake_data_path()?,
            root,
        ))
    }

    fn get_ducklake_data_path(&self) -> Result<String, ConfigError> {
        let path = self
            .ducklake_data_path
            .clone()
            .ok_or_else(|| ConfigError::new(ConfigErrorKind::MissingDucklakeDataPath))?;
        if path.trim().is_empty() {
            return Err(ConfigError::new(ConfigErrorKind::MissingDucklakeDataPath));
        }
        Ok(path)
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
        catalog_settings: DuckLakeCatalogSettings,
    },
}

impl StorageBackend {
    fn new_ducklake(
        catalog: CatalogType,
        catalog_path: String,
        data_path: String,
        root: Option<&RootConfig>,
    ) -> Self {
        Self::DuckLake {
            catalog,
            catalog_path,
            data_path,
            object_storage: root.map(|root| Box::new(root.analytics.object_storage.clone())),
            catalog_settings: root.map_or_else(DuckLakeCatalogSettings::default, |root| {
                DuckLakeCatalogSettings::from(&root.analytics.catalog)
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogType {
    Sqlite,
    AuxCatalog,
    MotherDuck,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DuckLakeCatalogSettings {
    pub motherduck_token: Option<String>,
    pub duckdb_threads: Option<usize>,
}

impl From<&AnalyticsCatalogConfig> for DuckLakeCatalogSettings {
    fn from(config: &AnalyticsCatalogConfig) -> Self {
        Self {
            motherduck_token: config.motherduck_token.clone(),
            duckdb_threads: config.duckdb_threads,
        }
    }
}

pub fn resolve_storage_backend(
    backend_override: &BackendOverride,
    root: &RootConfig,
) -> Result<StorageBackend, ConfigError> {
    if backend_override.has_backend() {
        return backend_override.resolve_storage_backend(None);
    }

    let catalog = &root.analytics.catalog;
    let Some(catalog_backend) = catalog.backend else {
        return Err(ConfigError::new(ConfigErrorKind::MissingCatalogBackend));
    };
    let connection_string = catalog
        .connection_string
        .as_deref()
        .ok_or_else(|| ConfigError::new(ConfigErrorKind::MissingCatalogConnectionString))?;
    if connection_string.trim().is_empty() {
        return Err(ConfigError::new(
            ConfigErrorKind::MissingCatalogConnectionString,
        ));
    }

    let catalog_type = match catalog_backend {
        AnalyticsCatalogBackend::Duckdb => {
            return Ok(StorageBackend::DuckDb {
                path: connection_string.to_string(),
            });
        }
        AnalyticsCatalogBackend::DucklakeSqlite => CatalogType::Sqlite,
        AnalyticsCatalogBackend::DucklakeAuxCatalog => CatalogType::AuxCatalog,
        AnalyticsCatalogBackend::DucklakeMotherduck => CatalogType::MotherDuck,
    };
    Ok(StorageBackend::new_ducklake(
        catalog_type,
        connection_string.to_string(),
        resolve_object_storage_path(root)?,
        Some(root),
    ))
}

pub(crate) fn resolve_storage_backend_with_root(
    backend_override: &BackendOverride,
    root: &RootConfig,
) -> Result<StorageBackend, ConfigError> {
    if backend_override.has_backend() {
        return backend_override.resolve_storage_backend(Some(root));
    }
    resolve_storage_backend(backend_override, root)
}

fn resolve_object_storage_path(root: &RootConfig) -> Result<String, ConfigError> {
    let path = root
        .analytics
        .object_storage
        .path
        .as_deref()
        .ok_or_else(|| ConfigError::new(ConfigErrorKind::MissingDucklakeObjectStoragePath))?;
    if path.trim().is_empty() {
        return Err(ConfigError::new(
            ConfigErrorKind::MissingDucklakeObjectStoragePath,
        ));
    }
    if let Some(bucket) = root.analytics.object_storage.bucket.as_deref() {
        let scheme = root
            .analytics
            .object_storage
            .scheme
            .trim_end_matches("://")
            .trim_matches('/');
        return Ok(format!(
            "{}://{}/{}",
            scheme,
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

pub fn validate_retention_config(retention: &AnalyticsRetentionConfig) -> Result<(), ConfigError> {
    if !retention.enabled {
        return Ok(());
    }
    if retention.sweep_interval_ms == 0 || retention.delete_batch_size == 0 {
        return Err(ConfigError::with_debug(
            ConfigErrorKind::InvalidRetentionConfig,
            ConfigErrorDebug::Message(
                "sweep_interval_ms and delete_batch_size must be greater than zero".to_string(),
            ),
        ));
    }
    if retention.delete_batch_size > MAX_RETENTION_DELETE_BATCH_SIZE {
        return Err(ConfigError::with_debug(
            ConfigErrorKind::InvalidRetentionConfig,
            ConfigErrorDebug::Message(format!(
                "delete_batch_size must be at most {MAX_RETENTION_DELETE_BATCH_SIZE}"
            )),
        ));
    }
    for table in &retention.tables {
        table.validate()?;
    }
    Ok(())
}

impl crate::AnalyticsRetentionTableConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        let table = self;
        validate_non_empty_retention_field(
            table.analytics_table_name.as_str(),
            "analytics_table_name",
            table.analytics_table_name.as_str(),
        )?;
        if table.default_period_ms == 0 {
            return Err(create_retention_table_error(
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
        table
            .tenant_policy
            .validate_source(&table.analytics_table_name)?;
        table
            .tenant_policy
            .request
            .validate(&table.analytics_table_name)
    }
}

impl crate::TenantRetentionPolicyConfig {
    fn validate_source(&self, table_name: &str) -> Result<(), ConfigError> {
        match self.source {
            TenantRetentionPolicySource::AuxStorage => {
                if self.endpoint_url.as_deref().is_none_or(str::is_empty) {
                    return Err(create_retention_table_error(
                        table_name,
                        "tenant_policy.endpoint_url is required for aux_storage",
                    ));
                }
            }
            TenantRetentionPolicySource::DynamoDb => {}
        }
        Ok(())
    }
}

impl TenantRetentionPolicyRequest {
    fn validate(&self, table_name: &str) -> Result<(), ConfigError> {
        match self {
            Self::GetItem(request) => request.validate(table_name),
            Self::QueryTable(request) => request.validate(table_name),
        }
    }
}

impl crate::TenantRetentionGetItemRequest {
    fn validate(&self, table_name: &str) -> Result<(), ConfigError> {
        validate_non_empty_retention_field(
            table_name,
            "tenant_policy.request.get_item.table_name",
            self.table_name.as_str(),
        )?;
        if self.key.is_empty() {
            return Err(create_retention_table_error(
                table_name,
                "tenant_policy.request.get_item.key must not be empty",
            ));
        }
        Ok(())
    }
}

impl crate::TenantRetentionQueryTableRequest {
    fn validate(&self, table_name: &str) -> Result<(), ConfigError> {
        validate_non_empty_retention_field(
            table_name,
            "tenant_policy.request.query_table.table_name",
            self.table_name.as_str(),
        )?;
        validate_non_empty_retention_field(
            table_name,
            "tenant_policy.request.query_table.key_condition_expression",
            self.key_condition_expression.as_str(),
        )?;
        if self.expression_attribute_values.is_empty() {
            return Err(create_retention_table_error(
                table_name,
                "tenant_policy.request.query_table.expression_attribute_values must not be empty",
            ));
        }
        if self.limit != 1 {
            return Err(create_retention_table_error(
                table_name,
                "tenant_policy.request.query_table.limit must equal 1",
            ));
        }
        Ok(())
    }
}

fn validate_non_empty_retention_field(
    table_name: &str,
    field: &str,
    value: &str,
) -> Result<(), ConfigError> {
    if value.trim().is_empty() {
        return Err(create_retention_table_error(
            table_name,
            format!("{field} must not be empty"),
        ));
    }
    Ok(())
}

fn create_retention_table_error(table_name: &str, message: impl Into<String>) -> ConfigError {
    ConfigError::with_debug(
        ConfigErrorKind::InvalidRetentionConfig,
        ConfigErrorDebug::Message(format!(
            "{}: analytics_table_name={table_name}",
            message.into()
        )),
    )
}
