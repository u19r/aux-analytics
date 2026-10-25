use std::{collections::BTreeMap, path::PathBuf};

use analytics_contract::{AnalyticsManifest, read_manifest};

use crate::{
    AnalyticsCatalogBackend, AnalyticsLambdaResponseCompression, BackendOverride, ConfigError,
    ConfigErrorDebug, ConfigErrorKind, RootConfig, StorageBackend, constants::*,
    load_optional_with_overrides, load_with_override_args, resolve_storage_backend,
    resolved::resolve_storage_backend_with_root, validate_retention_config, validate_source_config,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigCaller {
    Cli,
    Api,
    Lambda,
}

#[derive(Debug, Clone)]
pub struct ConfigRequest {
    caller: ConfigCaller,
    config_path: Option<PathBuf>,
    manifest_argument: Option<String>,
    override_args: Vec<String>,
    backend_override: BackendOverride,
    lambda_environment: Option<LambdaEnvironment>,
    manifest_required: bool,
    backend_required: bool,
}

impl ConfigRequest {
    pub fn cli(
        config_path: Option<PathBuf>,
        manifest_argument: Option<String>,
        override_args: Vec<String>,
        backend_override: BackendOverride,
    ) -> Self {
        Self {
            caller: ConfigCaller::Cli,
            config_path,
            manifest_argument,
            override_args,
            backend_override,
            lambda_environment: None,
            manifest_required: true,
            backend_required: true,
        }
    }

    pub fn cli_without_manifest(
        config_path: Option<PathBuf>,
        override_args: Vec<String>,
        backend_override: BackendOverride,
    ) -> Self {
        Self {
            manifest_required: false,
            ..Self::cli(config_path, None, override_args, backend_override)
        }
    }

    pub fn cli_manifest_only(
        config_path: Option<PathBuf>,
        manifest_argument: Option<String>,
        override_args: Vec<String>,
    ) -> Self {
        Self {
            backend_required: false,
            ..Self::cli(
                config_path,
                manifest_argument,
                override_args,
                BackendOverride::default(),
            )
        }
    }

    pub fn api(
        config_path: Option<PathBuf>,
        manifest_argument: Option<String>,
        override_args: Vec<String>,
        backend_override: BackendOverride,
    ) -> Self {
        Self {
            caller: ConfigCaller::Api,
            config_path,
            manifest_argument,
            override_args,
            backend_override,
            lambda_environment: None,
            manifest_required: false,
            backend_required: true,
        }
    }

    pub fn lambda(environment: LambdaEnvironment) -> Self {
        Self {
            caller: ConfigCaller::Lambda,
            config_path: None,
            manifest_argument: None,
            override_args: Vec::new(),
            backend_override: BackendOverride::default(),
            lambda_environment: Some(environment),
            manifest_required: true,
            backend_required: true,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct LambdaEnvironment {
    values: BTreeMap<String, String>,
}

impl LambdaEnvironment {
    pub fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Self {
        let keys = [
            ENV_CONFIG_PATH,
            ENV_MANIFEST_PATH,
            ENV_DUCKDB,
            ENV_DUCKLAKE_SQLITE_CATALOG,
            ENV_DUCKLAKE_AUX_CATALOG,
            ENV_DUCKLAKE_DATA_PATH,
            ENV_CATALOG_BACKEND,
            ENV_S3_BUCKET,
            ENV_S3_PREFIX,
            ENV_OBJECT_REGION,
            ENV_CATALOG_DUCKDB_THREADS,
            ENV_QUERY_MAX_READ_CONNECTIONS,
            ENV_QUERY_MAX_RESPONSE_SIZE_KB,
            ENV_QUERY_LAMBDA_RESPONSE_COMPRESSION,
            ENV_QUERY_LAMBDA_QUERY_ONLY,
            ENV_QUERY_DISABLE_QUERY_DEADLINE,
        ];
        Self {
            values: keys
                .into_iter()
                .filter_map(|key| {
                    lookup(key).map(|value| (key.to_string(), value.trim().to_string()))
                })
                .collect(),
        }
    }

    pub fn from_process() -> Self {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    fn get(&self, key: &str) -> Option<&str> {
        self.values.get(key).map(String::as_str)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestSource {
    ExplicitArgument,
    ConfigFile,
    ApiEmpty,
    Missing,
}

#[derive(Debug, Clone)]
pub struct ResolvedManifest {
    pub source: ManifestSource,
    pub path: Option<String>,
    pub value: AnalyticsManifest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigRoutes {
    pub ingest_enabled: bool,
    pub query_enabled: bool,
}

#[derive(Debug, Clone)]
pub struct ResolvedConfig {
    pub root: RootConfig,
    pub manifest: ResolvedManifest,
    pub storage_backend: Option<StorageBackend>,
    pub routes: ConfigRoutes,
}

impl ResolvedConfig {
    pub fn require_storage_backend(&self) -> Result<&StorageBackend, ConfigError> {
        self.storage_backend
            .as_ref()
            .ok_or_else(|| ConfigError::new(ConfigErrorKind::MissingBackend))
    }
}

pub fn resolve_config(request: ConfigRequest) -> Result<ResolvedConfig, ConfigError> {
    let (manifest_argument, backend_override, root) = load_request_root(&request)?;
    let manifest = resolve_manifest(
        request.caller,
        manifest_argument.as_deref(),
        &root,
        request.manifest_required,
    )?;
    validate_source_config(&root.analytics.source)?;
    validate_retention_config(&root.analytics.retention)?;
    validate_operations_config(&root.analytics.operations)?;
    let storage_backend = request
        .backend_required
        .then(|| {
            if request.caller == ConfigCaller::Lambda {
                resolve_storage_backend_with_root(&backend_override, &root)
            } else {
                resolve_storage_backend(&backend_override, &root)
            }
        })
        .transpose()?;
    let routes = resolve_routes(request.caller, &root, &manifest);
    Ok(ResolvedConfig {
        root,
        manifest,
        storage_backend,
        routes,
    })
}

fn validate_operations_config(
    operations: &crate::AnalyticsOperationsConfig,
) -> Result<(), ConfigError> {
    if operations
        .operation_store_path
        .as_deref()
        .is_some_and(|path| path.trim().is_empty())
    {
        return Err(ConfigError::argument(
            "analytics.operations.operation_store_path must not be empty",
        ));
    }
    Ok(())
}

fn load_request_root(
    request: &ConfigRequest,
) -> Result<(Option<String>, BackendOverride, RootConfig), ConfigError> {
    let Some(environment) = request.lambda_environment.as_ref() else {
        let config = load_with_override_args(
            request.config_path.as_deref(),
            request.override_args.as_slice(),
        )?;
        return Ok((
            request.manifest_argument.clone(),
            request.backend_override.clone(),
            config.root.clone(),
        ));
    };
    let config_path = environment.get(ENV_CONFIG_PATH).map(PathBuf::from);
    let config = load_optional_with_overrides(config_path.as_deref(), &[])?;
    let mut root = config.root.clone();
    apply_lambda_environment(&mut root, environment)?;
    Ok((
        environment.get(ENV_MANIFEST_PATH).map(ToOwned::to_owned),
        lambda_backend_override(environment),
        root,
    ))
}

fn resolve_manifest(
    caller: ConfigCaller,
    manifest_argument: Option<&str>,
    root: &RootConfig,
    manifest_required: bool,
) -> Result<ResolvedManifest, ConfigError> {
    let (source, path) = if let Some(path) = manifest_argument {
        (ManifestSource::ExplicitArgument, Some(path))
    } else if let Some(path) = root.analytics.manifest_path.as_deref() {
        (ManifestSource::ConfigFile, Some(path))
    } else if caller == ConfigCaller::Api && !manifest_required {
        (ManifestSource::ApiEmpty, None)
    } else if !manifest_required {
        (ManifestSource::Missing, None)
    } else {
        return Err(ConfigError::new(ConfigErrorKind::MissingManifestPath));
    };
    let Some(path) = path else {
        return Ok(ResolvedManifest {
            source,
            path: None,
            value: AnalyticsManifest::new(Vec::new()),
        });
    };
    if path.trim().is_empty() {
        return Err(ConfigError::with_debug(
            ConfigErrorKind::InvalidManifestPath,
            ConfigErrorDebug::Message("manifest path must not be empty".to_string()),
        ));
    }
    let value = read_manifest(path).map_err(ConfigError::manifest)?;
    Ok(ResolvedManifest {
        source,
        path: Some(path.to_string()),
        value,
    })
}

fn resolve_routes(
    caller: ConfigCaller,
    root: &RootConfig,
    manifest: &ResolvedManifest,
) -> ConfigRoutes {
    let accepted = !matches!(manifest.source, ManifestSource::Missing);
    let lambda_query_only =
        caller == ConfigCaller::Lambda && root.analytics.query.lambda_query_only;
    ConfigRoutes {
        ingest_enabled: accepted
            && !lambda_query_only
            && root.analytics.http.ingest_endpoint_enabled
            && root.analytics.ingest.processor_enabled,
        query_enabled: accepted,
    }
}

fn lambda_backend_override(environment: &LambdaEnvironment) -> BackendOverride {
    BackendOverride {
        duckdb: environment.get(ENV_DUCKDB).map(ToOwned::to_owned),
        ducklake_sqlite_catalog: environment
            .get(ENV_DUCKLAKE_SQLITE_CATALOG)
            .map(ToOwned::to_owned),
        ducklake_aux_catalog: environment
            .get(ENV_DUCKLAKE_AUX_CATALOG)
            .map(ToOwned::to_owned),
        ducklake_data_path: environment
            .get(ENV_DUCKLAKE_DATA_PATH)
            .map(ToOwned::to_owned),
    }
}

fn non_empty_lambda_env<'a>(
    environment: &'a LambdaEnvironment,
    name: &str,
) -> Result<Option<&'a str>, ConfigError> {
    let Some(value) = environment.get(name) else {
        return Ok(None);
    };
    if value.is_empty() {
        return Err(ConfigError::argument(format!("{name} must not be empty")));
    }
    Ok(Some(value))
}

fn apply_lambda_environment(
    root: &mut RootConfig,
    environment: &LambdaEnvironment,
) -> Result<(), ConfigError> {
    if let Some(backend) = environment.get(ENV_CATALOG_BACKEND) {
        root.analytics.catalog.backend = Some(parse_catalog_backend(backend)?);
    }
    if let Some(data_path) = environment.get(ENV_DUCKLAKE_DATA_PATH) {
        apply_ducklake_data_path(root, data_path);
    }
    if let Some(bucket) = non_empty_lambda_env(environment, ENV_S3_BUCKET)? {
        root.analytics.object_storage.bucket = Some(bucket.to_string());
    }
    if let Some(prefix) = non_empty_lambda_env(environment, ENV_S3_PREFIX)? {
        root.analytics.object_storage.path = Some(prefix.to_string());
    }
    if let Some(region) = non_empty_lambda_env(environment, ENV_OBJECT_REGION)? {
        root.analytics.object_storage.region = Some(region.to_string());
        if root.analytics.object_storage.endpoint_url.is_none() {
            root.analytics.object_storage.endpoint_url = Some(format!("s3.{region}.amazonaws.com"));
        }
    }
    if let Some(threads) = environment.get(ENV_CATALOG_DUCKDB_THREADS) {
        root.analytics.catalog.duckdb_threads =
            Some(parse_usize_env(ENV_CATALOG_DUCKDB_THREADS, threads)?);
    }
    apply_lambda_query_environment(root, environment)?;
    Ok(())
}

fn apply_lambda_query_environment(
    root: &mut RootConfig,
    environment: &LambdaEnvironment,
) -> Result<(), ConfigError> {
    if let Some(max_read_connections) = environment.get(ENV_QUERY_MAX_READ_CONNECTIONS) {
        root.analytics.query.max_read_connections =
            parse_usize_env(ENV_QUERY_MAX_READ_CONNECTIONS, max_read_connections)?;
    }
    if let Some(limit) = environment.get(ENV_QUERY_MAX_RESPONSE_SIZE_KB) {
        root.analytics.query.max_response_size_kb =
            Some(parse_usize_env(ENV_QUERY_MAX_RESPONSE_SIZE_KB, limit)?);
    }
    if let Some(compression) = environment.get(ENV_QUERY_LAMBDA_RESPONSE_COMPRESSION) {
        root.analytics.query.lambda_response_compression =
            parse_lambda_response_compression(compression)?;
    }
    if let Some(query_only) = environment.get(ENV_QUERY_LAMBDA_QUERY_ONLY) {
        root.analytics.query.lambda_query_only =
            parse_bool_env(ENV_QUERY_LAMBDA_QUERY_ONLY, query_only)?;
    }
    if let Some(disable_deadline) = environment.get(ENV_QUERY_DISABLE_QUERY_DEADLINE) {
        root.analytics.query.disable_query_deadline =
            parse_bool_env(ENV_QUERY_DISABLE_QUERY_DEADLINE, disable_deadline)?;
    }
    Ok(())
}

fn parse_catalog_backend(value: &str) -> Result<AnalyticsCatalogBackend, ConfigError> {
    match value.trim() {
        "duckdb" => Ok(AnalyticsCatalogBackend::Duckdb),
        "ducklake_sqlite" => Ok(AnalyticsCatalogBackend::DucklakeSqlite),
        "ducklake_aux_catalog" => Ok(AnalyticsCatalogBackend::DucklakeAuxCatalog),
        "ducklake_motherduck" => Ok(AnalyticsCatalogBackend::DucklakeMotherduck),
        other => Err(ConfigError::argument(format!(
            "{ENV_CATALOG_BACKEND} must be one of duckdb, ducklake_sqlite, ducklake_aux_catalog, \
             or ducklake_motherduck; got {other}"
        ))),
    }
}

fn parse_lambda_response_compression(
    value: &str,
) -> Result<AnalyticsLambdaResponseCompression, ConfigError> {
    match value.trim() {
        "none" => Ok(AnalyticsLambdaResponseCompression::None),
        "gzip_base64" => Ok(AnalyticsLambdaResponseCompression::GzipBase64),
        other => Err(ConfigError::argument(format!(
            "{ENV_QUERY_LAMBDA_RESPONSE_COMPRESSION} must be one of none or gzip_base64; got \
             {other}"
        ))),
    }
}

fn parse_usize_env(name: &str, value: &str) -> Result<usize, ConfigError> {
    value.trim().parse::<usize>().map_err(|source| {
        ConfigError::argument(format!("{name} must be a positive integer: {source}"))
    })
}

fn parse_bool_env(name: &str, value: &str) -> Result<bool, ConfigError> {
    match value.trim() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        other => Err(ConfigError::argument(format!(
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
