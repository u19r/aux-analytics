mod configuration;
mod constants;
mod error;
mod loader;
mod resolved;
mod runtime;

pub use configuration::*;
pub use constants::{DEFAULT_QUERY_MAX_READ_CONNECTIONS, MAX_RETENTION_DELETE_BATCH_SIZE};
pub use error::{ConfigError, ConfigErrorDebug, ConfigErrorKind};
pub use loader::{Config, load_optional_with_overrides};
pub use resolved::{
    BackendOverride, CatalogType, DuckLakeCatalogSettings, StorageBackend, load_with_override_args,
    parse_override_args, resolve_storage_backend, validate_retention_config,
    validate_source_config,
};
pub use runtime::{
    ConfigCaller, ConfigRequest, ConfigRoutes, LambdaEnvironment, ManifestSource, ResolvedConfig,
    ResolvedManifest, resolve_config,
};

#[cfg(test)]
mod error_tests;
#[cfg(test)]
mod loader_tests;
#[cfg(test)]
mod quint_config_tests;
#[cfg(test)]
mod resolved_tests;
#[cfg(test)]
mod runtime_tests;
