mod constants;
mod error;
mod loader;
mod model;
mod resolved;

pub use error::{ConfigError, ConfigErrorDebug, ConfigErrorKind};
pub use loader::{Config, load_optional_with_overrides};
pub use model::*;
pub use resolved::{
    BackendOverride, CatalogType, StorageBackend, load_with_override_args, parse_override_args,
    resolve_manifest_path, resolve_storage_backend, validate_retention_config,
    validate_source_config,
};

#[cfg(test)]
mod error_tests;
#[cfg(test)]
mod loader_tests;
#[cfg(test)]
mod resolved_tests;
