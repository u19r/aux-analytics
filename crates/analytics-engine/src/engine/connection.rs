use std::{
    cell::RefCell,
    collections::BTreeMap,
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Weak},
};

use aws_credentials::{AwsResolvedCredentials, resolve_default_chain_credentials_with_expiry};
use config::{CatalogType, DuckLakeCatalogSettings, StorageBackend};
use duckdb::{Config as DuckDbConfig, Connection};

use super::{
    AnalyticsEngine, AnalyticsEngineError, AnalyticsEngineResult, SharedConnectionAnchor,
    repair_contract::RepairReceiptFileGuard,
};
use crate::{
    cache::EngineCaches,
    constants::{REPAIR_RECEIPT_LOCKS, SHARED_CONNECTIONS},
    sql,
};

#[derive(Debug)]
pub(crate) struct DuckLakeObjectStorageCredentials {
    secret: sql::ObjectStorageSecret<'static>,
    resolved: AwsResolvedCredentials,
}

impl AnalyticsEngine {
    pub fn connect(backend: &StorageBackend) -> AnalyticsEngineResult<Self> {
        let object_storage_credentials = resolve_ducklake_object_storage_credentials(backend)?;
        let resolved_credentials = object_storage_credentials
            .as_ref()
            .map(|state| &state.resolved.credentials);

        let (conn, connection_anchor) =
            Self::open_configured_connection(backend, resolved_credentials)?;
        let repair_receipt_lock_identity = repair_receipt_lock_identity(backend)?;
        let repair_receipt_lock = repair_receipt_lock_identity
            .as_ref()
            .map(|identity| shared_repair_receipt_lock(identity.registry_key.clone()))
            .transpose()?;
        let repair_receipt_file_lock = repair_receipt_lock_identity
            .as_ref()
            .map(|identity| open_repair_receipt_file_lock(&identity.sidecar_path))
            .transpose()?;

        Ok(Self {
            conn,
            query_cancellation: RefCell::new(None),
            _connection_anchor: connection_anchor,
            repair_receipt_lock,
            repair_receipt_file_lock,
            destination_fence: super::destination_fence::DestinationFence::from(backend),
            supports_table_layout: matches!(backend, StorageBackend::DuckLake { .. }),
            object_storage_credentials: RefCell::new(object_storage_credentials),
            caches: RefCell::new(EngineCaches::new()),
            tenant_resolver: crate::TenantResolver::default(),
            #[cfg(test)]
            repair_receipt_barrier: None,
            #[cfg(test)]
            retention_effect_barriers: None,
        })
    }

    pub fn connect_duckdb(path: impl AsRef<Path>) -> AnalyticsEngineResult<Self> {
        Self::connect(&StorageBackend::DuckDb {
            path: path.as_ref().to_string_lossy().to_string(),
        })
    }

    #[must_use]
    pub const fn connection(&self) -> &Connection {
        &self.conn
    }

    #[cfg(test)]
    pub(crate) fn with_repair_receipt_barrier(mut self, barrier: Arc<std::sync::Barrier>) -> Self {
        self.repair_receipt_barrier = Some(barrier);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_retention_effect_barriers(
        mut self,
        ready: Arc<std::sync::Barrier>,
        release: Arc<std::sync::Barrier>,
    ) -> Self {
        self.retention_effect_barriers = Some((ready, release));
        self
    }

    pub(super) fn lock_repair_receipt_file(
        &self,
    ) -> AnalyticsEngineResult<Option<RepairReceiptFileGuard<'_>>> {
        let Some(file) = self.repair_receipt_file_lock.as_ref() else {
            return Ok(None);
        };
        file.lock()?;
        Ok(Some(RepairReceiptFileGuard { file }))
    }

    pub(super) fn refresh_object_storage_credentials_if_needed(&self) -> AnalyticsEngineResult<()> {
        let mut state = self.object_storage_credentials.borrow_mut();
        let Some(state) = state.as_mut() else {
            return Ok(());
        };
        if !state.resolved.needs_refresh() {
            return Ok(());
        }

        let resolved = resolve_default_chain_credentials_with_expiry()?;
        state
            .secret
            .create_or_replace_secret(&self.conn, Some(&resolved.credentials))?;
        state.resolved = resolved;
        Ok(())
    }

    fn open_configured_connection(
        backend: &StorageBackend,
        resolved_credentials: Option<&aws_credentials::AwsStaticCredentials>,
    ) -> AnalyticsEngineResult<(Connection, SharedConnectionAnchor)> {
        let initialize = || -> AnalyticsEngineResult<Connection> {
            let conn = match backend {
                StorageBackend::DuckDb { path } => Connection::open(path)?,
                StorageBackend::DuckLake {
                    catalog: CatalogType::MotherDuck,
                    catalog_settings,
                    ..
                } => Connection::open_in_memory_with_flags(motherduck_config(catalog_settings)?)?,
                StorageBackend::DuckLake { .. } => Connection::open_in_memory()?,
            };
            sql::configure_connection(&conn, backend, resolved_credentials)?;
            Ok(conn)
        };
        if let Some(key) = shared_connection_key(backend) {
            return shared_connection(key, initialize);
        }
        let conn = initialize()?;
        let connection_anchor = Arc::new(Mutex::new(conn.try_clone()?));
        Ok((conn, connection_anchor))
    }
}

fn resolve_ducklake_object_storage_credentials(
    backend: &StorageBackend,
) -> AnalyticsEngineResult<Option<DuckLakeObjectStorageCredentials>> {
    let StorageBackend::DuckLake {
        object_storage: Some(object_storage),
        ..
    } = backend
    else {
        return Ok(None);
    };
    let Some(secret) = sql::ObjectStorageSecret::from_config(object_storage) else {
        return Ok(None);
    };
    if object_storage
        .credentials
        .as_ref()
        .is_some_and(|credentials| {
            credentials.r#static.is_some() || !credentials.instance_keys.unwrap_or(false)
        })
    {
        return Ok(None);
    }
    Ok(Some(DuckLakeObjectStorageCredentials {
        secret: secret.into_owned(),
        resolved: resolve_default_chain_credentials_with_expiry()?,
    }))
}

fn shared_connection_key(backend: &StorageBackend) -> Option<String> {
    // Separate `Connection::open` handles can both commit stale fence updates
    // for the same file. Cloned handles share DuckDB's conflict detector.
    match backend {
        StorageBackend::DuckDb { path } if path != ":memory:" => Some(format!("duckdb:{path}")),
        _ => None,
    }
}

fn shared_connection(
    key: String,
    initialize: impl FnOnce() -> AnalyticsEngineResult<Connection>,
) -> AnalyticsEngineResult<(Connection, SharedConnectionAnchor)> {
    let registry = SHARED_CONNECTIONS.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut registry = registry.lock().map_err(|_| {
        AnalyticsEngineError::Io(std::io::Error::other(
            "shared DuckDB connection registry is poisoned",
        ))
    })?;
    if let Some(anchor) = registry.get(&key).and_then(Weak::upgrade) {
        let connection = anchor
            .lock()
            .map_err(|_| {
                AnalyticsEngineError::Io(std::io::Error::other(
                    "shared DuckDB connection anchor is poisoned",
                ))
            })?
            .try_clone()?;
        return Ok((connection, anchor));
    }

    let anchor = Arc::new(Mutex::new(initialize()?));
    let connection = anchor
        .lock()
        .map_err(|_| {
            AnalyticsEngineError::Io(std::io::Error::other(
                "shared DuckDB connection anchor is poisoned",
            ))
        })?
        .try_clone()?;
    registry.insert(key, Arc::downgrade(&anchor));
    Ok((connection, anchor))
}

fn shared_repair_receipt_lock(key: String) -> AnalyticsEngineResult<Arc<Mutex<()>>> {
    let registry = REPAIR_RECEIPT_LOCKS.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut registry = registry.lock().map_err(|_| {
        AnalyticsEngineError::Io(std::io::Error::other(
            "repair receipt lock registry is poisoned",
        ))
    })?;
    if let Some(lock) = registry.get(&key).and_then(Weak::upgrade) {
        return Ok(lock);
    }
    let lock = Arc::new(Mutex::new(()));
    registry.insert(key, Arc::downgrade(&lock));
    Ok(lock)
}

struct RepairReceiptLockIdentity {
    registry_key: String,
    sidecar_path: PathBuf,
}

fn canonical_repair_receipt_catalog_path(path: &str) -> AnalyticsEngineResult<PathBuf> {
    let candidate = Path::new(path);
    if candidate.file_name().is_none() {
        return Err(AnalyticsEngineError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "repair receipt catalog path must name a catalog file",
        )));
    }
    let canonical_path = std::fs::canonicalize(candidate)?;
    if !std::fs::metadata(&canonical_path)?.is_file() {
        return Err(AnalyticsEngineError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "repair receipt catalog path must resolve to a regular file",
        )));
    }
    Ok(canonical_path)
}

fn repair_receipt_lock_identity(
    backend: &StorageBackend,
) -> AnalyticsEngineResult<Option<RepairReceiptLockIdentity>> {
    let (prefix, catalog_path) = match backend {
        StorageBackend::DuckDb { path } if path != ":memory:" => ("duckdb", path),
        StorageBackend::DuckLake {
            catalog: CatalogType::Sqlite,
            catalog_path,
            ..
        } if catalog_path != ":memory:" => ("ducklake:sqlite", catalog_path),
        _ => return Ok(None),
    };
    let canonical_path = canonical_repair_receipt_catalog_path(catalog_path)?;
    let canonical_path_string = canonical_path.to_string_lossy();
    Ok(Some(RepairReceiptLockIdentity {
        registry_key: format!("{prefix}:{canonical_path_string}"),
        sidecar_path: PathBuf::from(format!("{canonical_path_string}.repair-receipt.lock")),
    }))
}

fn open_repair_receipt_file_lock(path: &Path) -> AnalyticsEngineResult<File> {
    Ok(OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?)
}

fn motherduck_config(settings: &DuckLakeCatalogSettings) -> AnalyticsEngineResult<DuckDbConfig> {
    let Some(token) = settings.motherduck_token.as_deref() else {
        return Ok(DuckDbConfig::default());
    };
    Ok(DuckDbConfig::default().with("motherduck_token", token)?)
}
