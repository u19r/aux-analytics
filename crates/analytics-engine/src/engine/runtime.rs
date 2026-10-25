use std::{
    cell::RefCell,
    fs::File,
    sync::{Arc, Mutex},
};

use duckdb::Connection;

use super::connection;
use crate::cache::EngineCaches;

pub(crate) type SharedConnectionAnchor = Arc<Mutex<Connection>>;

pub struct AnalyticsEngine {
    pub(super) conn: Connection,
    pub(super) query_cancellation: RefCell<Option<super::query_execution::QueryCancellation>>,
    // DuckDB scopes optimistic write-conflict detection to a database instance.
    // Keep the shared instance alive while cloned engine connections exist.
    pub(super) _connection_anchor: Arc<Mutex<Connection>>,
    // Local file-backed catalogs can use a process-local mutex plus an OS
    // sidecar lock. The latter extends the receipt fence across processes.
    pub(super) repair_receipt_lock: Option<Arc<Mutex<()>>>,
    pub(super) repair_receipt_file_lock: Option<File>,
    pub(super) destination_fence: super::destination_fence::DestinationFence,
    pub(super) supports_table_layout: bool,
    pub(super) object_storage_credentials:
        RefCell<Option<connection::DuckLakeObjectStorageCredentials>>,
    pub(crate) caches: RefCell<EngineCaches>,
    pub(crate) tenant_resolver: crate::TenantResolver,
    #[cfg(test)]
    pub(super) repair_receipt_barrier: Option<Arc<std::sync::Barrier>>,
    #[cfg(test)]
    pub(super) retention_effect_barriers:
        Option<(Arc<std::sync::Barrier>, Arc<std::sync::Barrier>)>,
}
