use config::{CatalogType, StorageBackend};
use duckdb::Connection;

use super::{
    AnalyticsEngine, AnalyticsEngineError, AnalyticsEngineResult, transaction::EngineTransaction,
};

/// Backend policy is shared by effect entrypoints and background job
/// composition.
#[derive(Clone, Copy)]
pub(super) enum DestinationFence {
    Unavailable,
    Local { repair_receipts: bool },
    CatalogSnapshot,
}

impl From<&StorageBackend> for DestinationFence {
    fn from(backend: &StorageBackend) -> Self {
        match backend {
            StorageBackend::DuckDb { .. } => Self::Local {
                repair_receipts: true,
            },
            StorageBackend::DuckLake {
                catalog: CatalogType::AuxCatalog,
                ..
            } => Self::CatalogSnapshot,
            StorageBackend::DuckLake {
                catalog: CatalogType::Sqlite,
                catalog_path,
                data_path,
                object_storage,
                ..
            } if !object_storage
                .as_ref()
                .is_some_and(|storage| storage.bucket.is_some())
                && !data_path
                    .split_once("://")
                    .is_some_and(|(scheme, _)| !scheme.eq_ignore_ascii_case("file")) =>
            {
                Self::Local {
                    repair_receipts: catalog_path != ":memory:",
                }
            }
            StorageBackend::DuckLake { .. } => Self::Unavailable,
        }
    }
}

impl DestinationFence {
    pub(super) fn supports_sources(self) -> bool {
        !matches!(self, Self::Unavailable)
    }

    pub(super) fn supports_repairs(self) -> bool {
        matches!(
            self,
            Self::Local {
                repair_receipts: true
            } | Self::CatalogSnapshot
        )
    }

    pub(super) fn enable(self, connection: &Connection) -> AnalyticsEngineResult<()> {
        match self {
            Self::Local { .. } => Ok(()),
            Self::CatalogSnapshot => {
                connection.execute_batch("CALL ducklake_set_transaction_fence('dlake')")?;
                Ok(())
            }
            Self::Unavailable => Err(AnalyticsEngineError::SourceEffectFenceUnavailable),
        }
    }
}

impl AnalyticsEngine {
    /// Checks the actual extension and runtime before workers register any
    /// jobs. Query-only processes can continue using an older catalog
    /// runtime.
    pub fn verify_destination_fence(&self) -> AnalyticsEngineResult<()> {
        EngineTransaction::begin_fenced(self)?.rollback()
    }

    /// Reports the supported backend design. Worker startup also verifies the
    /// installed runtime.
    pub fn supports_fenced_backend(backend: &StorageBackend) -> bool {
        DestinationFence::from(backend).supports_sources()
    }
}
