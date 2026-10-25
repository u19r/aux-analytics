use super::*;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum BackfillPlanError {
    #[error("backfill request must target at least one table")]
    MissingTargetTables,
    #[error("backfill plan rejected for table {table}: stable row identity is required")]
    MissingRowIdentity { table: String },
    #[error(
        "backfill plan rejected for table {table}: stream-only backfill is unsafe beyond stream \
         retention"
    )]
    UnsafeStreamOnlyBackfill { table: String },
    #[error("backfill plan rejected: source cannot enumerate historical rows")]
    HistoricalEnumerationUnsupported,
    #[error("backfill source kind {0} is not supported")]
    UnsupportedSourceKind(String),
    #[error("backfill capability discovery failed: {0}")]
    CapabilityDiscoveryFailed(String),
    #[error("backfill capability discovery found incomplete support for {source_kind:?}: {reason}")]
    PartialCapabilitySupport {
        source_kind: BackfillSourceKind,
        reason: String,
    },
}

pub type BackfillPlanResult<T> = Result<T, BackfillPlanError>;
