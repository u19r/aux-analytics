mod contracts;
mod executor;
mod raw_backup;
mod verification;
#[cfg(test)]
mod verification_tests;

#[cfg(test)]
pub(crate) use contracts::ProjectionBackedTableFixCurrentSource;
pub use contracts::{
    TableFixApplyOutcome, TableFixConfirmation, TableFixError, TableFixReport, TableFixRequest,
    TableFixResult, TableFixSelection, TableFixSource, TableFixSourceSnapshot, TableFixTruthRecord,
    TableFixValidation, VerifiedTableFixDestination, VersionedTableFixCurrentSource,
};
pub(crate) use executor::TableFixExecutor;
