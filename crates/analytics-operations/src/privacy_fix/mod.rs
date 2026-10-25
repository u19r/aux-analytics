mod authority;
mod cleanup;
mod contracts;
mod effects;
mod executor;
mod planning;
mod resume;
mod rewrite;
mod verification;

pub(crate) use authority::{
    PrivacyBackupRoots, StagedPrivacyOutput, cleanup_authority, destructive_action_name,
    push_unique,
};
pub(crate) use cleanup::PendingCleanupExecution;
#[cfg(test)]
pub(crate) use contracts::NoopPrivacyFixEffectBoundary;
pub(crate) use contracts::{
    IndexedPrivacyRawBackupIndexes, LinearPrivacyAffectedTables, PendingPrivacyCleanup,
    PrivacyAffectedTables, PrivacyFixEffectBoundary, PrivacyFixExecutor,
    PrivacyRawBackupIndexLookup, ResolvedPrivacyRawBackupIndex, same_cleanup_scope,
    validate_cleanup_partition,
};
pub use contracts::{
    PrivacyFixDestructiveAction, PrivacyFixError, PrivacyFixReport, PrivacyFixRequest,
    PrivacyFixResult,
};
pub(crate) use effects::{cleanup_ordinal, run_effect};
pub(crate) use executor::{PrivacyRewriteExecution, PrivacyRewriteInput};
pub(crate) use verification::{
    clear_cleanup_progress_with_effects, save_cleanup_progress_with_effects, verify_clean_output,
    verify_quarantine_output, verify_source_index_for_cleanup, verify_source_object_for_cleanup,
};
