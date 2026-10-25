use super::{PrivacyFixEffectBoundary, PrivacyFixError, PrivacyFixResult};
use crate::{OperationChunkKey, RawBackupObjectId};

pub(crate) fn run_effect<T, F>(
    effects: &mut impl PrivacyFixEffectBoundary,
    key: OperationChunkKey,
    effect: F,
) -> PrivacyFixResult<T>
where
    F: FnOnce() -> PrivacyFixResult<T>,
{
    effects.before_effect(&key)?;
    match effect() {
        Ok(value) => match effects.after_effect(&key) {
            Ok(()) => Ok(value),
            Err(error) => match effects.abandon_effect(&key) {
                Ok(()) => Err(error),
                Err(recovery_error) => Err(PrivacyFixError::CleanupFailed {
                    original_error: error.to_string(),
                    cleanup_error: recovery_error.to_string(),
                }),
            },
        },
        Err(error) => match effects.abandon_effect(&key) {
            Ok(()) => Err(error),
            Err(recovery_error) => Err(PrivacyFixError::CleanupFailed {
                original_error: error.to_string(),
                cleanup_error: recovery_error.to_string(),
            }),
        },
    }
}

pub(crate) fn cleanup_ordinal(
    progress: &crate::RawBackupCleanupProgress,
    object_id: &RawBackupObjectId,
) -> u64 {
    progress
        .authority
        .as_ref()
        .and_then(|authority| {
            authority
                .object_ids
                .iter()
                .position(|candidate| candidate == object_id)
        })
        .or_else(|| {
            progress
                .pending_object_ids
                .iter()
                .position(|candidate| candidate == object_id)
        })
        .and_then(|position| u64::try_from(position).ok())
        .unwrap_or(u64::MAX)
}
