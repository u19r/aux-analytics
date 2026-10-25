use super::{
    contracts::{SourceEffectAuthority, SourceEffectScope},
    persistence::{StoredAuthority, StoredReceipt, StoredReceiptScope},
};
use crate::{
    constants::CONTENT_DIGEST_BYTES,
    engine::{AnalyticsEngineError, AnalyticsEngineResult},
};

impl StoredReceiptScope {
    fn matches(&self, scope: &SourceEffectScope) -> bool {
        match (self, scope) {
            (
                Self::Table {
                    source_table_name,
                    shard_id,
                },
                SourceEffectScope::Table {
                    source_table_name: requested_table,
                    shard_id: requested_shard,
                    ..
                },
            ) => source_table_name == requested_table && shard_id == requested_shard,
            (
                Self::System {
                    source_scope,
                    checkpoint,
                },
                SourceEffectScope::System {
                    source_scope: requested_scope,
                    checkpoint: requested_checkpoint,
                    ..
                },
            ) => source_scope == requested_scope && checkpoint == requested_checkpoint,
            _ => false,
        }
    }
}

impl StoredReceipt {
    pub(crate) fn validate_replay(
        &self,
        scope: &SourceEffectScope,
        authority: &SourceEffectAuthority,
        content_digest: &[u8; CONTENT_DIGEST_BYTES],
        accepted: Option<&StoredAuthority>,
    ) -> AnalyticsEngineResult<()> {
        if !self.scope.matches(scope) {
            return Err(AnalyticsEngineError::ConflictingSourceEffectReceipt);
        }
        if self.fence != authority.fence() {
            return Err(AnalyticsEngineError::ConflictingSourceEffectReceipt);
        }
        let Some(accepted_authority) = accepted else {
            return Err(AnalyticsEngineError::IncompleteSourceEffectState);
        };
        if self.content_digest != Some(*content_digest) {
            return Err(AnalyticsEngineError::ConflictingSourceEffectReceipt);
        }
        if self.fence > accepted_authority.fence {
            return Err(AnalyticsEngineError::IncompleteSourceEffectState);
        }
        if self.fence == accepted_authority.fence
            && accepted_authority.execution_id != *authority.execution_id()
        {
            return Err(AnalyticsEngineError::ConflictingSourceEffectReceipt);
        }
        Ok(())
    }
}
