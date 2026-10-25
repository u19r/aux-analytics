use super::{ExecutionEffectLease, ExecutionEffects};
use crate::{
    MaintenanceExecutionAuthority, OperationChunkKey, PrivacyFixError, TableFixError,
    TrimDestination, TrimError, VerifiedTableFixDestination, privacy_fix::PrivacyFixEffectBoundary,
    table_fix::TableFixApplyOutcome,
};

pub(super) struct GuardedTableFixDestination<'effects, 'operation, Destination> {
    pub(super) destination: &'effects mut Destination,
    pub(super) effects: &'effects ExecutionEffects<'operation>,
    pub(super) authority: MaintenanceExecutionAuthority,
}

impl<'effects, 'operation, Destination> VerifiedTableFixDestination
    for GuardedTableFixDestination<'effects, 'operation, Destination>
where Destination: VerifiedTableFixDestination
{
    fn apply_repair_with_key(
        &mut self,
        authority: &MaintenanceExecutionAuthority,
        key: &OperationChunkKey,
        table_name: &str,
        record_key: &[u8],
        record: analytics_contract::StorageStreamRecord,
        expected_source_revision: &str,
    ) -> Result<TableFixApplyOutcome, TableFixError> {
        if authority != &self.authority {
            return Err(TableFixError::Destination(
                "maintenance execution authority does not match the attempt".to_string(),
            ));
        }
        let lease = self
            .effects
            .begin_effect(key)
            .map_err(TableFixError::OperationStore)?;
        let result = self.destination.apply_repair_with_key(
            authority,
            key,
            table_name,
            record_key,
            record,
            expected_source_revision,
        );
        match result {
            Err(error) => match self.effects.abandon_effect(&lease, key) {
                Ok(()) => Err(error),
                Err(recovery_error) => Err(TableFixError::OperationStore(recovery_error)),
            },
            Ok(outcome) => match self.effects.complete_effect(&lease, key) {
                Ok(()) => Ok(outcome),
                Err(error) => match self.effects.abandon_effect(&lease, key) {
                    Ok(()) => Err(TableFixError::OperationStore(error)),
                    Err(recovery_error) => Err(TableFixError::OperationStore(recovery_error)),
                },
            },
        }
    }

    fn verify_repair(
        &self,
        authority: &MaintenanceExecutionAuthority,
        key: &OperationChunkKey,
        table_name: &str,
        record_key: &[u8],
        expected: &analytics_contract::StorageStreamRecord,
        expected_source_revision: &str,
    ) -> Result<bool, TableFixError> {
        if authority != &self.authority {
            return Err(TableFixError::Destination(
                "maintenance execution authority does not match the attempt".to_string(),
            ));
        }
        self.destination.verify_repair(
            authority,
            key,
            table_name,
            record_key,
            expected,
            expected_source_revision,
        )
    }
}

pub(super) struct GuardedTrimDestination<'effects, 'operation, Destination> {
    pub(super) destination: &'effects mut Destination,
    pub(super) effects: &'effects ExecutionEffects<'operation>,
    pub(super) authority: MaintenanceExecutionAuthority,
}

impl<'effects, 'operation, Destination> TrimDestination
    for GuardedTrimDestination<'effects, 'operation, Destination>
where Destination: TrimDestination
{
    fn delete_candidate_snapshot_with_key(
        &mut self,
        authority: &MaintenanceExecutionAuthority,
        key: &OperationChunkKey,
        table_name: &str,
        target: &crate::TrimTarget,
        expected: &crate::TrimCandidateSnapshot,
    ) -> Result<crate::TrimDeletionOutcome, TrimError> {
        if authority != &self.authority {
            return Err(TrimError::DestinationAuthorityMismatch);
        }
        let lease = self
            .effects
            .begin_effect(key)
            .map_err(TrimError::OperationStore)?;
        let result = self
            .destination
            .delete_candidate_snapshot_with_key(authority, key, table_name, target, expected);
        match result {
            Err(error) => match self.effects.abandon_effect(&lease, key) {
                Ok(()) => Err(error),
                Err(recovery_error) => Err(TrimError::OperationStore(recovery_error)),
            },
            Ok(outcome) => match self.effects.complete_effect(&lease, key) {
                Ok(()) => Ok(outcome),
                Err(error) => match self.effects.abandon_effect(&lease, key) {
                    Ok(()) => Err(TrimError::OperationStore(error)),
                    Err(recovery_error) => Err(TrimError::OperationStore(recovery_error)),
                },
            },
        }
    }
}

pub(super) struct GuardedPrivacyFixEffects<'effects, 'operation> {
    pub(super) effects: &'effects ExecutionEffects<'operation>,
    pub(super) leases: std::collections::HashMap<OperationChunkKey, ExecutionEffectLease>,
}

impl<'effects, 'operation> GuardedPrivacyFixEffects<'effects, 'operation> {
    pub(super) fn new(effects: &'effects ExecutionEffects<'operation>) -> Self {
        Self {
            effects,
            leases: std::collections::HashMap::new(),
        }
    }
}

impl<'effects, 'operation> PrivacyFixEffectBoundary
    for GuardedPrivacyFixEffects<'effects, 'operation>
{
    fn before_effect(&mut self, key: &OperationChunkKey) -> Result<(), PrivacyFixError> {
        let lease = self
            .effects
            .begin_effect(key)
            .map_err(PrivacyFixError::OperationStore)?;
        self.leases.insert(key.clone(), lease);
        Ok(())
    }

    fn after_effect(&mut self, key: &OperationChunkKey) -> Result<(), PrivacyFixError> {
        let lease = self
            .leases
            .get(key)
            .ok_or_else(|| self.effects.missing_effect_lease(key))?;
        match self.effects.complete_effect(lease, key) {
            Ok(()) => {
                self.leases.remove(key);
                Ok(())
            }
            Err(error) => Err(PrivacyFixError::OperationStore(error)),
        }
    }

    fn abandon_effect(&mut self, key: &OperationChunkKey) -> Result<(), PrivacyFixError> {
        let lease = self
            .leases
            .get(key)
            .ok_or_else(|| self.effects.missing_effect_lease(key))?;
        match self.effects.abandon_effect(lease, key) {
            Ok(()) => {
                self.leases.remove(key);
                Ok(())
            }
            Err(error) => Err(PrivacyFixError::OperationStore(error)),
        }
    }
}
