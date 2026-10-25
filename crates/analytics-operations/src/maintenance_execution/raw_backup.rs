use crate::{
    MaintenanceError, MaintenanceResult, OperationActor, RateLimitPolicy, RawBackupError,
    RawBackupObjectStore, RawBackupWriteRequest,
    maintenance::{MaintenanceExecutionReport, MaintenanceOperations},
};

impl<'a> MaintenanceOperations<'a> {
    pub fn execute_raw_backup_write<Store>(
        &self,
        request: &RawBackupWriteRequest,
        actor: &OperationActor,
        rate_limit: RateLimitPolicy,
        raw_backup_store: &Store,
    ) -> MaintenanceResult<MaintenanceExecutionReport>
    where
        Store: RawBackupObjectStore,
    {
        request.validate()?;
        let _ = (actor, rate_limit);
        Err(MaintenanceError::RawBackup(
            RawBackupError::DestinationFenceUnavailable {
                destination: raw_backup_store.destination_kind(),
            },
        ))
    }
}
