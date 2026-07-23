mod batch;
mod checkpoint_lease;
mod entry;
mod health;
mod legacy;
mod legacy_lease;
mod metrics;
mod planning;
mod time;

#[cfg(test)]
pub(crate) use batch::source_batch_integration_outcome;
#[cfg(test)]
pub(crate) use batch::{
    is_iterator_checkpoint, persistable_checkpoints, source_batch_outcome,
    source_batch_outcome_with_ownership_loss,
};
pub(crate) use entry::spawn_source_polling;
#[cfg(test)]
pub(crate) use health::{
    apply_source_job_phase, apply_source_poll_error_health, apply_source_success_health,
    upsert_checkpoint_health,
};
#[cfg(test)]
pub(crate) use planning::{
    SourcePollingStartup, source_plans_are_aux_storage_only, source_polling_startup,
    storage_checkpoints_from_engine,
};
#[cfg(test)]
pub(crate) use time::source_polling_lease_renew_interval;
#[cfg(test)]
pub(crate) use time::{source_polling_lease_until_ms, source_polling_released_until_ms};
