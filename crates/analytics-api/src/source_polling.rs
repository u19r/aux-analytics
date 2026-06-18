mod batch;
mod batch_metrics;
mod checkpoint_lease;
mod entry;
mod hashed_range;
mod hashed_range_coordination;
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
    source_batch_outcome_with_ownership_loss, source_progress_from_batch,
};
#[cfg(test)]
pub(crate) use batch_metrics::{
    batch_source_bytes, batch_tenant_id, ingest_rate_rollup_from_batch, upsert_table_lag_health,
    versionstamp_lag_ms,
};
pub(crate) use entry::spawn_source_polling;
#[cfg(test)]
pub(crate) use health::{
    apply_source_job_phase, apply_source_poll_error_health, apply_source_poll_timeout_health,
    apply_source_registry_refresh_health, apply_source_success_health, upsert_checkpoint_health,
};
#[cfg(test)]
pub(crate) use planning::{
    SourcePollingStartup, merge_source_table_plans, registered_rows_to_source_table_plans,
    source_plans_are_aux_storage_only, source_poll_plan_refresh_timeout_error,
    source_polling_startup, static_source_table_plans, storage_checkpoints_from_engine,
};
#[cfg(test)]
pub(crate) use time::source_polling_lease_renew_interval;
#[cfg(test)]
pub(crate) use time::{source_polling_lease_until_ms, source_polling_released_until_ms};
