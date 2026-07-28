use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use analytics_contract::{
    AnalyticsManifest, RetentionTimestamp, StorageItem, StorageStreamRecord, StorageValue,
    TenantSelector,
};
use analytics_engine::IngestRetention;
use analytics_storage::{AuxStorageLeaseClient, AuxStorageLeaseOutcome, RetentionPolicyLookup};
use config::{
    AnalyticsRetentionConfig, AnalyticsRetentionTableConfig, AnalyticsSourceConfig,
    RetentionTimestampConfig,
};
use tokio::sync::RwLock;

use crate::{
    source_polling::{job_lease_client, job_lease_token, job_worker_id},
    types::{AppState, RetentionHealth, RetentionHealthStatus},
};

const RETENTION_JOB_ID: &str = "analytics_retention_sweep";
const RETENTION_LEASE_DURATION_MS: i64 = 60_000;
const RETENTION_LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(20);
const RETENTION_LEASE_ATTEMPTS_TOTAL_METRIC: &str = "analytics.retention.lease_attempts_total";
const RETENTION_LOOKUPS_TOTAL_METRIC: &str = "analytics.retention.lookups_total";
const RETENTION_LOOKUP_LATENCY_METRIC: &str = "analytics.retention.lookup_latency_ms";
const RETENTION_LOOKUP_FAILURES_TOTAL_METRIC: &str = "analytics.retention.lookup_failures_total";
const RETENTION_MISSING_ROWS_METRIC: &str = "analytics.retention.missing_rows";
const RETENTION_SWEEPS_TOTAL_METRIC: &str = "analytics.retention.sweeps_total";
const RETENTION_SWEEP_DURATION_METRIC: &str = "analytics.retention.sweep_duration_ms";
const RETENTION_DELETE_BATCHES_TOTAL_METRIC: &str = "analytics.retention.delete_batches_total";
const RETENTION_DELETE_BATCH_DURATION_METRIC: &str = "analytics.retention.delete_batch_duration_ms";
const RETENTION_ROWS_DELETED_TOTAL_METRIC: &str = "analytics.retention.rows_deleted_total";

#[derive(Debug)]
pub struct RetentionRuntime {
    config: AnalyticsRetentionConfig,
    tables: HashMap<String, RetentionTableRuntime>,
    cache: RwLock<HashMap<RetentionCacheKey, RetentionCacheEntry>>,
}

#[derive(Debug)]
struct RetentionTableRuntime {
    config: AnalyticsRetentionTableConfig,
    lookup: RetentionPolicyLookup,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RetentionCacheKey {
    analytics_table_name: String,
    tenant_id: String,
}

#[derive(Debug, Clone)]
struct RetentionCacheEntry {
    period_ms: u64,
    expires_at_ms: u128,
}

impl RetentionRuntime {
    pub async fn from_config(
        config: &AnalyticsRetentionConfig,
    ) -> Result<Option<Self>, analytics_storage::AnalyticsStorageError> {
        if !config.enabled || config.tables.is_empty() {
            return Ok(None);
        }
        let mut tables = HashMap::new();
        for table in &config.tables {
            let lookup = RetentionPolicyLookup::from_config(&table.tenant_policy).await?;
            tables.insert(
                table.analytics_table_name.clone(),
                RetentionTableRuntime {
                    config: table.clone(),
                    lookup,
                },
            );
        }
        Ok(Some(Self {
            config: config.clone(),
            tables,
            cache: RwLock::new(HashMap::new()),
        }))
    }

    pub fn table_names(&self) -> impl Iterator<Item = &str> {
        self.tables.keys().map(String::as_str)
    }

    pub async fn retention_for_record(
        &self,
        manifest: &AnalyticsManifest,
        analytics_table_name: &str,
        record: &StorageStreamRecord,
    ) -> Option<IngestRetention> {
        let table = self.tables.get(analytics_table_name)?;
        let item = record.new_image.as_ref()?;
        let tenant_id = resolve_tenant_id(manifest, analytics_table_name, item).unwrap_or_default();
        if tenant_id.is_empty() {
            return Some(Self::lookup_failed_retention(table));
        }
        let started = Instant::now();
        match self
            .lookup_period_ms(table, analytics_table_name, tenant_id.as_str())
            .await
        {
            Ok(period_ms) => {
                record_lookup_metrics(
                    analytics_table_name,
                    policy_source(table),
                    "success",
                    started,
                );
                Some(IngestRetention {
                    period_ms: Some(period_ms),
                    timestamp: timestamp_from_config(&table.config.timestamp),
                    missing_retention: false,
                })
            }
            Err(error) => {
                record_lookup_metrics(analytics_table_name, policy_source(table), "error", started);
                metrics::counter!(
                    RETENTION_LOOKUP_FAILURES_TOTAL_METRIC,
                    "table" => analytics_table_name.to_string(),
                    "source" => policy_source(table),
                    "tenant_id" => tenant_id.clone()
                )
                .increment(1);
                tracing::warn!(
                    analytics_table_name,
                    tenant_id,
                    error = %error,
                    "analytics retention policy lookup failed"
                );
                Some(Self::lookup_failed_retention(table))
            }
        }
    }

    async fn lookup_period_ms(
        &self,
        table: &RetentionTableRuntime,
        analytics_table_name: &str,
        tenant_id: &str,
    ) -> Result<u64, analytics_storage::AnalyticsStorageError> {
        let key = RetentionCacheKey {
            analytics_table_name: analytics_table_name.to_string(),
            tenant_id: tenant_id.to_string(),
        };
        let now = now_ms();
        if let Some(entry) = self.cache.read().await.get(&key)
            && entry.expires_at_ms > now
        {
            return Ok(entry.period_ms);
        }
        let period_ms = table.lookup.lookup_period_ms(tenant_id).await?;
        self.cache.write().await.insert(
            key,
            RetentionCacheEntry {
                period_ms,
                expires_at_ms: now
                    .saturating_add(u128::from(table.config.tenant_policy.cache_ttl_ms)),
            },
        );
        Ok(period_ms)
    }

    fn lookup_failed_retention(table: &RetentionTableRuntime) -> IngestRetention {
        if table.config.strict {
            IngestRetention {
                period_ms: None,
                timestamp: timestamp_from_config(&table.config.timestamp),
                missing_retention: true,
            }
        } else {
            IngestRetention {
                period_ms: Some(table.config.default_period_ms),
                timestamp: timestamp_from_config(&table.config.timestamp),
                missing_retention: false,
            }
        }
    }
}

pub async fn spawn_retention_sweeper(
    retention: Option<Arc<RetentionRuntime>>,
    app_state: Arc<AppState>,
    source: &AnalyticsSourceConfig,
) -> crate::error::ApiResult<()> {
    let Some(retention) = retention else {
        *app_state.retention_health.write().await = RetentionHealth::disabled();
        return Ok(());
    };
    let lease_client = job_lease_client(source)?.map(Arc::new);
    *app_state.retention_health.write().await = RetentionHealth::starting(retention.tables.len());
    tokio::spawn(run_retention_sweeper(retention, app_state, lease_client));
    Ok(())
}

async fn run_retention_sweeper(
    retention: Arc<RetentionRuntime>,
    app_state: Arc<AppState>,
    lease_client: Option<Arc<AuxStorageLeaseClient>>,
) {
    let mut interval =
        tokio::time::interval(Duration::from_millis(retention.config.sweep_interval_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let worker_id = format!("{}-retention", job_worker_id());
    let mut lease_table_ready = lease_client.is_none();
    loop {
        interval.tick().await;
        let Some(lease_client) = lease_client.as_ref() else {
            run_retention_sweep_cycle(&retention, &app_state).await;
            continue;
        };
        if !lease_table_ready {
            match lease_client.ensure_job_lease_table().await {
                Ok(()) => lease_table_ready = true,
                Err(error) => {
                    record_retention_lease_error(&app_state).await;
                    tracing::warn!(
                        error = %error,
                        job_id = RETENTION_JOB_ID,
                        worker_id,
                        "analytics retention lease table ensure failed"
                    );
                    continue;
                }
            }
        }
        let now_ms = i64::try_from(now_ms()).unwrap_or(i64::MAX);
        let lease_until_ms = now_ms.saturating_add(RETENTION_LEASE_DURATION_MS);
        let lease_token = job_lease_token(worker_id.as_str());
        match lease_client
            .try_acquire_job_lease(
                RETENTION_JOB_ID,
                worker_id.as_str(),
                lease_token.as_str(),
                now_ms,
                lease_until_ms,
            )
            .await
        {
            Ok(AuxStorageLeaseOutcome::Acquired) => {
                metrics::counter!(
                    RETENTION_LEASE_ATTEMPTS_TOTAL_METRIC,
                    "outcome" => "acquired"
                )
                .increment(1);
                run_retention_sweep_with_lease(
                    &retention,
                    &app_state,
                    lease_client,
                    worker_id.as_str(),
                    lease_token.as_str(),
                )
                .await;
            }
            Ok(AuxStorageLeaseOutcome::HeldByAnotherWorker) => {
                metrics::counter!(
                    RETENTION_LEASE_ATTEMPTS_TOTAL_METRIC,
                    "outcome" => "standby"
                )
                .increment(1);
                app_state.retention_health.write().await.status = RetentionHealthStatus::Healthy;
            }
            Err(error) => {
                metrics::counter!(
                    RETENTION_LEASE_ATTEMPTS_TOTAL_METRIC,
                    "outcome" => "error"
                )
                .increment(1);
                record_retention_lease_error(&app_state).await;
                tracing::warn!(
                    error = %error,
                    job_id = RETENTION_JOB_ID,
                    worker_id,
                    "analytics retention lease acquisition failed"
                );
            }
        }
    }
}

async fn run_retention_sweep_with_lease(
    retention: &RetentionRuntime,
    app_state: &Arc<AppState>,
    lease_client: &AuxStorageLeaseClient,
    worker_id: &str,
    lease_token: &str,
) {
    let sweep = run_retention_sweep_cycle(retention, app_state);
    tokio::pin!(sweep);
    loop {
        tokio::select! {
            () = &mut sweep => return,
            () = tokio::time::sleep(RETENTION_LEASE_RENEW_INTERVAL) => {
                let now_ms = i64::try_from(now_ms()).unwrap_or(i64::MAX);
                let lease_until_ms = now_ms.saturating_add(RETENTION_LEASE_DURATION_MS);
                match lease_client
                    .renew_job_lease(
                        RETENTION_JOB_ID,
                        worker_id,
                        lease_token,
                        lease_until_ms,
                    )
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        record_retention_lease_error(app_state).await;
                        tracing::warn!(
                            job_id = RETENTION_JOB_ID,
                            worker_id,
                            "analytics retention sweep stopped after losing its lease"
                        );
                        return;
                    }
                    Err(error) => {
                        record_retention_lease_error(app_state).await;
                        tracing::warn!(
                            error = %error,
                            job_id = RETENTION_JOB_ID,
                            worker_id,
                            "analytics retention lease renewal failed"
                        );
                        return;
                    }
                }
            }
        }
    }
}

async fn run_retention_sweep_cycle(retention: &RetentionRuntime, app_state: &Arc<AppState>) {
    for table_name in retention.table_names() {
        sweep_table(retention, app_state, table_name).await;
    }
}

async fn record_retention_lease_error(app_state: &Arc<AppState>) {
    let mut health = app_state.retention_health.write().await;
    health.status = RetentionHealthStatus::Degraded;
    health.total_sweep_errors = health.total_sweep_errors.saturating_add(1);
}

async fn sweep_table(retention: &RetentionRuntime, app_state: &Arc<AppState>, table_name: &str) {
    let started = Instant::now();
    let mut deleted_total = 0_u64;
    let mut outcome = "success";
    loop {
        let batch_started = Instant::now();
        let engine_table_name = table_name.to_owned();
        let delete_batch_size = retention.config.delete_batch_size;
        let deleted = {
            match app_state
                .engine
                .with_write(move |engine| {
                    engine.delete_expired_rows(
                        engine_table_name.as_str(),
                        i64::try_from(now_ms()).unwrap_or(i64::MAX),
                        delete_batch_size,
                    )
                })
                .await
            {
                Ok(deleted) => deleted,
                Err(error) => {
                    outcome = "error";
                    tracing::warn!(analytics_table_name = table_name, error = %error, "analytics retention sweep failed");
                    0
                }
            }
        };
        metrics::counter!(RETENTION_DELETE_BATCHES_TOTAL_METRIC, "table" => table_name.to_string(), "outcome" => outcome).increment(1);
        metrics::histogram!(RETENTION_DELETE_BATCH_DURATION_METRIC, "table" => table_name.to_string(), "outcome" => outcome).record(duration_ms(batch_started));
        if outcome == "error" || deleted == 0 {
            break;
        }
        deleted_total = deleted_total.saturating_add(deleted);
        metrics::counter!(RETENTION_ROWS_DELETED_TOTAL_METRIC, "table" => table_name.to_string())
            .increment(deleted);
        tokio::time::sleep(Duration::from_millis(
            retention.config.delete_batch_pause_ms,
        ))
        .await;
    }
    let engine_table_name = table_name.to_owned();
    let missing = app_state
        .engine
        .with_read(move |engine| engine.missing_retention_count(engine_table_name.as_str()))
        .await
        .ok()
        .and_then(Result::ok)
        .unwrap_or(0);
    metrics::gauge!(RETENTION_MISSING_ROWS_METRIC, "table" => table_name.to_string())
        .set(u64_to_f64(missing));
    metrics::counter!(RETENTION_SWEEPS_TOTAL_METRIC, "table" => table_name.to_string(), "outcome" => outcome).increment(1);
    metrics::histogram!(RETENTION_SWEEP_DURATION_METRIC, "table" => table_name.to_string(), "outcome" => outcome).record(duration_ms(started));
    {
        let mut health = app_state.retention_health.write().await;
        health.status = if outcome == "success" {
            RetentionHealthStatus::Healthy
        } else {
            RetentionHealthStatus::Degraded
        };
        health.last_sweep_completed_at_ms = Some(now_ms());
        health.last_sweep_duration_ms = Some(duration_ms(started));
        health.total_rows_deleted = health.total_rows_deleted.saturating_add(deleted_total);
        if outcome == "error" {
            health.total_sweep_errors = health.total_sweep_errors.saturating_add(1);
        }
    }
}

fn resolve_tenant_id(
    manifest: &AnalyticsManifest,
    analytics_table_name: &str,
    item: &StorageItem,
) -> Option<String> {
    let table = manifest
        .tables
        .iter()
        .find(|table| table.analytics_table_name == analytics_table_name)?;
    if let Some(tenant_id) = table.tenant_id.as_ref() {
        return Some(tenant_id.clone());
    }
    match &table.tenant_selector {
        TenantSelector::TableName => Some(table.source_table_name.clone()),
        TenantSelector::Attribute { attribute_name } => {
            string_attribute(item, attribute_name).map(ToOwned::to_owned)
        }
        TenantSelector::PartitionKeyPrefix { attribute_name }
        | TenantSelector::TableNameOrPartitionKeyPrefix { attribute_name } => {
            string_attribute(item, attribute_name)
                .and_then(|value| value.split('#').nth(1))
                .map(ToOwned::to_owned)
        }
        TenantSelector::None | TenantSelector::AttributeRegex { .. } => None,
    }
}

fn string_attribute<'a>(item: &'a StorageItem, attribute_name: &str) -> Option<&'a str> {
    match item.get(attribute_name) {
        Some(StorageValue::S(value)) => Some(value.as_str()),
        _ => None,
    }
}

fn timestamp_from_config(timestamp: &RetentionTimestampConfig) -> RetentionTimestamp {
    match timestamp {
        RetentionTimestampConfig::Attribute { attribute_path } => RetentionTimestamp::Attribute {
            attribute_path: attribute_path.clone(),
        },
        RetentionTimestampConfig::IngestedAt => RetentionTimestamp::IngestedAt,
    }
}

fn policy_source(table: &RetentionTableRuntime) -> &'static str {
    match table.config.tenant_policy.source {
        config::TenantRetentionPolicySource::AuxStorage => "aux_storage",
        config::TenantRetentionPolicySource::DynamoDb => "dynamodb",
    }
}

fn record_lookup_metrics(
    table: &str,
    source: &'static str,
    outcome: &'static str,
    started: Instant,
) {
    metrics::counter!(RETENTION_LOOKUPS_TOTAL_METRIC, "table" => table.to_string(), "source" => source, "outcome" => outcome).increment(1);
    metrics::histogram!(RETENTION_LOOKUP_LATENCY_METRIC, "table" => table.to_string(), "source" => source, "outcome" => outcome).record(duration_ms(started));
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis())
}

fn duration_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

#[allow(clippy::cast_precision_loss)]
fn u64_to_f64(value: u64) -> f64 {
    value as f64
}
