use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::source_polling::metrics::*;

static SOURCE_POLLING_LEASE_TOKEN_COUNTER: AtomicU64 = AtomicU64::new(0);
const SOURCE_RETRY_BASE_DELAY: Duration = Duration::from_millis(100);
const SOURCE_RETRY_MAX_DELAY: Duration = Duration::from_secs(5);

pub(crate) fn source_polling_lease_until_ms(now_ms: i64) -> i64 {
    now_ms.saturating_add(SOURCE_POLLING_LEASE_DURATION_MS)
}

pub(crate) fn source_retry_delay(consecutive_failures: u32) -> Duration {
    if consecutive_failures == 0 {
        return Duration::ZERO;
    }
    let exponent = consecutive_failures.saturating_sub(1).min(6);
    SOURCE_RETRY_BASE_DELAY
        .saturating_mul(1_u32 << exponent)
        .min(SOURCE_RETRY_MAX_DELAY)
}

#[cfg(test)]
pub(crate) fn source_polling_lease_renew_interval() -> Duration {
    SOURCE_POLLING_LEASE_RENEW_INTERVAL
}

pub(crate) fn source_polling_lease_token(worker_id: &str) -> String {
    let sequence = SOURCE_POLLING_LEASE_TOKEN_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{}-{}-{}", worker_id, now_ms_i64(), sequence)
}

pub(crate) fn source_polling_worker_id() -> String {
    std::env::var("AUX_ANALYTICS_WORKER_ID")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| std::env::var("ECS_CONTAINER_METADATA_URI_V4").ok())
        .filter(|value| !value.trim().is_empty())
        .or_else(|| std::env::var("ECS_CONTAINER_METADATA_URI").ok())
        .filter(|value| !value.trim().is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| format!("pid-{}-{}", std::process::id(), now_ms()))
}

pub(crate) fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis())
}

pub(crate) fn now_ms_i64() -> i64 {
    let now = now_ms();
    i64::try_from(now).unwrap_or(i64::MAX)
}

#[allow(clippy::cast_precision_loss)]
pub(crate) fn usize_to_f64(value: usize) -> f64 {
    value as f64
}

#[allow(clippy::cast_precision_loss)]
pub(crate) fn milliseconds_to_seconds_f64(value: u128) -> f64 {
    value as f64 / 1_000.0
}

#[allow(clippy::cast_precision_loss)]
pub(crate) fn milliseconds_i64_to_f64(value: i64) -> f64 {
    value as f64
}
