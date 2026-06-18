#[cfg(test)]
use std::time::Duration;
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::source_polling::metrics::*;

static SOURCE_POLLING_LEASE_TOKEN_COUNTER: AtomicU64 = AtomicU64::new(0);

pub(crate) fn source_polling_lease_until_ms(now_ms: i64) -> i64 {
    now_ms.saturating_add(SOURCE_POLLING_LEASE_DURATION_MS)
}

#[cfg(test)]
pub(crate) fn source_polling_lease_renew_interval() -> Duration {
    SOURCE_POLLING_LEASE_RENEW_INTERVAL
}

pub(crate) fn source_polling_released_until_ms(now_ms: i64) -> i64 {
    now_ms.saturating_sub(1)
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
