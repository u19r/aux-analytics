use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

use tokio::task::JoinHandle;

use crate::constants::{SOURCE_BATCH_MEMORY_INCREMENT_BYTES_METRIC, SOURCE_MEMORY_SAMPLE_INTERVAL};

static PEAK_BATCH_INCREMENT_BYTES: Mutex<u64> = Mutex::new(0);

/// Samples process-resident growth during one source poll or durable job
/// attempt. Linux reports current resident bytes; unsupported hosts omit this
/// metric.
pub(crate) struct SourceMemoryObservation {
    baseline: u64,
    peak: Arc<AtomicU64>,
    sampler: JoinHandle<()>,
}

impl SourceMemoryObservation {
    pub(crate) fn start() -> Option<Self> {
        let baseline = read_resident_bytes()?;
        let peak = Arc::new(AtomicU64::new(baseline));
        let sampled_peak = Arc::clone(&peak);
        let sampler = tokio::spawn(async move {
            let mut interval = tokio::time::interval(SOURCE_MEMORY_SAMPLE_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if let Some(bytes) = read_resident_bytes() {
                    sampled_peak.fetch_max(bytes, Ordering::Relaxed);
                }
            }
        });
        Some(Self {
            baseline,
            peak,
            sampler,
        })
    }
}

impl Drop for SourceMemoryObservation {
    fn drop(&mut self) {
        self.sampler.abort();
        if let Some(bytes) = read_resident_bytes() {
            self.peak.fetch_max(bytes, Ordering::Relaxed);
        }
        let increment = self
            .peak
            .load(Ordering::Relaxed)
            .saturating_sub(self.baseline);
        // Serialize publication too, so concurrent attempts cannot lower the
        // peak gauge.
        if let Ok(mut peak) = PEAK_BATCH_INCREMENT_BYTES.lock() {
            *peak = (*peak).max(increment);
            metrics::gauge!(SOURCE_BATCH_MEMORY_INCREMENT_BYTES_METRIC).set(*peak as f64);
        }
    }
}

fn read_resident_bytes() -> Option<u64> {
    if !cfg!(target_os = "linux") {
        return None;
    }
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    parse_resident_bytes(&status)
}

pub(super) fn parse_resident_bytes(status: &str) -> Option<u64> {
    let mut fields = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))?
        .split_ascii_whitespace();
    let kibibytes = fields.next()?.parse::<u64>().ok()?;
    if fields.next()? != "kB" {
        return None;
    }
    kibibytes.checked_mul(1024)
}
