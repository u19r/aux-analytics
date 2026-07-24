use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use tokio::{sync::oneshot, task::JoinHandle};

static PEAK_BATCH_INCREMENT_BYTES: AtomicU64 = AtomicU64::new(0);

pub(crate) struct BatchResidentMemorySampler {
    baseline_bytes: Option<u64>,
    stop: oneshot::Sender<()>,
    task: JoinHandle<Option<u64>>,
}

pub(crate) fn record_batch_memory_increment(increment_bytes: u64) -> u64 {
    PEAK_BATCH_INCREMENT_BYTES.fetch_max(increment_bytes, Ordering::Relaxed);
    PEAK_BATCH_INCREMENT_BYTES.load(Ordering::Relaxed)
}

impl BatchResidentMemorySampler {
    pub(crate) fn start() -> Self {
        let baseline_bytes = linux_resident_memory_bytes();
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut peak_bytes = baseline_bytes;
            let mut interval = tokio::time::interval(Duration::from_millis(10));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        peak_bytes = peak_bytes.max(linux_resident_memory_bytes());
                    }
                    _ = &mut stopped => break,
                }
            }
            peak_bytes
        });
        Self {
            baseline_bytes,
            stop,
            task,
        }
    }

    pub(crate) async fn finish(self) -> Option<u64> {
        let _ = self.stop.send(());
        let peak_bytes = self.task.await.ok().flatten()?;
        Some(peak_bytes.saturating_sub(self.baseline_bytes?))
    }
}

fn linux_resident_memory_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    parse_linux_resident_memory_bytes(&status)
}

pub(crate) fn parse_linux_resident_memory_bytes(status: &str) -> Option<u64> {
    let kibibytes = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))?
        .split_ascii_whitespace()
        .next()?
        .parse::<u64>()
        .ok()?;
    kibibytes.checked_mul(1_024)
}
