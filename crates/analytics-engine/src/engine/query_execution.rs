use std::{
    cell::RefCell,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use duckdb::Connection;

use super::{AnalyticsEngine, AnalyticsEngineError, AnalyticsEngineResult};

/// Cancellation belongs to one read operation, including its native work.
#[derive(Clone, Default)]
pub struct QueryCancellation(Arc<QueryCancellationState>);

#[derive(Default)]
struct QueryCancellationState {
    cancelled: AtomicBool,
    watcher: Mutex<Option<mpsc::Sender<QueryWatchdogSignal>>>,
}

enum QueryWatchdogSignal {
    Cancelled,
    Finished,
}

impl QueryCancellation {
    pub fn cancel(&self) {
        self.0.cancelled.store(true, Ordering::SeqCst);
        if let Some(watcher) = self
            .0
            .watcher
            .lock()
            .expect("query cancellation lock")
            .as_ref()
        {
            let _ = watcher.send(QueryWatchdogSignal::Cancelled);
        }
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::SeqCst)
    }
}

impl AnalyticsEngine {
    pub fn with_query_cancellation<T>(
        &self,
        cancellation: QueryCancellation,
        operation: impl FnOnce(&Self) -> T,
    ) -> T {
        let previous = self.query_cancellation.replace(Some(cancellation));
        let _scope = QueryCancellationScope {
            slot: &self.query_cancellation,
            previous,
        };
        operation(self)
    }
}

struct QueryCancellationScope<'a> {
    slot: &'a RefCell<Option<QueryCancellation>>,
    previous: Option<QueryCancellation>,
}

impl Drop for QueryCancellationScope<'_> {
    fn drop(&mut self) {
        self.slot.replace(self.previous.take());
    }
}

pub(super) struct QueryExecutionWatchdog {
    stop: mpsc::Sender<QueryWatchdogSignal>,
    worker: Option<thread::JoinHandle<()>>,
    cancellation: Option<QueryCancellation>,
    timed_out: Arc<AtomicBool>,
    timeout: Option<Duration>,
}

impl QueryExecutionWatchdog {
    pub(super) fn start(
        conn: &Connection,
        timeout: Option<Duration>,
        cancellation: Option<QueryCancellation>,
    ) -> AnalyticsEngineResult<Option<Self>> {
        if cancellation
            .as_ref()
            .is_some_and(QueryCancellation::is_cancelled)
        {
            return Err(AnalyticsEngineError::QueryCancelled);
        }
        if timeout.is_none() && cancellation.is_none() {
            return Ok(None);
        }
        let (stop, signals) = mpsc::channel();
        let timed_out = Arc::new(AtomicBool::new(false));
        let interrupt = conn.interrupt_handle();
        let deadline = timeout.and_then(|timeout| Instant::now().checked_add(timeout));
        let worker_timeout = Arc::clone(&timed_out);
        let worker = thread::spawn(move || {
            Self::watch_query_execution(signals, interrupt, deadline, worker_timeout)
        });
        if let Some(cancellation) = &cancellation {
            *cancellation
                .0
                .watcher
                .lock()
                .expect("query cancellation lock") = Some(stop.clone());
            if cancellation.is_cancelled() {
                let _ = stop.send(QueryWatchdogSignal::Cancelled);
            }
        }
        Ok(Some(Self {
            stop,
            worker: Some(worker),
            cancellation,
            timed_out,
            timeout,
        }))
    }

    pub(super) fn elapsed_error(&self) -> Option<AnalyticsEngineError> {
        if self
            .cancellation
            .as_ref()
            .is_some_and(QueryCancellation::is_cancelled)
        {
            return Some(AnalyticsEngineError::QueryCancelled);
        }
        let timeout = self.timeout?;
        self.timed_out
            .load(Ordering::SeqCst)
            .then(|| AnalyticsEngineError::QueryTimeout {
                timeout_ms: timeout.as_millis(),
            })
    }

    fn watch_query_execution(
        signals: mpsc::Receiver<QueryWatchdogSignal>,
        interrupt: Arc<duckdb::InterruptHandle>,
        deadline: Option<Instant>,
        timed_out: Arc<AtomicBool>,
    ) {
        let mut interrupting = false;
        loop {
            // Repeat only after cancellation/expiry to cover cancellation
            // between preparation and native execution. Completion joins this
            // thread before the connection can be reused, so a late interrupt
            // cannot escape.
            let wait = if interrupting {
                Some(Duration::from_millis(1))
            } else {
                deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()))
            };
            let signal = match wait {
                Some(wait) => signals.recv_timeout(wait),
                None => signals
                    .recv()
                    .map_err(|_| mpsc::RecvTimeoutError::Disconnected),
            };
            match signal {
                Ok(QueryWatchdogSignal::Finished) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return;
                }
                Ok(QueryWatchdogSignal::Cancelled) => interrupting = true,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if !interrupting {
                        timed_out.store(true, Ordering::SeqCst);
                    }
                    interrupting = true;
                }
            }
            interrupt.interrupt();
        }
    }
}

impl Drop for QueryExecutionWatchdog {
    fn drop(&mut self) {
        if let Some(cancellation) = &self.cancellation {
            cancellation
                .0
                .watcher
                .lock()
                .expect("query cancellation lock")
                .take();
        }
        let _ = self.stop.send(QueryWatchdogSignal::Finished);
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .expect("query watchdog completes before connection release");
        }
    }
}
