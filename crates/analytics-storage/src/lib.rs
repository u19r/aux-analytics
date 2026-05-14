mod aux_storage_client;
mod aws_stream;
mod error;
mod facade;
pub(crate) mod planning;
mod poller;
mod poller_config;
mod retention;
mod types;

pub use error::{AnalyticsStorageError, AnalyticsStorageResult};
pub use poller::SourcePoller;
pub use poller_config::{PollRequest, PollerConfig};
pub use retention::RetentionPolicyLookup;
pub use types::{PollBatch, PolledRecord, SourceCheckpoint};

#[cfg(test)]
mod aux_storage_client_tests;
#[cfg(test)]
mod aws_stream_tests;
#[cfg(test)]
mod error_tests;
#[cfg(test)]
mod poller_tests;
#[cfg(test)]
mod quint_polling_restart_resume_tests;
#[cfg(test)]
mod quint_polling_storage_tests;
#[cfg(test)]
mod storage_tests;
