mod composition;
mod cors;
mod lifecycle;
mod listener;
mod process_duty;
mod telemetry;

#[cfg(test)]
mod process_duty_tests;

#[cfg(test)]
pub(crate) use composition::{AppStateStartupInput, initialize_app_state, start_retention_jobs};
#[cfg(test)]
pub(crate) use cors::{CorsOrigins, cors_origins};
#[cfg(test)]
pub(crate) use lifecycle::parse_shutdown_grace_period;
pub(crate) use listener::serve;
pub(crate) use process_duty::ProcessDuty;
#[cfg(test)]
pub(crate) use telemetry::{FilterSource, metrics_endpoint_config, resolve_filter};
