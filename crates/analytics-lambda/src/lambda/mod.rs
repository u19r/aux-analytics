use std::{path::PathBuf, sync::Arc};

use analytics_api::AnalyticsEngineAccess;
use analytics_contract::{AnalyticsManifest, PrivacyPolicy};
use analytics_engine::{AnalyticsEngine, StorageBackend};
use analytics_operations::{
    MaintenanceError, MaintenanceOperations, OperationActor, OperationStore,
};
use config::{
    AnalyticsLambdaResponseCompression, ConfigRequest, LambdaEnvironment, resolve_config,
};
use serde_json::{Value, value::RawValue};

mod event;
mod ingestion;
mod queries;
mod response;

pub(crate) use event::ingest_outcome_name;
use event::validate_event;
pub use event::{
    AnalyticsLambdaEvent, IngestBatchLambdaResponse, IngestLambdaResponse, InitResponse,
    QueryBatchLambdaResponse, TenantQueryBatchItem,
};
use response::{load_privacy_policy, serialize_response};

use crate::error::AnalyticsLambdaError;

mod dispatch;
mod handler;
mod maintenance;
mod options;
pub use handler::AnalyticsLambdaHandler;
pub use options::{LambdaHandlerOptions, LambdaResponseConfig};
