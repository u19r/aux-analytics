use analytics_engine::AnalyticsEngineError;
use analytics_operations::MaintenanceError;
use thiserror::Error;

pub type BootstrapError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Error)]
pub enum AnalyticsLambdaError {
    #[error("analytics engine error: {0}")]
    Engine(#[from] analytics_engine::AnalyticsEngineError),
    #[error("analytics_record_contract: {0}")]
    RecordContract(String),
    #[error("analytics engine access error: {0}")]
    EngineAccess(#[from] analytics_api::AnalyticsEngineAccessError),
    #[error("config error: {0}")]
    Config(#[from] config::ConfigError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error(
        "analytics_response_too_large: serialized response is {actual_bytes} bytes, limit is \
         {limit_bytes} bytes"
    )]
    ResponseTooLarge {
        actual_bytes: usize,
        limit_bytes: usize,
    },
    #[error("schema error: {0}")]
    Schema(String),
    #[error("privacy policy error: {0}")]
    PrivacyPolicy(#[from] analytics_contract::PrivacyPolicyError),
    #[error("maintenance operation error: {0}")]
    Maintenance(#[from] MaintenanceError),
    #[error("query task failed: {0}")]
    QueryTaskJoin(#[from] tokio::task::JoinError),
    #[error("validation error: {0}")]
    Validation(String),
}

impl AnalyticsLambdaError {
    pub(crate) fn schema(message: impl Into<String>) -> Self {
        Self::Schema(message.into())
    }

    pub(crate) fn validation(message: impl Into<String>) -> Self {
        Self::Validation(message.into())
    }

    pub(crate) fn ingest(error: AnalyticsEngineError) -> Self {
        if error.is_record_contract_failure() {
            Self::RecordContract(error.to_string())
        } else {
            Self::Engine(error)
        }
    }
}
