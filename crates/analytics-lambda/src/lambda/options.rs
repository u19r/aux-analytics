use super::*;

#[derive(Debug, Clone, Copy)]
pub struct LambdaResponseConfig {
    pub(crate) max_response_size_kb: Option<usize>,
    pub(crate) response_compression: AnalyticsLambdaResponseCompression,
    pub(crate) query_only: bool,
    pub(crate) disable_query_deadline: bool,
}

impl Default for LambdaResponseConfig {
    fn default() -> Self {
        Self {
            max_response_size_kb: None,
            response_compression: AnalyticsLambdaResponseCompression::None,
            query_only: false,
            disable_query_deadline: false,
        }
    }
}

impl From<&config::AnalyticsQueryConfig> for LambdaResponseConfig {
    fn from(query: &config::AnalyticsQueryConfig) -> Self {
        Self {
            max_response_size_kb: query.max_response_size_kb,
            response_compression: query.lambda_response_compression,
            query_only: query.lambda_query_only,
            disable_query_deadline: query.disable_query_deadline,
        }
    }
}

pub struct LambdaHandlerOptions {
    pub privacy_policy: Option<Arc<PrivacyPolicy>>,
    pub max_read_connections: usize,
    pub response_config: LambdaResponseConfig,
    pub ingest_enabled: bool,
}

impl Default for LambdaHandlerOptions {
    fn default() -> Self {
        Self {
            privacy_policy: None,
            max_read_connections: config::DEFAULT_QUERY_MAX_READ_CONNECTIONS,
            response_config: LambdaResponseConfig::default(),
            ingest_enabled: true,
        }
    }
}
