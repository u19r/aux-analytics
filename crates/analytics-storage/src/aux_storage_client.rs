use std::time::Duration;

use http_request::RetryConfig;
use serde::{Serialize, de::DeserializeOwned};
use storage_types::{
    DescribeSystemStreamRequest, DescribeSystemStreamResponse, GetStreamRecordsRequest,
    GetStreamRecordsResponse, SystemStreamCheckpointToken, SystemStreamScope,
};

use crate::{
    aux_storage_http::{AuxStorageHttpClient, ResponseBodyPolicy},
    constants::{SOURCE_READ_BASE_DELAY, SOURCE_READ_MAX_ATTEMPTS, SOURCE_READ_MAX_DELAY},
    error::AnalyticsStorageResult,
};

#[derive(Debug)]
pub(crate) struct AuxStorageStreamClient {
    client: AuxStorageHttpClient,
}

impl AuxStorageStreamClient {
    pub(crate) fn try_new(base_url: &str, timeout: Duration) -> AnalyticsStorageResult<Self> {
        Ok(Self {
            client: AuxStorageHttpClient::try_new(
                base_url,
                timeout,
                RetryConfig {
                    max_retries: 0,
                    ..RetryConfig::default()
                },
                ResponseBodyPolicy::Unbounded,
            )?,
        })
    }

    pub(crate) async fn describe_system_stream(
        &self,
    ) -> AnalyticsStorageResult<DescribeSystemStreamResponse> {
        self.send_with_retry(
            "DescribeSystemStream",
            &DescribeSystemStreamRequest::default(),
        )
        .await
    }

    pub(crate) async fn get_system_stream_records(
        &self,
        source_scope: SystemStreamScope,
        checkpoint: Option<SystemStreamCheckpointToken>,
        limit: u32,
    ) -> AnalyticsStorageResult<GetStreamRecordsResponse> {
        self.send_with_retry(
            "GetStreamRecords",
            &GetStreamRecordsRequest::system_stream(source_scope, checkpoint, Some(limit)),
        )
        .await
    }

    async fn send_with_retry<TRequest, TResponse>(
        &self,
        action: &str,
        request: &TRequest,
    ) -> AnalyticsStorageResult<TResponse>
    where
        TRequest: Serialize + ?Sized,
        TResponse: DeserializeOwned,
    {
        let mut attempt = 0_u32;
        loop {
            match self.client.send_action(action, request).await {
                Ok(response) => return Ok(response),
                Err(error)
                    if attempt + 1 < SOURCE_READ_MAX_ATTEMPTS
                        && error.is_retryable_source_read() =>
                {
                    let delay = SOURCE_READ_BASE_DELAY
                        .saturating_mul(1_u32 << attempt.min(5))
                        .min(SOURCE_READ_MAX_DELAY);
                    tracing::debug!(
                        action,
                        attempt,
                        delay_ms = delay.as_millis(),
                        "retrying analytics source read"
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }
}
