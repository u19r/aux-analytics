use std::time::Duration;

use http_request::RetryConfig;
use storage_types::{
    DescribeSystemStreamRequest, DescribeSystemStreamResponse, GetStreamRecordsRequest,
    GetStreamRecordsResponse, SystemStreamCheckpointToken, SystemStreamScope,
};

use crate::{
    aux_storage_http::{AuxStorageHttpClient, ResponseBodyPolicy},
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
        self.client
            .send_action(
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
        self.client
            .send_action(
                "GetStreamRecords",
                &GetStreamRecordsRequest::system_stream(source_scope, checkpoint, Some(limit)),
            )
            .await
    }
}
