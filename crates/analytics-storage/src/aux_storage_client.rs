use std::time::Duration;

use reqwest::{
    Method, Url,
    header::{CONTENT_TYPE, HeaderMap, HeaderValue},
};
use serde::{Serialize, de::DeserializeOwned};
use storage_types::{GetStreamRecordsRequest, GetStreamRecordsResponse, TableName};

use crate::error::{
    AnalyticsStorageError, AnalyticsStorageErrorDebug, AnalyticsStorageErrorKind,
    AnalyticsStorageResult,
};

#[derive(Debug)]
pub(crate) struct AuxStorageStreamClient {
    client: reqwest::Client,
    base_url: Url,
}

impl AuxStorageStreamClient {
    pub(crate) fn new(base_url: &str, timeout: Duration) -> AnalyticsStorageResult<Self> {
        let mut normalized = base_url.to_string();
        if !normalized.ends_with('/') {
            normalized.push('/');
        }
        Ok(Self {
            client: reqwest::Client::builder().timeout(timeout).build()?,
            base_url: Url::parse(&normalized)?,
        })
    }

    pub(crate) async fn get_stream_records(
        &self,
        table_name: &str,
        last_evaluated_key: Option<String>,
    ) -> AnalyticsStorageResult<GetStreamRecordsResponse> {
        self.dynamo(
            "GetStreamRecords",
            &GetStreamRecordsRequest {
                table_name: TableName::new(table_name),
                last_evaluated_key,
                limit: Some(1000),
            },
        )
        .await
    }

    async fn dynamo<TRequest, TResponse>(
        &self,
        action: &str,
        request: &TRequest,
    ) -> AnalyticsStorageResult<TResponse>
    where
        TRequest: Serialize + ?Sized,
        TResponse: DeserializeOwned,
    {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-amz-json-1.0"),
        );
        headers.insert(
            "x-amz-target",
            HeaderValue::from_str(&format!("DynamoDB_20120810.{action}"))?,
        );
        self.send_json(Method::POST, "", Some(headers), Some(request))
            .await
    }

    async fn send_json<TRequest, TResponse>(
        &self,
        method: Method,
        path: &str,
        headers: Option<HeaderMap>,
        request: Option<&TRequest>,
    ) -> AnalyticsStorageResult<TResponse>
    where
        TRequest: Serialize + ?Sized,
        TResponse: DeserializeOwned,
    {
        let url = if path.is_empty() {
            let mut url = self.base_url.clone();
            let current_path = url.path().to_string();
            if current_path.len() > 1 && current_path.ends_with('/') {
                url.set_path(current_path.trim_end_matches('/'));
            }
            url
        } else {
            self.base_url.join(path)?
        };
        let mut builder = self.client.request(method, url);
        if let Some(headers) = headers {
            builder = builder.headers(headers);
        }
        if let Some(request) = request {
            builder = builder.json(request);
        }
        let response = builder.send().await?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(AnalyticsStorageError::with_debug(
                AnalyticsStorageErrorKind::HttpStatus,
                AnalyticsStorageErrorDebug::HttpStatus { status, body },
            ));
        }
        Ok(serde_json::from_str(body.as_str())?)
    }
}
