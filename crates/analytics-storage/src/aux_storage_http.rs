use std::time::Duration;

use http_request::{
    HttpClient, HttpClientBuilder, HttpRequestError, RetryConfig, Url,
    header::{CONTENT_TYPE, HeaderMap, HeaderValue},
};
use serde::{Serialize, de::DeserializeOwned};

use crate::error::{
    AnalyticsStorageError, AnalyticsStorageErrorDebug, AnalyticsStorageErrorKind,
    AnalyticsStorageResult,
};

const INTERNAL_SERVICE_TOKEN_HEADER: &str = "x-aux-internal-token";

#[derive(Debug, Clone, Copy)]
pub(crate) enum ResponseBodyPolicy {
    Bounded,
    Unbounded,
}

impl ResponseBodyPolicy {
    fn decode_error_body(self, body: &[u8]) -> AnalyticsStorageResult<String> {
        match self {
            Self::Bounded => {
                String::from_utf8(body.to_vec()).map_err(|_| HttpRequestError::Decode.into())
            }
            Self::Unbounded => Ok(String::from_utf8_lossy(body).into_owned()),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AuxStorageHttpClient {
    client: HttpClient,
    base_url: Url,
    body_policy: ResponseBodyPolicy,
    service_token: Option<HeaderValue>,
}

impl AuxStorageHttpClient {
    pub(crate) fn try_new(
        base_url: &str,
        timeout: Duration,
        retry: RetryConfig,
        body_policy: ResponseBodyPolicy,
    ) -> AnalyticsStorageResult<Self> {
        let mut base_url = Url::parse(base_url)?;
        let path = base_url.path().trim_end_matches('/').to_string();
        base_url.set_path(if path.is_empty() { "/" } else { &path });
        Ok(Self {
            client: HttpClientBuilder::new()
                .retry(retry)
                .with_reqwest_builder(|builder| builder.timeout(timeout).connect_timeout(timeout))
                .build()?,
            base_url,
            body_policy,
            service_token: None,
        })
    }

    /// Sends the shared internal-service secret with every request.
    pub(crate) fn with_service_token(mut self, token: &str) -> AnalyticsStorageResult<Self> {
        let mut value = HeaderValue::from_str(token)?;
        value.set_sensitive(true);
        self.service_token = Some(value);
        Ok(self)
    }

    pub(crate) async fn send_action<TRequest, TResponse>(
        &self,
        action: &str,
        request: &TRequest,
    ) -> AnalyticsStorageResult<TResponse>
    where
        TRequest: Serialize + ?Sized,
        TResponse: DeserializeOwned,
    {
        let response = self
            .client
            .post(self.base_url.clone())
            .headers(self.build_action_headers(action)?)
            .json(request)
            .send()
            .await?;
        let status = response.status();
        let response = match self.body_policy {
            ResponseBodyPolicy::Unbounded => response.with_max_body_size(None),
            ResponseBodyPolicy::Bounded => response,
        };
        let body = response.bytes().await?;
        if !status.is_success() {
            return Err(AnalyticsStorageError::with_debug(
                AnalyticsStorageErrorKind::HttpStatus,
                AnalyticsStorageErrorDebug::HttpStatus {
                    status,
                    body: self.body_policy.decode_error_body(&body)?,
                },
            ));
        }
        Ok(serde_json::from_slice(&body)?)
    }
    fn build_action_headers(&self, action: &str) -> AnalyticsStorageResult<HeaderMap> {
        let mut headers = HeaderMap::new();
        if let Some(token) = &self.service_token {
            headers.insert(INTERNAL_SERVICE_TOKEN_HEADER, token.clone());
        }
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-amz-json-1.0"),
        );
        headers.insert(
            "x-amz-target",
            HeaderValue::from_str(&format!("DynamoDB_20120810.{action}"))?,
        );
        Ok(headers)
    }
}
