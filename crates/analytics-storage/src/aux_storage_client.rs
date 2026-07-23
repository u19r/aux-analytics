use std::{collections::HashMap, time::Duration};

use reqwest::{
    Method, Url,
    header::{CONTENT_TYPE, HeaderMap, HeaderValue},
};
use serde::{Serialize, de::DeserializeOwned};
use storage_types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableRequest, GetStreamRecordsRequest,
    GetStreamRecordsResponse, KeyAttributeType, KeyAttributes, KeySchemaElement, KeyType,
    TableName, UpdateItemRequest, UpdateItemResponse,
};

use crate::error::{
    AnalyticsStorageError, AnalyticsStorageErrorDebug, AnalyticsStorageErrorKind,
    AnalyticsStorageResult,
};

#[derive(Debug)]
pub(crate) struct AuxStorageStreamClient {
    client: reqwest::Client,
    base_url: Url,
}

#[derive(Debug)]
pub struct AuxStorageLeaseClient {
    client: AuxStorageStreamClient,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuxStorageLeaseOutcome {
    Acquired,
    HeldByAnotherWorker,
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
        last_evaluated_key: Option<String>,
        limit: u32,
    ) -> AnalyticsStorageResult<GetStreamRecordsResponse> {
        self.dynamo(
            "GetStreamRecords",
            &GetStreamRecordsRequest {
                table_name: None,
                system_stream: true,
                last_evaluated_key,
                limit: Some(limit),
            },
        )
        .await
    }

    pub(crate) async fn update_item(
        &self,
        request: &UpdateItemRequest,
    ) -> AnalyticsStorageResult<UpdateItemResponse> {
        self.dynamo("UpdateItem", request).await
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

impl AuxStorageLeaseClient {
    pub fn new(base_url: &str, timeout: Duration) -> AnalyticsStorageResult<Self> {
        Ok(Self {
            client: AuxStorageStreamClient::new(base_url, timeout)?,
        })
    }

    pub async fn ensure_source_polling_lease_table(&self) -> AnalyticsStorageResult<()> {
        self.client
            .dynamo::<_, serde_json::Value>("CreateTable", &source_polling_lease_table_request())
            .await
            .map(|_| ())
    }

    pub async fn try_acquire_source_polling_lease(
        &self,
        worker_id: &str,
        lease_token: &str,
        now_ms: i64,
        lease_until_ms: i64,
    ) -> AnalyticsStorageResult<AuxStorageLeaseOutcome> {
        let request =
            source_polling_lease_acquire_request(worker_id, lease_token, now_ms, lease_until_ms);
        match self.client.update_item(&request).await {
            Ok(_) => Ok(AuxStorageLeaseOutcome::Acquired),
            Err(error) if is_conditional_check_failed(&error) => {
                Ok(AuxStorageLeaseOutcome::HeldByAnotherWorker)
            }
            Err(error) => Err(error),
        }
    }

    pub async fn renew_source_polling_lease(
        &self,
        worker_id: &str,
        lease_token: &str,
        lease_until_ms: i64,
    ) -> AnalyticsStorageResult<bool> {
        let request = source_polling_lease_renew_request(worker_id, lease_token, lease_until_ms);
        match self.client.update_item(&request).await {
            Ok(_) => Ok(true),
            Err(error) if is_conditional_check_failed(&error) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub async fn release_source_polling_lease(
        &self,
        worker_id: &str,
        lease_token: &str,
        released_until_ms: i64,
    ) -> AnalyticsStorageResult<bool> {
        let request =
            source_polling_lease_release_request(worker_id, lease_token, released_until_ms);
        match self.client.update_item(&request).await {
            Ok(_) => Ok(true),
            Err(error) if is_conditional_check_failed(&error) => Ok(false),
            Err(error) => Err(error),
        }
    }
}

fn source_polling_lease_table_request() -> CreateTableRequest {
    CreateTableRequest::new(
        TableName::new("sys_jobs"),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        BillingMode::PayPerRequest,
    )
}

fn source_polling_lease_acquire_request(
    worker_id: &str,
    lease_token: &str,
    now_ms: i64,
    lease_until_ms: i64,
) -> UpdateItemRequest {
    source_polling_lease_update_request(
        worker_id,
        lease_token,
        lease_until_ms,
        Some(now_ms),
        "(attribute_not_exists(lease_until_ms) OR lease_until_ms < :now)".to_string(),
        "acquired",
    )
}

fn source_polling_lease_renew_request(
    worker_id: &str,
    lease_token: &str,
    lease_until_ms: i64,
) -> UpdateItemRequest {
    source_polling_lease_update_request(
        worker_id,
        lease_token,
        lease_until_ms,
        None,
        "leased_by = :worker AND lease_token = :lease_token".to_string(),
        "renewed",
    )
}

fn source_polling_lease_release_request(
    worker_id: &str,
    lease_token: &str,
    released_until_ms: i64,
) -> UpdateItemRequest {
    source_polling_lease_update_request(
        worker_id,
        lease_token,
        released_until_ms,
        None,
        "leased_by = :worker AND lease_token = :lease_token".to_string(),
        "released",
    )
}

fn source_polling_lease_update_request(
    worker_id: &str,
    lease_token: &str,
    lease_until_ms: i64,
    now_ms: Option<i64>,
    condition_expression: String,
    state: &str,
) -> UpdateItemRequest {
    let mut key = KeyAttributes::new();
    key.insert(
        "pk".to_string(),
        AttributeValue::S("JOB#analytics_source_polling".to_string()),
    );
    key.insert("sk".to_string(), AttributeValue::S("LOCK".to_string()));

    let mut values = HashMap::new();
    values.insert(
        ":worker".to_string(),
        AttributeValue::S(worker_id.to_string()),
    );
    values.insert(
        ":lease".to_string(),
        AttributeValue::N(lease_until_ms.to_string()),
    );
    if let Some(now_ms) = now_ms {
        values.insert(":now".to_string(), AttributeValue::N(now_ms.to_string()));
    }
    values.insert(
        ":lease_token".to_string(),
        AttributeValue::S(lease_token.to_string()),
    );
    values.insert(
        ":job_id".to_string(),
        AttributeValue::S("analytics_source_polling".to_string()),
    );
    values.insert(":state".to_string(), AttributeValue::S(state.to_string()));

    UpdateItemRequest::builder()
        .table_name(TableName::new("sys_jobs"))
        .key(key)
        .update_expression(
            "SET leased_by = :worker, lease_until_ms = :lease, lease_token = :lease_token, job_id \
             = :job_id, job_state = :state"
                .to_string(),
        )
        .condition_expression(Some(condition_expression))
        .expression_attribute_values(Some(values))
        .build()
}

fn is_conditional_check_failed(error: &AnalyticsStorageError) -> bool {
    error.to_string().contains("ConditionalCheckFailed")
}
