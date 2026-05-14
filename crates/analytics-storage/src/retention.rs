use std::{collections::HashMap, time::Duration};

use analytics_contract::{StorageItem, StorageValue};
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_dynamodb::{
    Client as DynamoDbClient, config::Region, types::AttributeValue as DynamoDbAttributeValue,
};
use config::{
    TenantRetentionPolicyConfig, TenantRetentionPolicyRequest, TenantRetentionPolicySource,
};
use reqwest::{
    Method, StatusCode, Url,
    header::{CONTENT_TYPE, HeaderMap, HeaderValue},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::error::{
    AnalyticsStorageError, AnalyticsStorageErrorDebug, AnalyticsStorageErrorKind,
    AnalyticsStorageResult,
};

const AUX_STORAGE_POLICY_ATTEMPTS: usize = 3;
const BASE_BACKOFF_MS: u64 = 50;

#[derive(Debug, Clone)]
pub struct RetentionPolicyLookup {
    policy: TenantRetentionPolicyConfig,
    client: RetentionPolicyClient,
}

#[derive(Debug, Clone)]
enum RetentionPolicyClient {
    AuxStorage(AuxStoragePolicyClient),
    DynamoDb(DynamoDbClient),
}

impl RetentionPolicyLookup {
    pub async fn from_config(policy: &TenantRetentionPolicyConfig) -> AnalyticsStorageResult<Self> {
        let client = match policy.source {
            TenantRetentionPolicySource::AuxStorage => {
                let endpoint = policy.endpoint_url.as_deref().ok_or_else(|| {
                    AnalyticsStorageError::new(
                        AnalyticsStorageErrorKind::MissingRetentionPolicyEndpoint,
                    )
                })?;
                RetentionPolicyClient::AuxStorage(AuxStoragePolicyClient::new(
                    endpoint,
                    Duration::from_secs(5),
                )?)
            }
            TenantRetentionPolicySource::DynamoDb => {
                RetentionPolicyClient::DynamoDb(dynamodb_client(policy).await)
            }
        };
        Ok(Self {
            policy: policy.clone(),
            client,
        })
    }

    pub async fn lookup_period_ms(&self, tenant_id: &str) -> AnalyticsStorageResult<u64> {
        let item = match &self.client {
            RetentionPolicyClient::AuxStorage(client) => {
                client.lookup(&self.policy.request, tenant_id).await?
            }
            RetentionPolicyClient::DynamoDb(client) => {
                dynamodb_lookup(client, &self.policy.request, tenant_id).await?
            }
        };
        duration_ms_from_item(&item, self.policy.duration_selector.attribute_path.as_str())
    }
}

async fn dynamodb_client(policy: &TenantRetentionPolicyConfig) -> DynamoDbClient {
    let mut loader = aws_config::defaults(BehaviorVersion::latest());
    if let Some(region) = policy.region.as_deref() {
        loader = loader.region(Region::new(region.to_string()));
    }
    if let Some(credentials) = policy.credentials.as_ref()
        && let Some(static_credentials) = credentials.r#static.as_ref()
    {
        loader = loader.credentials_provider(static_credentials_provider(static_credentials));
    }
    DynamoDbClient::new(&loader.load().await)
}

fn static_credentials_provider(credentials: &config::RemoteStaticCredentialsConfig) -> Credentials {
    Credentials::new(
        credentials.access_key.clone(),
        credentials.secret_key.clone(),
        credentials.session_token.clone(),
        None,
        "aux-analytics",
    )
}

async fn dynamodb_lookup(
    client: &DynamoDbClient,
    request: &TenantRetentionPolicyRequest,
    tenant_id: &str,
) -> AnalyticsStorageResult<StorageItem> {
    match request {
        TenantRetentionPolicyRequest::GetItem(request) => {
            let response = client
                .get_item()
                .table_name(request.table_name.clone())
                .set_key(Some(template_aws_item(&request.key, tenant_id)?))
                .set_consistent_read(request.consistent_read)
                .set_projection_expression(request.projection_expression.clone())
                .set_expression_attribute_names(request.expression_attribute_names.clone())
                .send()
                .await
                .map_err(aws_sdk_error)?;
            response
                .item
                .map(aws_item_to_storage_item)
                .ok_or_else(|| retention_lookup_error("DynamoDB GetItem returned no item"))
        }
        TenantRetentionPolicyRequest::QueryTable(request) => {
            let response = client
                .query()
                .table_name(request.table_name.clone())
                .set_index_name(request.index_name.clone())
                .key_condition_expression(request.key_condition_expression.clone())
                .set_filter_expression(request.filter_expression.clone())
                .set_expression_attribute_names(request.expression_attribute_names.clone())
                .set_expression_attribute_values(Some(template_aws_item(
                    &request.expression_attribute_values,
                    tenant_id,
                )?))
                .set_scan_index_forward(request.scan_index_forward)
                .limit(1)
                .send()
                .await
                .map_err(aws_sdk_error)?;
            response
                .items
                .and_then(|items| items.into_iter().next())
                .map(aws_item_to_storage_item)
                .ok_or_else(|| retention_lookup_error("DynamoDB Query returned no item"))
        }
    }
}

fn aws_sdk_error(error: impl std::fmt::Display) -> AnalyticsStorageError {
    AnalyticsStorageError::with_debug(
        AnalyticsStorageErrorKind::RetentionPolicyLookup,
        AnalyticsStorageErrorDebug::Message(error.to_string()),
    )
}

#[derive(Debug, Clone)]
struct AuxStoragePolicyClient {
    client: reqwest::Client,
    base_url: Url,
}

impl AuxStoragePolicyClient {
    fn new(base_url: &str, timeout: Duration) -> AnalyticsStorageResult<Self> {
        let mut normalized = base_url.to_string();
        if !normalized.ends_with('/') {
            normalized.push('/');
        }
        Ok(Self {
            client: reqwest::Client::builder().timeout(timeout).build()?,
            base_url: Url::parse(&normalized)?,
        })
    }

    async fn lookup(
        &self,
        request: &TenantRetentionPolicyRequest,
        tenant_id: &str,
    ) -> AnalyticsStorageResult<StorageItem> {
        match request {
            TenantRetentionPolicyRequest::GetItem(request) => {
                let response: GetItemResponse = self
                    .dynamo_with_retry(
                        "GetItem",
                        &GetItemRequest {
                            table_name: request.table_name.clone(),
                            key: template_json_map(&request.key, tenant_id)?,
                            consistent_read: request.consistent_read,
                            projection_expression: request.projection_expression.clone(),
                            expression_attribute_names: request.expression_attribute_names.clone(),
                        },
                    )
                    .await?;
                response
                    .item
                    .map(json_item_to_storage_item)
                    .transpose()?
                    .ok_or_else(|| retention_lookup_error("aux-storage GetItem returned no item"))
            }
            TenantRetentionPolicyRequest::QueryTable(request) => {
                let response: QueryResponse = self
                    .dynamo_with_retry(
                        "Query",
                        &QueryRequest {
                            table_name: request.table_name.clone(),
                            index_name: request.index_name.clone(),
                            key_condition_expression: request.key_condition_expression.clone(),
                            filter_expression: request.filter_expression.clone(),
                            expression_attribute_names: request.expression_attribute_names.clone(),
                            expression_attribute_values: template_json_map(
                                &request.expression_attribute_values,
                                tenant_id,
                            )?,
                            scan_index_forward: request.scan_index_forward,
                            limit: 1,
                        },
                    )
                    .await?;
                response
                    .items
                    .into_iter()
                    .next()
                    .map(json_item_to_storage_item)
                    .transpose()?
                    .ok_or_else(|| retention_lookup_error("aux-storage Query returned no item"))
            }
        }
    }

    async fn dynamo_with_retry<TRequest, TResponse>(
        &self,
        action: &str,
        request: &TRequest,
    ) -> AnalyticsStorageResult<TResponse>
    where
        TRequest: Serialize + ?Sized,
        TResponse: DeserializeOwned,
    {
        let mut delay = Duration::from_millis(BASE_BACKOFF_MS);
        for attempt in 1..=AUX_STORAGE_POLICY_ATTEMPTS {
            match self.dynamo(action, request).await {
                Ok(response) => return Ok(response),
                Err(error) if attempt < AUX_STORAGE_POLICY_ATTEMPTS && is_retryable(&error) => {
                    tokio::time::sleep(delay).await;
                    delay = delay.saturating_mul(2);
                }
                Err(error) => return Err(error),
            }
        }
        Err(retention_lookup_error(
            "aux-storage lookup attempts exhausted",
        ))
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
        let url = {
            let mut url = self.base_url.clone();
            let current_path = url.path().to_string();
            if current_path.len() > 1 && current_path.ends_with('/') {
                url.set_path(current_path.trim_end_matches('/'));
            }
            url
        };
        let response = self
            .client
            .request(Method::POST, url)
            .headers(headers)
            .json(request)
            .send()
            .await?;
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

fn is_retryable(error: &AnalyticsStorageError) -> bool {
    let message = error.to_string();
    message.contains("analytics source http request failed")
        || message.contains("status=500")
        || message.contains("status=502")
        || message.contains("status=503")
        || message.contains("status=504")
        || message.contains(&StatusCode::INTERNAL_SERVER_ERROR.to_string())
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct GetItemRequest {
    table_name: String,
    key: serde_json::Map<String, Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    consistent_read: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    projection_expression: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expression_attribute_names: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GetItemResponse {
    #[serde(default)]
    item: Option<serde_json::Map<String, Value>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct QueryRequest {
    table_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    index_name: Option<String>,
    key_condition_expression: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    filter_expression: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expression_attribute_names: Option<HashMap<String, String>>,
    expression_attribute_values: serde_json::Map<String, Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    scan_index_forward: Option<bool>,
    limit: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct QueryResponse {
    #[serde(default)]
    items: Vec<serde_json::Map<String, Value>>,
}

fn template_json_map(
    values: &std::collections::BTreeMap<String, Value>,
    tenant_id: &str,
) -> AnalyticsStorageResult<serde_json::Map<String, Value>> {
    values
        .iter()
        .map(|(key, value)| Ok((key.clone(), template_json_value(value, tenant_id)?)))
        .collect()
}

fn template_json_value(value: &Value, tenant_id: &str) -> AnalyticsStorageResult<Value> {
    Ok(match value {
        Value::String(value) => Value::String(value.replace("${tenant_id}", tenant_id)),
        Value::Array(values) => Value::Array(
            values
                .iter()
                .map(|value| template_json_value(value, tenant_id))
                .collect::<AnalyticsStorageResult<Vec<_>>>()?,
        ),
        Value::Object(values) => Value::Object(
            values
                .iter()
                .map(|(key, value)| Ok((key.clone(), template_json_value(value, tenant_id)?)))
                .collect::<AnalyticsStorageResult<serde_json::Map<_, _>>>()?,
        ),
        other => other.clone(),
    })
}

fn template_aws_item(
    values: &std::collections::BTreeMap<String, Value>,
    tenant_id: &str,
) -> AnalyticsStorageResult<HashMap<String, DynamoDbAttributeValue>> {
    values
        .iter()
        .map(|(key, value)| {
            Ok((
                key.clone(),
                json_value_to_aws_attribute_value(&template_json_value(value, tenant_id)?)?,
            ))
        })
        .collect()
}

fn json_item_to_storage_item(
    item: serde_json::Map<String, Value>,
) -> AnalyticsStorageResult<StorageItem> {
    item.into_iter()
        .map(|(key, value)| Ok((key, serde_json::from_value::<StorageValue>(value)?)))
        .collect()
}

fn aws_item_to_storage_item(item: HashMap<String, DynamoDbAttributeValue>) -> StorageItem {
    item.into_iter()
        .map(|(key, value)| (key, aws_attribute_value_to_storage_value(value)))
        .collect()
}

fn duration_ms_from_item(item: &StorageItem, path: &str) -> AnalyticsStorageResult<u64> {
    let value = storage_value_at_path(item, path).ok_or_else(|| {
        invalid_retention_value(format!("duration selector did not match path {path}"))
    })?;
    match value {
        StorageValue::N(value) => value
            .parse::<u64>()
            .map_err(|_| invalid_retention_value(format!("duration value at {path} is not a u64"))),
        _ => Err(invalid_retention_value(format!(
            "duration value at {path} must be a DynamoDB N attribute"
        ))),
    }
}

fn storage_value_at_path<'a>(item: &'a StorageItem, path: &str) -> Option<&'a StorageValue> {
    let mut segments = path.split('.');
    let first = segments.next()?;
    if first.is_empty() {
        return None;
    }
    let mut current = item.get(first)?;
    for segment in segments {
        if segment.is_empty() {
            return None;
        }
        let StorageValue::M(map) = current else {
            return None;
        };
        current = map.get(segment)?;
    }
    Some(current)
}

fn json_value_to_aws_attribute_value(
    value: &Value,
) -> AnalyticsStorageResult<DynamoDbAttributeValue> {
    let storage_value = serde_json::from_value::<StorageValue>(value.clone())?;
    Ok(storage_value_to_aws_attribute_value(&storage_value))
}

fn storage_value_to_aws_attribute_value(value: &StorageValue) -> DynamoDbAttributeValue {
    match value {
        StorageValue::S(value) => DynamoDbAttributeValue::S(value.clone()),
        StorageValue::N(value) => DynamoDbAttributeValue::N(value.clone()),
        StorageValue::B(value) => DynamoDbAttributeValue::B(value.clone().into_bytes().into()),
        StorageValue::SS(values) => DynamoDbAttributeValue::Ss(values.clone()),
        StorageValue::NS(values) => DynamoDbAttributeValue::Ns(values.clone()),
        StorageValue::BS(values) => DynamoDbAttributeValue::Bs(
            values
                .iter()
                .map(|value| value.clone().into_bytes().into())
                .collect(),
        ),
        StorageValue::BOOL(value) => DynamoDbAttributeValue::Bool(*value),
        StorageValue::NULL(value) => DynamoDbAttributeValue::Null(*value),
        StorageValue::L(values) => DynamoDbAttributeValue::L(
            values
                .iter()
                .map(storage_value_to_aws_attribute_value)
                .collect(),
        ),
        StorageValue::M(values) => DynamoDbAttributeValue::M(
            values
                .iter()
                .map(|(key, value)| (key.clone(), storage_value_to_aws_attribute_value(value)))
                .collect(),
        ),
    }
}

fn aws_attribute_value_to_storage_value(value: DynamoDbAttributeValue) -> StorageValue {
    match value {
        DynamoDbAttributeValue::S(value) => StorageValue::S(value),
        DynamoDbAttributeValue::N(value) => StorageValue::N(value),
        DynamoDbAttributeValue::B(value) => {
            StorageValue::B(String::from_utf8_lossy(value.as_ref()).to_string())
        }
        DynamoDbAttributeValue::Ss(values) => StorageValue::SS(values),
        DynamoDbAttributeValue::Ns(values) => StorageValue::NS(values),
        DynamoDbAttributeValue::Bs(values) => StorageValue::BS(
            values
                .into_iter()
                .map(|value| String::from_utf8_lossy(value.as_ref()).to_string())
                .collect(),
        ),
        DynamoDbAttributeValue::Bool(value) => StorageValue::BOOL(value),
        DynamoDbAttributeValue::Null(value) => StorageValue::NULL(value),
        DynamoDbAttributeValue::L(values) => StorageValue::L(
            values
                .into_iter()
                .map(aws_attribute_value_to_storage_value)
                .collect(),
        ),
        DynamoDbAttributeValue::M(values) => StorageValue::M(aws_item_to_storage_item(values)),
        _ => StorageValue::NULL(true),
    }
}

fn retention_lookup_error(message: impl Into<String>) -> AnalyticsStorageError {
    AnalyticsStorageError::with_debug(
        AnalyticsStorageErrorKind::RetentionPolicyLookup,
        AnalyticsStorageErrorDebug::Message(message.into()),
    )
}

fn invalid_retention_value(message: impl Into<String>) -> AnalyticsStorageError {
    AnalyticsStorageError::with_debug(
        AnalyticsStorageErrorKind::InvalidRetentionPolicyValue,
        AnalyticsStorageErrorDebug::Message(message.into()),
    )
}
