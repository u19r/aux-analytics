use std::{collections::HashMap, time::Duration};

use aws_sdk_dynamodb::types::AttributeValue as DynamoDbAttributeValue;
use config::{
    TenantRetentionGetItemRequest, TenantRetentionPolicyRequest, TenantRetentionQueryTableRequest,
};
use http_request::RetryConfig;
use serde::{Deserialize, Serialize};
use storage_types::{AttributeMap, AttributeValue, KeyAttributes, VecMap};

use crate::{
    aux_storage_http::{AuxStorageHttpClient, ResponseBodyPolicy},
    constants::{AUX_STORAGE_POLICY_ATTEMPTS, BASE_BACKOFF_MS},
    error::{
        AnalyticsStorageError, AnalyticsStorageErrorDebug, AnalyticsStorageErrorKind,
        AnalyticsStorageResult,
    },
};

#[derive(Debug)]
pub(crate) struct AuxStoragePolicyClient {
    client: AuxStorageHttpClient,
}

impl AuxStoragePolicyClient {
    pub(crate) fn try_new(base_url: &str, timeout: Duration) -> AnalyticsStorageResult<Self> {
        Ok(Self {
            client: AuxStorageHttpClient::try_new(
                base_url,
                timeout,
                RetryConfig {
                    max_retries: AUX_STORAGE_POLICY_ATTEMPTS.saturating_sub(1),
                    base_delay: Duration::from_millis(BASE_BACKOFF_MS),
                    retry_non_idempotent: true,
                    ..RetryConfig::default()
                },
                ResponseBodyPolicy::Bounded,
            )?,
        })
    }

    pub(crate) async fn lookup(
        &self,
        request: &TenantRetentionPolicyRequest,
        tenant_id: &str,
    ) -> AnalyticsStorageResult<AttributeMap> {
        match request {
            TenantRetentionPolicyRequest::GetItem(request) => {
                self.lookup_get_item(request, tenant_id).await
            }
            TenantRetentionPolicyRequest::QueryTable(request) => {
                self.lookup_query(request, tenant_id).await
            }
        }
    }

    async fn lookup_get_item(
        &self,
        request: &TenantRetentionGetItemRequest,
        tenant_id: &str,
    ) -> AnalyticsStorageResult<AttributeMap> {
        let response: GetItemResponse = self
            .client
            .send_action(
                "GetItem",
                &GetItemRequest {
                    table_name: &request.table_name,
                    key: template_key_attributes(&request.key, tenant_id),
                    consistent_read: request.consistent_read,
                    projection_expression: request.projection_expression.as_deref(),
                    expression_attribute_names: request.expression_attribute_names.as_ref(),
                },
            )
            .await?;
        response
            .item
            .ok_or_else(|| retention_lookup_error("aux-storage GetItem returned no item"))
    }

    async fn lookup_query(
        &self,
        request: &TenantRetentionQueryTableRequest,
        tenant_id: &str,
    ) -> AnalyticsStorageResult<AttributeMap> {
        let response: QueryResponse = self
            .client
            .send_action(
                "Query",
                &QueryRequest {
                    table_name: &request.table_name,
                    index_name: request.index_name.as_deref(),
                    key_condition_expression: &request.key_condition_expression,
                    filter_expression: request.filter_expression.as_deref(),
                    expression_attribute_names: request.expression_attribute_names.as_ref(),
                    expression_attribute_values: template_expression_attribute_values(
                        &request.expression_attribute_values,
                        tenant_id,
                    ),
                    scan_index_forward: request.scan_index_forward,
                    limit: 1,
                },
            )
            .await?;
        response
            .items
            .into_iter()
            .next()
            .ok_or_else(|| retention_lookup_error("aux-storage Query returned no item"))
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub(super) struct GetItemRequest<'a> {
    pub(super) table_name: &'a str,
    pub(super) key: KeyAttributes,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) consistent_read: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) projection_expression: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) expression_attribute_names: Option<&'a VecMap<String, String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(super) struct GetItemResponse {
    #[serde(default)]
    pub(super) item: Option<AttributeMap>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub(super) struct QueryRequest<'a> {
    pub(super) table_name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) index_name: Option<&'a str>,
    pub(super) key_condition_expression: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) filter_expression: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) expression_attribute_names: Option<&'a VecMap<String, String>>,
    pub(super) expression_attribute_values: VecMap<String, AttributeValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) scan_index_forward: Option<bool>,
    pub(super) limit: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(super) struct QueryResponse {
    #[serde(default)]
    pub(super) items: Vec<AttributeMap>,
}

pub(super) fn template_key_attributes(values: &KeyAttributes, tenant_id: &str) -> KeyAttributes {
    values
        .iter()
        .map(|(key, value)| (key.to_string(), template_attribute_value(value, tenant_id)))
        .collect()
}

pub(super) fn template_expression_attribute_values(
    values: &VecMap<String, AttributeValue>,
    tenant_id: &str,
) -> VecMap<String, AttributeValue> {
    values
        .iter()
        .map(|(key, value)| (key.clone(), template_attribute_value(value, tenant_id)))
        .collect()
}

pub(crate) fn template_aws_attribute_names(
    values: Option<&VecMap<String, String>>,
) -> Option<HashMap<String, String>> {
    values.map(|values| {
        values
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    })
}

fn template_attribute_value(value: &AttributeValue, tenant_id: &str) -> AttributeValue {
    let mut value = value.clone();
    template_attribute_strings(&mut value, tenant_id);
    value
}

fn template_attribute_strings(value: &mut AttributeValue, tenant_id: &str) {
    match value {
        AttributeValue::S(value) | AttributeValue::N(value) | AttributeValue::B(value) => {
            replace_tenant_id(value, tenant_id)
        }
        AttributeValue::SS(values) | AttributeValue::NS(values) | AttributeValue::BS(values) => {
            for value in values {
                replace_tenant_id(value, tenant_id);
            }
        }
        AttributeValue::L(values) => {
            for value in values {
                template_attribute_strings(value, tenant_id);
            }
        }
        AttributeValue::M(values) => {
            for value in values.values_mut() {
                template_attribute_strings(value, tenant_id);
            }
        }
        AttributeValue::BOOL(_) | AttributeValue::NULL(_) => {}
    }
}

fn replace_tenant_id(value: &mut String, tenant_id: &str) {
    if value.contains("${tenant_id}") {
        *value = value.replace("${tenant_id}", tenant_id);
    }
}

pub(crate) fn template_aws_item(
    values: &KeyAttributes,
    tenant_id: &str,
) -> HashMap<String, DynamoDbAttributeValue> {
    template_aws_attributes(values.iter(), tenant_id)
}

pub(crate) fn template_aws_expression_values(
    values: &VecMap<String, AttributeValue>,
    tenant_id: &str,
) -> HashMap<String, DynamoDbAttributeValue> {
    template_aws_attributes(
        values.iter().map(|(key, value)| (key.as_str(), value)),
        tenant_id,
    )
}

fn template_aws_attributes<'a, I>(
    values: I,
    tenant_id: &str,
) -> HashMap<String, DynamoDbAttributeValue>
where
    I: Iterator<Item = (&'a str, &'a AttributeValue)>,
{
    values
        .map(|(key, value)| {
            (
                key.to_string(),
                attribute_value_to_aws(template_attribute_value(value, tenant_id)),
            )
        })
        .collect()
}

pub(crate) fn duration_ms_from_attribute_map(
    item: &AttributeMap,
    path: &str,
) -> AnalyticsStorageResult<u64> {
    let mut segments = path.split('.');
    let value = segments
        .next()
        .filter(|segment| !segment.is_empty())
        .and_then(|segment| item.get(segment))
        .map(RetentionAttribute::Aux);
    duration_ms_from_value_at_path(value, segments, path)
}

pub(crate) fn duration_ms_from_aws_item(
    item: &HashMap<String, DynamoDbAttributeValue>,
    path: &str,
) -> AnalyticsStorageResult<u64> {
    let mut segments = path.split('.');
    let value = segments
        .next()
        .filter(|segment| !segment.is_empty())
        .and_then(|segment| item.get(segment))
        .map(RetentionAttribute::Aws);
    duration_ms_from_value_at_path(value, segments, path)
}

fn duration_ms_from_value_at_path<'value, 'path>(
    value: Option<RetentionAttribute<'value>>,
    segments: impl Iterator<Item = &'path str>,
    path: &str,
) -> AnalyticsStorageResult<u64> {
    let mut value = value.ok_or_else(|| {
        invalid_retention_value(format!("duration selector did not match path {path}"))
    })?;
    for segment in segments {
        if segment.is_empty() {
            return Err(invalid_retention_value(format!(
                "duration selector did not match path {path}"
            )));
        }
        value = value.child(segment).ok_or_else(|| {
            invalid_retention_value(format!("duration selector did not match path {path}"))
        })?;
    }
    match value.number() {
        Some(number) => number
            .parse::<u64>()
            .map_err(|_| invalid_retention_value(format!("duration value at {path} is not a u64"))),
        None => Err(invalid_retention_value(format!(
            "duration value at {path} must be a DynamoDB N attribute"
        ))),
    }
}

enum RetentionAttribute<'a> {
    Aux(&'a AttributeValue),
    Aws(&'a DynamoDbAttributeValue),
}

impl<'a> RetentionAttribute<'a> {
    fn child(self, name: &str) -> Option<Self> {
        match self {
            Self::Aux(AttributeValue::M(values)) => values.get(name).map(Self::Aux),
            Self::Aws(DynamoDbAttributeValue::M(values)) => values.get(name).map(Self::Aws),
            Self::Aux(_) | Self::Aws(_) => None,
        }
    }

    fn number(self) -> Option<&'a str> {
        match self {
            Self::Aux(AttributeValue::N(value)) => Some(value),
            Self::Aws(DynamoDbAttributeValue::N(value)) => Some(value),
            Self::Aux(_) | Self::Aws(_) => None,
        }
    }
}

fn attribute_value_to_aws(value: AttributeValue) -> DynamoDbAttributeValue {
    match value {
        AttributeValue::S(value) => DynamoDbAttributeValue::S(value),
        AttributeValue::N(value) => DynamoDbAttributeValue::N(value),
        AttributeValue::B(value) => DynamoDbAttributeValue::B(value.into_bytes().into()),
        AttributeValue::SS(values) => DynamoDbAttributeValue::Ss(values),
        AttributeValue::NS(values) => DynamoDbAttributeValue::Ns(values),
        AttributeValue::BS(values) => DynamoDbAttributeValue::Bs(
            values
                .into_iter()
                .map(|value| value.into_bytes().into())
                .collect(),
        ),
        AttributeValue::BOOL(value) => DynamoDbAttributeValue::Bool(value),
        AttributeValue::NULL(value) => DynamoDbAttributeValue::Null(value),
        AttributeValue::L(values) => {
            DynamoDbAttributeValue::L(values.into_iter().map(attribute_value_to_aws).collect())
        }
        AttributeValue::M(values) => DynamoDbAttributeValue::M(
            values
                .into_iter()
                .map(|(key, value)| (key, attribute_value_to_aws(value)))
                .collect(),
        ),
    }
}

pub(crate) fn retention_lookup_error(message: impl Into<String>) -> AnalyticsStorageError {
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
