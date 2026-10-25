use std::collections::HashMap;

use aws_sdk_dynamodb::{Client as DynamoDbClient, types::AttributeValue};
use config::{TenantRetentionPolicyConfig, TenantRetentionPolicyRequest};

use super::remote::{
    retention_lookup_error, template_aws_attribute_names, template_aws_expression_values,
    template_aws_item,
};
use crate::{
    aws_client::resolve_aws_config,
    error::{
        AnalyticsStorageError, AnalyticsStorageErrorDebug, AnalyticsStorageErrorKind,
        AnalyticsStorageResult,
    },
};

#[derive(Debug)]
pub(super) struct DynamoDbPolicyClient {
    client: DynamoDbClient,
}

impl DynamoDbPolicyClient {
    pub(super) async fn from_policy(policy: &TenantRetentionPolicyConfig) -> Self {
        let config = resolve_aws_config(
            policy.region.as_deref(),
            policy.endpoint_url.as_deref(),
            policy.credentials.as_ref(),
        )
        .await;
        Self {
            client: DynamoDbClient::new(&config),
        }
    }

    pub(super) async fn lookup(
        &self,
        request: &TenantRetentionPolicyRequest,
        tenant_id: &str,
    ) -> AnalyticsStorageResult<HashMap<String, AttributeValue>> {
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
        request: &config::TenantRetentionGetItemRequest,
        tenant_id: &str,
    ) -> AnalyticsStorageResult<HashMap<String, AttributeValue>> {
        let response = self
            .client
            .get_item()
            .table_name(request.table_name.clone())
            .set_key(Some(template_aws_item(&request.key, tenant_id)))
            .set_consistent_read(request.consistent_read)
            .set_projection_expression(request.projection_expression.clone())
            .set_expression_attribute_names(template_aws_attribute_names(
                request.expression_attribute_names.as_ref(),
            ))
            .send()
            .await
            .map_err(map_aws_sdk_error)?;
        response
            .item
            .ok_or_else(|| retention_lookup_error("DynamoDB GetItem returned no item"))
    }

    async fn lookup_query(
        &self,
        request: &config::TenantRetentionQueryTableRequest,
        tenant_id: &str,
    ) -> AnalyticsStorageResult<HashMap<String, AttributeValue>> {
        let response = self
            .client
            .query()
            .table_name(request.table_name.clone())
            .set_index_name(request.index_name.clone())
            .key_condition_expression(request.key_condition_expression.clone())
            .set_filter_expression(request.filter_expression.clone())
            .set_expression_attribute_names(template_aws_attribute_names(
                request.expression_attribute_names.as_ref(),
            ))
            .set_expression_attribute_values(Some(template_aws_expression_values(
                &request.expression_attribute_values,
                tenant_id,
            )))
            .set_scan_index_forward(request.scan_index_forward)
            .limit(1)
            .send()
            .await
            .map_err(map_aws_sdk_error)?;
        response
            .items
            .and_then(|items| items.into_iter().next())
            .ok_or_else(|| retention_lookup_error("DynamoDB Query returned no item"))
    }
}

fn map_aws_sdk_error(error: impl std::fmt::Display) -> AnalyticsStorageError {
    AnalyticsStorageError::with_debug(
        AnalyticsStorageErrorKind::RetentionPolicyLookup,
        AnalyticsStorageErrorDebug::Message(error.to_string()),
    )
}
