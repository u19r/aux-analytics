use config::{
    TenantRetentionPolicyConfig, TenantRetentionPolicyRequest, TenantRetentionPolicySource,
};

use super::{aws, remote};
use crate::{
    constants::RETENTION_REQUEST_TIMEOUT,
    error::{AnalyticsStorageError, AnalyticsStorageErrorKind, AnalyticsStorageResult},
};

#[derive(Debug)]
pub struct RetentionPolicyLookup {
    request: TenantRetentionPolicyRequest,
    duration_attribute_path: String,
    client: RetentionPolicyClient,
}

#[derive(Debug)]
enum RetentionPolicyClient {
    AuxStorage(remote::AuxStoragePolicyClient),
    DynamoDb(aws::DynamoDbPolicyClient),
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
                RetentionPolicyClient::AuxStorage(remote::AuxStoragePolicyClient::try_new(
                    endpoint,
                    RETENTION_REQUEST_TIMEOUT,
                )?)
            }
            TenantRetentionPolicySource::DynamoDb => RetentionPolicyClient::DynamoDb(
                aws::DynamoDbPolicyClient::from_policy(policy).await,
            ),
        };
        Ok(Self {
            request: policy.request.clone(),
            duration_attribute_path: policy.duration_selector.attribute_path.clone(),
            client,
        })
    }

    pub async fn lookup_period_ms(&self, tenant_id: &str) -> AnalyticsStorageResult<u64> {
        let path = self.duration_attribute_path.as_str();
        match &self.client {
            RetentionPolicyClient::AuxStorage(client) => remote::duration_ms_from_attribute_map(
                &client.lookup(&self.request, tenant_id).await?,
                path,
            ),
            RetentionPolicyClient::DynamoDb(client) => remote::duration_ms_from_aws_item(
                &client.lookup(&self.request, tenant_id).await?,
                path,
            ),
        }
    }
}
