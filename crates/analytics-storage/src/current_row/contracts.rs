use analytics_operations::{CheckError, CurrentRowStreamOrder};
use aws_sdk_dynamodb::types::AttributeValue as DynamoDbAttributeValue;
use serde::{Deserialize, Serialize};
use storage_types::{ExclusiveStartKey, ScanResponse, VecMap};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuxStorageCurrentRowScanRequest {
    pub table_name: String,
    pub projection_expression: String,
    pub expression_attribute_names: VecMap<String, String>,
    pub limit: u32,
    pub exclusive_start_key: Option<ExclusiveStartKey>,
}

pub trait AuxStorageCurrentRowClient {
    fn scan_current_rows(
        &self,
        request: &AuxStorageCurrentRowScanRequest,
    ) -> Result<ScanResponse, CheckError>;
}

/// SDK-facing current-row request data uses the small ordered map shared by
/// storage types. The caller's AWS SDK response is converted into this map at
/// the adapter boundary, once, before the analytics reader sees it.
#[derive(Debug, Clone, PartialEq)]
pub struct DynamoDbCurrentRowScanRequest {
    pub table_name: String,
    pub projection_expression: String,
    pub expression_attribute_names: VecMap<String, String>,
    pub limit: i32,
    pub exclusive_start_key: Option<VecMap<String, DynamoDbAttributeValue>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DynamoDbCurrentRowScanResponse {
    pub items: Vec<VecMap<String, DynamoDbAttributeValue>>,
    pub last_evaluated_key: Option<VecMap<String, DynamoDbAttributeValue>>,
}

pub trait DynamoDbCurrentRowClient {
    fn scan_current_rows(
        &self,
        request: &DynamoDbCurrentRowScanRequest,
    ) -> Result<DynamoDbCurrentRowScanResponse, CheckError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuxStorageCurrentRowPageReader<Client> {
    pub(super) client: Client,
    pub(super) stream_order: CurrentRowStreamOrder,
    pub(super) partition_column: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynamoDbCurrentRowPageReader<Client> {
    pub(super) client: Client,
    pub(super) stream_order: CurrentRowStreamOrder,
    pub(super) partition_column: Option<String>,
}
