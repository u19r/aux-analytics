use std::{
    cell::RefCell,
    collections::{BTreeMap, VecDeque},
};

use analytics_operations::{CheckError, CheckRowProjection};
use aws_sdk_dynamodb::types::AttributeValue as DynamoDbAttributeValue;
use storage_types::{AttributeMap, AttributeValue, KeyAttributes, ScanResponse};

use crate::{
    AuxStorageCurrentRowClient, AuxStorageCurrentRowScanRequest, DynamoDbCurrentRowClient,
    DynamoDbCurrentRowScanRequest, DynamoDbCurrentRowScanResponse,
};

#[derive(Debug, Clone)]
pub(crate) struct RecordingClient {
    pub(crate) requests: RefCell<Vec<AuxStorageCurrentRowScanRequest>>,
    responses: RefCell<VecDeque<ScanResponse>>,
}

#[derive(Debug, Clone)]
pub(crate) struct RecordingDynamoDbClient {
    pub(crate) requests: RefCell<Vec<DynamoDbCurrentRowScanRequest>>,
    responses: RefCell<VecDeque<DynamoDbCurrentRowScanResponse>>,
}

impl RecordingClient {
    pub(crate) fn new(responses: Vec<ScanResponse>) -> Self {
        Self {
            requests: RefCell::new(Vec::new()),
            responses: RefCell::new(responses.into()),
        }
    }
}

impl RecordingDynamoDbClient {
    pub(crate) fn new(responses: Vec<DynamoDbCurrentRowScanResponse>) -> Self {
        Self {
            requests: RefCell::new(Vec::new()),
            responses: RefCell::new(responses.into()),
        }
    }
}

impl AuxStorageCurrentRowClient for RecordingClient {
    fn scan_current_rows(
        &self,
        request: &AuxStorageCurrentRowScanRequest,
    ) -> Result<ScanResponse, CheckError> {
        self.requests.borrow_mut().push(request.clone());
        self.responses
            .borrow_mut()
            .pop_front()
            .ok_or_else(missing_test_response)
    }
}

impl AuxStorageCurrentRowClient for &RecordingClient {
    fn scan_current_rows(
        &self,
        request: &AuxStorageCurrentRowScanRequest,
    ) -> Result<ScanResponse, CheckError> {
        (*self).scan_current_rows(request)
    }
}

impl DynamoDbCurrentRowClient for RecordingDynamoDbClient {
    fn scan_current_rows(
        &self,
        request: &DynamoDbCurrentRowScanRequest,
    ) -> Result<DynamoDbCurrentRowScanResponse, CheckError> {
        self.requests.borrow_mut().push(request.clone());
        self.responses
            .borrow_mut()
            .pop_front()
            .ok_or_else(missing_test_response)
    }
}

impl DynamoDbCurrentRowClient for &RecordingDynamoDbClient {
    fn scan_current_rows(
        &self,
        request: &DynamoDbCurrentRowScanRequest,
    ) -> Result<DynamoDbCurrentRowScanResponse, CheckError> {
        (*self).scan_current_rows(request)
    }
}

pub(crate) fn projection() -> CheckRowProjection {
    CheckRowProjection::new_with_optional_columns(
        "users",
        "user_id",
        vec!["email".to_string(), "org_id".to_string()],
        Some("__source_position".to_string()),
        Some("contains_private_data".to_string()),
    )
    .unwrap()
}

pub(crate) fn scan_response(items: Vec<AttributeMap>, cursor: Option<&str>) -> ScanResponse {
    let count = u32::try_from(items.len()).unwrap();
    ScanResponse {
        count,
        scanned_count: count,
        items: Some(items),
        last_evaluated_key: cursor.map(key_attributes),
        consumed_capacity: None,
    }
}

pub(crate) fn key_attributes(key: &str) -> KeyAttributes {
    KeyAttributes::from([("user_id".to_string(), AttributeValue::S(key.to_string()))])
}

pub(crate) fn item(
    key: &str,
    email: &str,
    org_id: &str,
    source_position: &str,
    contains_private_data: bool,
) -> AttributeMap {
    let mut item = AttributeMap::new();
    item.insert("user_id", AttributeValue::S(key.to_string()));
    item.insert("email", AttributeValue::S(email.to_string()));
    item.insert("org_id", AttributeValue::S(org_id.to_string()));
    item.insert(
        "__source_position",
        AttributeValue::N(source_position.to_string()),
    );
    item.insert(
        "contains_private_data",
        AttributeValue::BOOL(contains_private_data),
    );
    item
}

pub(crate) fn dynamodb_response(
    items: Vec<BTreeMap<String, DynamoDbAttributeValue>>,
    cursor: Option<BTreeMap<String, DynamoDbAttributeValue>>,
) -> DynamoDbCurrentRowScanResponse {
    DynamoDbCurrentRowScanResponse {
        items,
        last_evaluated_key: cursor,
    }
}

pub(crate) fn dynamodb_item(
    key: &str,
    email: &str,
    org_id: &str,
    source_position: &str,
    contains_private_data: bool,
) -> BTreeMap<String, DynamoDbAttributeValue> {
    BTreeMap::from([
        (
            "user_id".to_string(),
            DynamoDbAttributeValue::S(key.to_string()),
        ),
        (
            "email".to_string(),
            DynamoDbAttributeValue::S(email.to_string()),
        ),
        (
            "org_id".to_string(),
            DynamoDbAttributeValue::S(org_id.to_string()),
        ),
        (
            "__source_position".to_string(),
            DynamoDbAttributeValue::N(source_position.to_string()),
        ),
        (
            "contains_private_data".to_string(),
            DynamoDbAttributeValue::Bool(contains_private_data),
        ),
    ])
}

pub(crate) fn dynamodb_key(key: &str) -> BTreeMap<String, DynamoDbAttributeValue> {
    BTreeMap::from([(
        "user_id".to_string(),
        DynamoDbAttributeValue::S(key.to_string()),
    )])
}

fn missing_test_response() -> CheckError {
    CheckError::InvalidBackendConfiguration("missing test response".to_string())
}
