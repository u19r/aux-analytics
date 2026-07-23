use std::collections::BTreeMap;

use analytics_operations::{
    CheckError, CurrentRowPageRequest, CurrentRowStreamOrder, ProductionCurrentRowPageReader,
    ProjectedCurrentRow, ProjectedCurrentRowPage,
};
use aws_sdk_dynamodb::types::AttributeValue as DynamoDbAttributeValue;
use serde::{Deserialize, Serialize};
use storage_types::{
    AttributeMap, AttributeValue, ExclusiveStartKey, KeyAttributes, ScanRequest, ScanResponse,
    TableName,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuxStorageCurrentRowScanRequest {
    pub table_name: String,
    pub projection_expression: String,
    pub limit: u32,
    pub exclusive_start_key: Option<ExclusiveStartKey>,
}

pub trait AuxStorageCurrentRowClient {
    fn scan_current_rows(
        &self,
        request: &AuxStorageCurrentRowScanRequest,
    ) -> Result<ScanResponse, CheckError>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct DynamoDbCurrentRowScanRequest {
    pub table_name: String,
    pub projection_expression: String,
    pub limit: i32,
    pub exclusive_start_key: Option<BTreeMap<String, DynamoDbAttributeValue>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DynamoDbCurrentRowScanResponse {
    pub items: Vec<BTreeMap<String, DynamoDbAttributeValue>>,
    pub last_evaluated_key: Option<BTreeMap<String, DynamoDbAttributeValue>>,
}

pub trait DynamoDbCurrentRowClient {
    fn scan_current_rows(
        &self,
        request: &DynamoDbCurrentRowScanRequest,
    ) -> Result<DynamoDbCurrentRowScanResponse, CheckError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuxStorageCurrentRowPageReader<Client> {
    client: Client,
    stream_order: CurrentRowStreamOrder,
    partition_column: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynamoDbCurrentRowPageReader<Client> {
    client: Client,
    stream_order: CurrentRowStreamOrder,
    partition_column: Option<String>,
}

impl<Client> AuxStorageCurrentRowPageReader<Client> {
    #[must_use]
    pub fn new(client: Client) -> Self {
        Self {
            client,
            stream_order: CurrentRowStreamOrder::Unordered,
            partition_column: None,
        }
    }

    #[must_use]
    pub fn new_globally_sorted(client: Client) -> Self {
        Self {
            client,
            stream_order: CurrentRowStreamOrder::GloballySortedByKey,
            partition_column: None,
        }
    }

    #[must_use]
    pub fn new_partition_sorted(client: Client, partition_column: impl Into<String>) -> Self {
        Self {
            client,
            stream_order: CurrentRowStreamOrder::PartitionSortedByKey,
            partition_column: Some(partition_column.into()),
        }
    }
}

impl<Client> DynamoDbCurrentRowPageReader<Client> {
    #[must_use]
    pub fn new(client: Client) -> Self {
        Self {
            client,
            stream_order: CurrentRowStreamOrder::Unordered,
            partition_column: None,
        }
    }

    #[must_use]
    pub fn new_globally_sorted(client: Client) -> Self {
        Self {
            client,
            stream_order: CurrentRowStreamOrder::GloballySortedByKey,
            partition_column: None,
        }
    }

    #[must_use]
    pub fn new_partition_sorted(client: Client, partition_column: impl Into<String>) -> Self {
        Self {
            client,
            stream_order: CurrentRowStreamOrder::PartitionSortedByKey,
            partition_column: Some(partition_column.into()),
        }
    }
}

impl<Client> ProductionCurrentRowPageReader for AuxStorageCurrentRowPageReader<Client>
where
    Client: AuxStorageCurrentRowClient,
{
    fn current_row_page(
        &self,
        request: &CurrentRowPageRequest,
    ) -> Result<ProjectedCurrentRowPage, CheckError> {
        let scan_request = aux_storage_scan_request(request, self.partition_column.as_deref())?;
        let response = self.client.scan_current_rows(&scan_request)?;
        let items = response.items.unwrap_or_default();
        let partition = aux_storage_page_partition(
            &items,
            self.partition_column.as_deref(),
            &request.projection.table_name,
        )?;
        let rows = items
            .iter()
            .map(|item| projected_row_from_item(request, item))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ProjectedCurrentRowPage {
            rows,
            next_cursor: response
                .last_evaluated_key
                .as_ref()
                .map(aux_storage_cursor_from_key)
                .transpose()?,
            partition,
        })
    }

    fn stream_order(&self) -> CurrentRowStreamOrder {
        self.stream_order
    }
}

impl<Client> ProductionCurrentRowPageReader for DynamoDbCurrentRowPageReader<Client>
where
    Client: DynamoDbCurrentRowClient,
{
    fn current_row_page(
        &self,
        request: &CurrentRowPageRequest,
    ) -> Result<ProjectedCurrentRowPage, CheckError> {
        let scan_request = dynamodb_scan_request(request, self.partition_column.as_deref())?;
        let response = self.client.scan_current_rows(&scan_request)?;
        let partition = dynamodb_page_partition(
            &response.items,
            self.partition_column.as_deref(),
            &request.projection.table_name,
        )?;
        let rows = response
            .items
            .iter()
            .map(|item| projected_row_from_dynamodb_item(request, item))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ProjectedCurrentRowPage {
            rows,
            next_cursor: response
                .last_evaluated_key
                .as_ref()
                .map(dynamodb_cursor_from_key)
                .transpose()?,
            partition,
        })
    }

    fn stream_order(&self) -> CurrentRowStreamOrder {
        self.stream_order
    }
}

fn aux_storage_scan_request(
    request: &CurrentRowPageRequest,
    partition_column: Option<&str>,
) -> Result<AuxStorageCurrentRowScanRequest, CheckError> {
    let limit = u32::try_from(request.page_size).map_err(|_| {
        CheckError::InvalidBackendConfiguration(
            "aux-storage current-row page size exceeds u32 range".to_string(),
        )
    })?;
    if limit == 0 {
        return Err(CheckError::InvalidBackendConfiguration(
            "aux-storage current-row page size must be greater than zero".to_string(),
        ));
    }
    let scan_request = ScanRequest::new(TableName::new(&request.projection.table_name))
        .with_limit(Some(limit))
        .with_exclusive_start_key(request.cursor.clone());
    Ok(AuxStorageCurrentRowScanRequest {
        table_name: scan_request.table_name.to_string(),
        projection_expression: projection_expression(request, partition_column),
        limit,
        exclusive_start_key: request
            .cursor
            .as_deref()
            .map(aux_storage_cursor_to_exclusive_start_key),
    })
}

fn dynamodb_scan_request(
    request: &CurrentRowPageRequest,
    partition_column: Option<&str>,
) -> Result<DynamoDbCurrentRowScanRequest, CheckError> {
    let limit = i32::try_from(request.page_size).map_err(|_| {
        CheckError::InvalidBackendConfiguration(
            "DynamoDB current-row page size exceeds i32 range".to_string(),
        )
    })?;
    if limit == 0 {
        return Err(CheckError::InvalidBackendConfiguration(
            "DynamoDB current-row page size must be greater than zero".to_string(),
        ));
    }
    Ok(DynamoDbCurrentRowScanRequest {
        table_name: request.projection.table_name.clone(),
        projection_expression: projection_expression(request, partition_column),
        limit,
        exclusive_start_key: request
            .cursor
            .as_deref()
            .map(dynamodb_cursor_to_key)
            .transpose()?,
    })
}

fn projection_expression(
    request: &CurrentRowPageRequest,
    partition_column: Option<&str>,
) -> String {
    let mut columns = vec![request.projection.key_column.clone()];
    columns.extend(request.projection.hash_columns.iter().cloned());
    if let Some(column) = partition_column {
        columns.push(column.to_string());
    }
    if let Some(column) = request.projection.source_position_column.as_ref() {
        columns.push(column.clone());
    }
    if let Some(column) = request.projection.private_data_column.as_ref() {
        columns.push(column.clone());
    }
    columns.sort();
    columns.dedup();
    columns.join(", ")
}

fn projected_row_from_item(
    request: &CurrentRowPageRequest,
    item: &AttributeMap,
) -> Result<ProjectedCurrentRow, CheckError> {
    let key = string_attribute(item, &request.projection.key_column)?;
    let values = request
        .projection
        .hash_columns
        .iter()
        .map(|column| Ok((column.clone(), projected_value_attribute(item, column)?)))
        .collect::<Result<BTreeMap<_, _>, CheckError>>()?;
    let source_position = if let Some(column) = request.projection.source_position_column.as_deref()
    {
        optional_string_attribute(item, column)?
            .as_deref()
            .map(parse_source_position)
            .transpose()?
            .unwrap_or_default()
    } else {
        0
    };
    let contains_private_data = request
        .projection
        .private_data_column
        .as_deref()
        .and_then(|column| item.get(column))
        .map(bool_attribute)
        .transpose()?
        .unwrap_or(false);
    Ok(ProjectedCurrentRow::new(key, values, source_position)
        .with_private_data(contains_private_data))
}

fn projected_row_from_dynamodb_item(
    request: &CurrentRowPageRequest,
    item: &BTreeMap<String, DynamoDbAttributeValue>,
) -> Result<ProjectedCurrentRow, CheckError> {
    let key = dynamodb_string_attribute(item, &request.projection.key_column)?;
    let values = request
        .projection
        .hash_columns
        .iter()
        .map(|column| {
            Ok((
                column.clone(),
                Some(dynamodb_string_attribute(item, column)?),
            ))
        })
        .collect::<Result<BTreeMap<_, _>, CheckError>>()?;
    let source_position = if let Some(column) = request.projection.source_position_column.as_deref()
    {
        item.get(column)
            .map(dynamodb_attribute_to_string)
            .transpose()?
            .as_deref()
            .map(parse_source_position)
            .transpose()?
            .unwrap_or_default()
    } else {
        0
    };
    let contains_private_data = request
        .projection
        .private_data_column
        .as_deref()
        .and_then(|column| item.get(column))
        .map(dynamodb_bool_attribute)
        .transpose()?
        .unwrap_or(false);
    Ok(ProjectedCurrentRow::new(key, values, source_position)
        .with_private_data(contains_private_data))
}

fn aux_storage_page_partition(
    items: &[AttributeMap],
    partition_column: Option<&str>,
    table_name: &str,
) -> Result<Option<String>, CheckError> {
    let Some(partition_column) = partition_column else {
        return Ok(None);
    };
    page_partition(
        items
            .iter()
            .map(|item| string_attribute(item, partition_column)),
        partition_column,
        table_name,
    )
}

fn dynamodb_page_partition(
    items: &[BTreeMap<String, DynamoDbAttributeValue>],
    partition_column: Option<&str>,
    table_name: &str,
) -> Result<Option<String>, CheckError> {
    let Some(partition_column) = partition_column else {
        return Ok(None);
    };
    page_partition(
        items
            .iter()
            .map(|item| dynamodb_string_attribute(item, partition_column)),
        partition_column,
        table_name,
    )
}

fn page_partition(
    partitions: impl IntoIterator<Item = Result<String, CheckError>>,
    partition_column: &str,
    table_name: &str,
) -> Result<Option<String>, CheckError> {
    let mut partition = None;
    for value in partitions {
        let value = value?;
        match partition.as_ref() {
            Some(existing) if existing != &value => {
                return Err(CheckError::InvalidBackendConfiguration(format!(
                    "partition-sorted current-row response for {table_name} returned multiple \
                     {partition_column} values in one page"
                )));
            }
            Some(_) => {}
            None => partition = Some(value),
        }
    }
    Ok(partition)
}

fn string_attribute(item: &AttributeMap, column: &str) -> Result<String, CheckError> {
    optional_string_attribute(item, column)?.ok_or_else(|| missing_projected_column(column))
}

fn projected_value_attribute(
    item: &AttributeMap,
    column: &str,
) -> Result<Option<String>, CheckError> {
    item.get(column)
        .map(storage_attribute_to_string)
        .transpose()
        .and_then(|value| {
            value.map_or_else(
                || Err(missing_projected_column(column)),
                |value| Ok(Some(value)),
            )
        })
}

fn optional_string_attribute(
    item: &AttributeMap,
    column: &str,
) -> Result<Option<String>, CheckError> {
    item.get(column)
        .map(storage_attribute_to_string)
        .transpose()
}

fn missing_projected_column(column: &str) -> CheckError {
    CheckError::InvalidBackendConfiguration(format!(
        "aux-storage current-row response missing projected column {column}"
    ))
}

fn storage_attribute_to_string(value: &AttributeValue) -> Result<String, CheckError> {
    Ok(match value {
        AttributeValue::S(value) | AttributeValue::N(value) | AttributeValue::B(value) => {
            value.clone()
        }
        AttributeValue::BOOL(value) => value.to_string(),
        AttributeValue::NULL(true) => return Ok(String::new()),
        other => {
            return Err(CheckError::InvalidBackendConfiguration(format!(
                "aux-storage current-row projected scalar cannot use attribute variant {other:?}"
            )));
        }
    })
}

fn bool_attribute(value: &AttributeValue) -> Result<bool, CheckError> {
    match value {
        AttributeValue::BOOL(value) => Ok(*value),
        AttributeValue::S(value) if value == "true" => Ok(true),
        AttributeValue::S(value) if value == "false" => Ok(false),
        other => Err(CheckError::InvalidBackendConfiguration(format!(
            "aux-storage current-row private-data column must be BOOL or boolean string, got \
             {other:?}"
        ))),
    }
}

fn dynamodb_string_attribute(
    item: &BTreeMap<String, DynamoDbAttributeValue>,
    column: &str,
) -> Result<String, CheckError> {
    item.get(column)
        .map(dynamodb_attribute_to_string)
        .transpose()?
        .ok_or_else(|| missing_projected_column(column))
}

fn dynamodb_attribute_to_string(value: &DynamoDbAttributeValue) -> Result<String, CheckError> {
    Ok(match value {
        DynamoDbAttributeValue::S(value) | DynamoDbAttributeValue::N(value) => value.clone(),
        DynamoDbAttributeValue::B(value) => String::from_utf8_lossy(value.as_ref()).to_string(),
        DynamoDbAttributeValue::Bool(value) => value.to_string(),
        DynamoDbAttributeValue::Null(true) => String::new(),
        other => {
            return Err(CheckError::InvalidBackendConfiguration(format!(
                "DynamoDB current-row projected scalar cannot use attribute variant {other:?}"
            )));
        }
    })
}

fn dynamodb_bool_attribute(value: &DynamoDbAttributeValue) -> Result<bool, CheckError> {
    match value {
        DynamoDbAttributeValue::Bool(value) => Ok(*value),
        DynamoDbAttributeValue::S(value) if value == "true" => Ok(true),
        DynamoDbAttributeValue::S(value) if value == "false" => Ok(false),
        other => Err(CheckError::InvalidBackendConfiguration(format!(
            "DynamoDB current-row private-data column must be BOOL or boolean string, got \
             {other:?}"
        ))),
    }
}

fn dynamodb_cursor_from_key(
    key: &BTreeMap<String, DynamoDbAttributeValue>,
) -> Result<String, CheckError> {
    serde_json::to_string(
        &key.iter()
            .map(|(name, value)| Ok((name.clone(), dynamodb_cursor_value(value)?)))
            .collect::<Result<BTreeMap<_, _>, CheckError>>()?,
    )
    .map_err(CheckError::from)
}

fn aux_storage_cursor_from_key(key: &KeyAttributes) -> Result<String, CheckError> {
    key.canonical_dynamo_json().map_err(|error| {
        CheckError::InvalidBackendConfiguration(format!(
            "aux-storage current-row cursor cannot encode key attributes: {error}"
        ))
    })
}

fn aux_storage_cursor_to_exclusive_start_key(cursor: &str) -> ExclusiveStartKey {
    serde_json::from_str::<KeyAttributes>(cursor)
        .map(ExclusiveStartKey::Key)
        .unwrap_or_else(|_| ExclusiveStartKey::Token(cursor.to_string()))
}

fn dynamodb_cursor_to_key(
    cursor: &str,
) -> Result<BTreeMap<String, DynamoDbAttributeValue>, CheckError> {
    serde_json::from_str::<BTreeMap<String, CursorAttributeValue>>(cursor)
        .map_err(CheckError::from)?
        .into_iter()
        .map(|(name, value)| Ok((name, value.into_dynamodb_attribute())))
        .collect()
}

fn dynamodb_cursor_value(
    value: &DynamoDbAttributeValue,
) -> Result<CursorAttributeValue, CheckError> {
    Ok(match value {
        DynamoDbAttributeValue::S(value) => CursorAttributeValue::S(value.clone()),
        DynamoDbAttributeValue::N(value) => CursorAttributeValue::N(value.clone()),
        DynamoDbAttributeValue::B(value) => {
            CursorAttributeValue::B(String::from_utf8_lossy(value.as_ref()).to_string())
        }
        other => {
            return Err(CheckError::InvalidBackendConfiguration(format!(
                "DynamoDB current-row cursor cannot encode attribute variant {other:?}"
            )));
        }
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
enum CursorAttributeValue {
    S(String),
    N(String),
    B(String),
}

impl CursorAttributeValue {
    fn into_dynamodb_attribute(self) -> DynamoDbAttributeValue {
        match self {
            Self::S(value) => DynamoDbAttributeValue::S(value),
            Self::N(value) => DynamoDbAttributeValue::N(value),
            Self::B(value) => DynamoDbAttributeValue::B(value.into_bytes().into()),
        }
    }
}

fn parse_source_position(value: &str) -> Result<u64, CheckError> {
    value.parse::<u64>().map_err(|_| {
        CheckError::InvalidBackendConfiguration(format!(
            "aux-storage current-row source position must be a numeric string, got {value}"
        ))
    })
}
