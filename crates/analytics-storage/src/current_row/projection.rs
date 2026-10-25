use analytics_operations::{
    CheckError, CheckSourcePosition, CurrentRowPageRequest, ProjectedCurrentRow,
};
use aws_sdk_dynamodb::types::AttributeValue as DynamoDbAttributeValue;
use storage_types::{AttributeMap, TableName, VecMap, WireItem};

use super::{
    AuxStorageCurrentRowScanRequest, DynamoDbCurrentRowScanRequest,
    attributes::{
        bool_attribute, dynamodb_bool_attribute, dynamodb_string_attribute,
        optional_string_attribute, string_attribute, take_string_attribute,
    },
    cursor::{
        aux_storage_cursor_to_exclusive_start_key, dynamodb_cursor_to_key, parse_source_position,
    },
};

pub(super) fn aux_storage_scan_request(
    request: &CurrentRowPageRequest,
    partition_column: Option<&str>,
) -> Result<AuxStorageCurrentRowScanRequest, CheckError> {
    let limit = checked_limit(request.page_size, u32::MAX as usize, "aux-storage")?;
    let (projection_expression, expression_attribute_names) =
        scan_projection(request, partition_column);
    Ok(AuxStorageCurrentRowScanRequest {
        table_name: TableName::new(&request.projection.table_name).to_string(),
        projection_expression,
        expression_attribute_names,
        limit,
        exclusive_start_key: request
            .cursor
            .as_deref()
            .map(aux_storage_cursor_to_exclusive_start_key),
    })
}

pub(super) fn dynamodb_scan_request(
    request: &CurrentRowPageRequest,
    partition_column: Option<&str>,
) -> Result<DynamoDbCurrentRowScanRequest, CheckError> {
    let limit = checked_limit(request.page_size, i32::MAX as usize, "DynamoDB")?;
    let (projection_expression, expression_attribute_names) =
        scan_projection(request, partition_column);
    Ok(DynamoDbCurrentRowScanRequest {
        table_name: request.projection.table_name.clone(),
        projection_expression,
        expression_attribute_names,
        limit: i32::try_from(limit).expect("DynamoDB page limit was checked"),
        exclusive_start_key: request
            .cursor
            .as_deref()
            .map(dynamodb_cursor_to_key)
            .transpose()?,
    })
}

fn checked_limit(page_size: usize, maximum: usize, backend: &str) -> Result<u32, CheckError> {
    if page_size == 0 {
        return Err(CheckError::InvalidBackendConfiguration(format!(
            "{backend} current-row page size must be greater than zero"
        )));
    }
    if page_size > maximum {
        return Err(CheckError::InvalidBackendConfiguration(format!(
            "{backend} current-row page size exceeds {} range",
            if maximum == u32::MAX as usize {
                "u32"
            } else {
                "i32"
            }
        )));
    }
    u32::try_from(page_size).map_err(|_| {
        CheckError::InvalidBackendConfiguration(format!(
            "{backend} current-row page size exceeds u32 range"
        ))
    })
}

fn scan_projection(
    request: &CurrentRowPageRequest,
    partition_column: Option<&str>,
) -> (String, VecMap<String, String>) {
    let mut columns = vec![request.projection.key_column.as_str()];
    columns.extend(request.projection.hash_columns.iter().map(String::as_str));
    if let Some(column) = partition_column {
        columns.push(column);
    }
    if let Some(column) = request.projection.source_position_column.as_ref() {
        columns.push(column);
    }
    if let Some(column) = request.projection.private_data_column.as_ref() {
        columns.push(column);
    }
    columns.sort_unstable();
    columns.dedup();
    let names: VecMap<_, _> = columns
        .into_iter()
        .enumerate()
        .map(|(index, column)| (format!("#c{index}"), column.to_owned()))
        .collect();
    (
        names
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join(", "),
        names,
    )
}

pub(super) fn wire_item_to_attribute_map(item: WireItem) -> Result<AttributeMap, CheckError> {
    item.into_attribute_map().map_err(|error| {
        CheckError::InvalidBackendConfiguration(format!(
            "aux-storage current-row response contained an invalid item: {error}"
        ))
    })
}

pub(super) fn projected_row_from_item(
    request: &CurrentRowPageRequest,
    mut item: AttributeMap,
) -> Result<ProjectedCurrentRow, CheckError> {
    let source_position = source_position_from_storage(request, &item)?;
    let contains_private_data = request
        .projection
        .private_data_column
        .as_deref()
        .and_then(|column| item.get(column))
        .map(bool_attribute)
        .transpose()?
        .unwrap_or(false);
    let key_column = &request.projection.key_column;
    let key = if request.projection.hash_columns.contains(key_column) {
        string_attribute(&item, key_column)?
    } else {
        take_string_attribute(&mut item, key_column)?
    };
    let mut values = VecMap::with_capacity(request.projection.hash_columns.len());
    for column in &request.projection.hash_columns {
        if !values.contains_key(column) {
            let value = match item.remove(column) {
                Some(storage_types::AttributeValue::NULL(true)) => None,
                Some(
                    storage_types::AttributeValue::S(value)
                    | storage_types::AttributeValue::N(value)
                    | storage_types::AttributeValue::B(value),
                ) => Some(value),
                Some(value) => Some(super::attributes::storage_attribute_to_string(&value)?),
                None => return Err(super::attributes::missing_projected_column(column)),
            };
            values.insert(column.clone(), value);
        }
    }
    Ok(ProjectedCurrentRow::new(key, values, source_position)
        .with_private_data(contains_private_data))
}

fn source_position_from_storage(
    request: &CurrentRowPageRequest,
    item: &AttributeMap,
) -> Result<CheckSourcePosition, CheckError> {
    let Some(column) = request.projection.source_position_column.as_deref() else {
        return Ok(CheckSourcePosition::default());
    };
    optional_string_attribute(item, column)?
        .as_deref()
        .map(parse_source_position)
        .transpose()
        .map(|position| position.unwrap_or_default())
}

pub(super) fn projected_row_from_dynamodb_item(
    request: &CurrentRowPageRequest,
    item: &VecMap<String, DynamoDbAttributeValue>,
) -> Result<ProjectedCurrentRow, CheckError> {
    let key = dynamodb_string_attribute(item, &request.projection.key_column)?;
    let values = request
        .projection
        .hash_columns
        .iter()
        .map(|column| {
            Ok((
                column.clone(),
                match item.get(column) {
                    Some(DynamoDbAttributeValue::Null(true)) => None,
                    Some(value) => Some(super::attributes::dynamodb_attribute_to_string(value)?),
                    None => return Err(super::attributes::missing_projected_column(column)),
                },
            ))
        })
        .collect::<Result<VecMap<_, _>, CheckError>>()?;
    let source_position = source_position_from_dynamodb(request, item)?;
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

fn source_position_from_dynamodb(
    request: &CurrentRowPageRequest,
    item: &VecMap<String, DynamoDbAttributeValue>,
) -> Result<CheckSourcePosition, CheckError> {
    let Some(column) = request.projection.source_position_column.as_deref() else {
        return Ok(CheckSourcePosition::default());
    };
    item.get(column)
        .map(super::attributes::dynamodb_attribute_to_string)
        .transpose()?
        .as_deref()
        .map(parse_source_position)
        .transpose()
        .map(|position| position.unwrap_or_default())
}

pub(super) fn dynamodb_page_partition(
    items: &[VecMap<String, DynamoDbAttributeValue>],
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
        merge_page_partition(&mut partition, value?, partition_column, table_name)?;
    }
    Ok(partition)
}

pub(super) fn merge_page_partition(
    partition: &mut Option<String>,
    value: String,
    partition_column: &str,
    table_name: &str,
) -> Result<(), CheckError> {
    match partition.as_ref() {
        Some(existing) if existing != &value => {
            return Err(CheckError::InvalidBackendConfiguration(format!(
                "partition-sorted current-row response for {table_name} returned multiple \
                 {partition_column} values in one page"
            )));
        }
        Some(_) => {}
        None => *partition = Some(value),
    }
    Ok(())
}
