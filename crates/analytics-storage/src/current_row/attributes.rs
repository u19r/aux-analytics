use analytics_operations::CheckError;
use aws_sdk_dynamodb::types::AttributeValue as DynamoDbAttributeValue;
use storage_types::{AttributeMap, AttributeValue, VecMap};

pub(super) fn string_attribute(item: &AttributeMap, column: &str) -> Result<String, CheckError> {
    optional_string_attribute(item, column)?.ok_or_else(|| missing_projected_column(column))
}

pub(super) fn optional_string_attribute(
    item: &AttributeMap,
    column: &str,
) -> Result<Option<String>, CheckError> {
    item.get(column)
        .map(storage_attribute_to_string)
        .transpose()
}

pub(super) fn missing_projected_column(column: &str) -> CheckError {
    CheckError::InvalidBackendConfiguration(format!(
        "aux-storage current-row response missing projected column {column}"
    ))
}

pub(super) fn storage_attribute_to_string(value: &AttributeValue) -> Result<String, CheckError> {
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

pub(super) fn take_string_attribute(
    item: &mut AttributeMap,
    column: &str,
) -> Result<String, CheckError> {
    let value = item
        .remove(column)
        .ok_or_else(|| missing_projected_column(column))?;
    match value {
        AttributeValue::S(value) | AttributeValue::N(value) | AttributeValue::B(value) => Ok(value),
        other => storage_attribute_to_string(&other),
    }
}

pub(super) fn bool_attribute(value: &AttributeValue) -> Result<bool, CheckError> {
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

pub(super) fn dynamodb_string_attribute(
    item: &VecMap<String, DynamoDbAttributeValue>,
    column: &str,
) -> Result<String, CheckError> {
    item.get(column)
        .map(dynamodb_attribute_to_string)
        .transpose()?
        .ok_or_else(|| missing_projected_column(column))
}

pub(super) fn dynamodb_attribute_to_string(
    value: &DynamoDbAttributeValue,
) -> Result<String, CheckError> {
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

pub(super) fn dynamodb_bool_attribute(value: &DynamoDbAttributeValue) -> Result<bool, CheckError> {
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
