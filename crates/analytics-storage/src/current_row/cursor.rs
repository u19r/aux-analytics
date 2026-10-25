use analytics_operations::{CheckError, CheckSourcePosition};
use aws_sdk_dynamodb::types::AttributeValue as DynamoDbAttributeValue;
use serde::{Deserialize, Serialize, ser::SerializeMap};
use storage_types::{ExclusiveStartKey, KeyAttributes, VecMap};

pub(super) fn dynamodb_cursor_from_key(
    key: &VecMap<String, DynamoDbAttributeValue>,
) -> Result<String, CheckError> {
    let mut values = key
        .iter()
        .map(|(name, value)| Ok((name.clone(), dynamodb_cursor_value(value)?)))
        .collect::<Result<Vec<_>, CheckError>>()?;
    values.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    serde_json::to_string(&SortedCursor(&values)).map_err(CheckError::from)
}

pub(super) fn aux_storage_cursor_from_key(key: &KeyAttributes) -> Result<String, CheckError> {
    key.canonical_dynamo_json().map_err(|error| {
        CheckError::InvalidBackendConfiguration(format!(
            "aux-storage current-row cursor cannot encode key attributes: {error}"
        ))
    })
}

pub(super) fn aux_storage_cursor_to_exclusive_start_key(cursor: &str) -> ExclusiveStartKey {
    serde_json::from_str::<KeyAttributes>(cursor)
        .map(ExclusiveStartKey::Key)
        .unwrap_or_else(|_| ExclusiveStartKey::Token(cursor.to_string()))
}

/// Keep SDK attribute values in the small ordered map until the SDK boundary;
/// `KeyAttributes` carries storage values and would add an avoidable value map.
pub(super) fn dynamodb_cursor_to_key(
    cursor: &str,
) -> Result<VecMap<String, DynamoDbAttributeValue>, CheckError> {
    serde_json::from_str::<VecMap<String, CursorAttributeValue>>(cursor)
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
        DynamoDbAttributeValue::B(value) => CursorAttributeValue::B(value.as_ref().to_vec()),
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
    B(Vec<u8>),
}

struct SortedCursor<'a>(&'a [(String, CursorAttributeValue)]);

impl Serialize for SortedCursor<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (name, value) in self.0 {
            map.serialize_entry(name, value)?;
        }
        map.end()
    }
}

impl CursorAttributeValue {
    fn into_dynamodb_attribute(self) -> DynamoDbAttributeValue {
        match self {
            Self::S(value) => DynamoDbAttributeValue::S(value),
            Self::N(value) => DynamoDbAttributeValue::N(value),
            Self::B(value) => DynamoDbAttributeValue::B(value.into()),
        }
    }
}

pub(super) fn parse_source_position(value: &str) -> Result<CheckSourcePosition, CheckError> {
    CheckSourcePosition::parse_source_position(value).map_err(|error| {
        CheckError::InvalidBackendConfiguration(format!(
            "current-row source position is invalid: {error}"
        ))
    })
}
