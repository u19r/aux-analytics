use std::collections::{BTreeMap, HashMap};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub type StorageItem = HashMap<String, StorageValue>;

/// Domain-agnostic storage stream record accepted by the analytics ingestion
/// API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "PascalCase")]
#[schema(example = json!({
    "Keys": {"pk": {"S": "USER#user-1"}},
    "SequenceNumber": "49668899999999999999999999999999999999999999999999999999",
    "OldImage": null,
    "NewImage": {
        "pk": {"S": "USER#user-1"},
        "profile": {"M": {"email": {"S": "ada@example.com"}}},
        "org_id": {"S": "org-a"}
    }
}))]
pub struct StorageStreamRecord {
    /// Source storage key attributes for the record.
    #[schema(value_type = Object)]
    pub keys: StorageItem,
    /// Source stream sequence number or equivalent monotonically ordered
    /// position.
    #[schema(
        min_length = 1,
        max_length = 1024,
        example = "49668899999999999999999999999999999999999999999999999999"
    )]
    pub sequence_number: String,
    /// Previous source image for update and delete events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Object, nullable = true, default = json!(null), example = json!({
        "pk": {"S": "USER#user-1"},
        "profile": {"M": {"email": {"S": "old@example.com"}}}
    }))]
    pub old_image: Option<StorageItem>,
    /// New source image for insert and update events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Object, nullable = true, default = json!(null), example = json!({
        "pk": {"S": "USER#user-1"},
        "profile": {"M": {"email": {"S": "ada@example.com"}}},
        "org_id": {"S": "org-a"}
    }))]
    pub new_image: Option<StorageItem>,
}

/// DynamoDB-compatible attribute value wire shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum StorageValue {
    /// String scalar.
    S(String),
    /// Number encoded as a string to preserve exact source representation.
    N(String),
    /// Base64-encoded binary scalar.
    B(String),
    /// String set.
    SS(Vec<String>),
    /// Number set encoded as strings.
    NS(Vec<String>),
    /// Binary set encoded as base64 strings.
    BS(Vec<String>),
    /// Boolean scalar.
    BOOL(bool),
    /// Null marker. Only true represents JSON null.
    NULL(bool),
    /// List attribute.
    L(Vec<StorageValue>),
    /// Map attribute.
    M(StorageItem),
}

impl StorageValue {
    #[must_use]
    pub const fn variant_name(&self) -> &'static str {
        match self {
            Self::S(_) => "S",
            Self::N(_) => "N",
            Self::B(_) => "B",
            Self::SS(_) => "SS",
            Self::NS(_) => "NS",
            Self::BS(_) => "BS",
            Self::BOOL(_) => "BOOL",
            Self::NULL(_) => "NULL",
            Self::L(_) => "L",
            Self::M(_) => "M",
        }
    }

    pub fn to_json_value(&self) -> Result<serde_json::Value, StorageValueJsonError> {
        match self {
            Self::S(value) | Self::B(value) => Ok(serde_json::Value::String(value.clone())),
            Self::N(value) => Ok(value.parse::<serde_json::Number>().map_or_else(
                |_| serde_json::Value::String(value.clone()),
                serde_json::Value::Number,
            )),
            Self::SS(values) | Self::BS(values) => Ok(serde_json::Value::Array(
                values
                    .iter()
                    .map(|value| serde_json::Value::String(value.clone()))
                    .collect(),
            )),
            Self::NS(values) => Ok(serde_json::Value::Array(
                values
                    .iter()
                    .map(|value| {
                        value.parse::<serde_json::Number>().map_or_else(
                            |_| serde_json::Value::String(value.clone()),
                            serde_json::Value::Number,
                        )
                    })
                    .collect(),
            )),
            Self::BOOL(value) => Ok(serde_json::Value::Bool(*value)),
            Self::NULL(true) => Ok(serde_json::Value::Null),
            Self::NULL(false) => Err(StorageValueJsonError::UnsupportedNullFalse),
            Self::L(values) => {
                let mut out = Vec::with_capacity(values.len());
                for value in values {
                    out.push(value.to_json_value()?);
                }
                Ok(serde_json::Value::Array(out))
            }
            Self::M(values) => {
                let mut out = serde_json::Map::with_capacity(values.len());
                let ordered = values.iter().collect::<BTreeMap<_, _>>();
                for (key, value) in ordered {
                    out.insert(key.clone(), value.to_json_value()?);
                }
                Ok(serde_json::Value::Object(out))
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageValueJsonError {
    UnsupportedNullFalse,
}

impl std::fmt::Display for StorageValueJsonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedNullFalse => write!(f, "NULL(false) cannot be converted to JSON"),
        }
    }
}

impl std::error::Error for StorageValueJsonError {}
