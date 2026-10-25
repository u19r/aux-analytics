use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use storage_types::VecMap;
use utoipa::ToSchema;

/// Typed attributes support tenant selection, projection, privacy filtering and
/// retention lookup without reparsing an encoded document. Encode to bytes only
/// at transport or persisted-payload boundaries.
pub type StorageItem = VecMap<String, StorageValue>;

/// Domain-agnostic storage stream record accepted by the analytics ingestion
/// API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "PascalCase")]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
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
    #[schemars(with = "HashMap<String, StorageValue>")]
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
    #[schemars(with = "Option<HashMap<String, StorageValue>>")]
    #[schema(value_type = Option<std::collections::BTreeMap<String, serde_json::Value>>, nullable = true, default = json!(null), example = json!({
        "pk": {"S": "USER#user-1"},
        "profile": {"M": {"email": {"S": "old@example.com"}}}
    }))]
    pub old_image: Option<StorageItem>,
    /// New source image for insert and update events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<HashMap<String, StorageValue>>")]
    #[schema(value_type = Option<std::collections::BTreeMap<String, serde_json::Value>>, nullable = true, default = json!(null), example = json!({
        "pk": {"S": "USER#user-1"},
        "profile": {"M": {"email": {"S": "ada@example.com"}}},
        "org_id": {"S": "org-a"}
    }))]
    pub new_image: Option<StorageItem>,
}

impl StorageStreamRecord {
    /// Writes the canonical JSON bytes used by source-batch and source-effect
    /// identities. Map keys are sorted recursively while list order and image
    /// presence retain their wire-contract meaning.
    pub fn write_canonical_json(&self, output: &mut Vec<u8>) {
        output.push(b'{');
        write_object_field(output, b"Keys");
        write_storage_item(&self.keys, output);
        if let Some(new_image) = self.new_image.as_ref() {
            output.push(b',');
            write_object_field(output, b"NewImage");
            write_storage_item(new_image, output);
        }
        if let Some(old_image) = self.old_image.as_ref() {
            output.push(b',');
            write_object_field(output, b"OldImage");
            write_storage_item(old_image, output);
        }
        output.push(b',');
        write_object_field(output, b"SequenceNumber");
        write_json_string(output, &self.sequence_number);
        output.push(b'}');
    }
}

fn write_storage_item(item: &StorageItem, output: &mut Vec<u8>) {
    let mut entries = item.iter().collect::<Vec<_>>();
    entries.sort_unstable_by_key(|(key, _)| *key);
    output.push(b'{');
    for (index, (key, value)) in entries.into_iter().enumerate() {
        if index != 0 {
            output.push(b',');
        }
        write_json_string(output, key);
        output.push(b':');
        write_storage_value(value, output);
    }
    output.push(b'}');
}

fn write_storage_value(value: &StorageValue, output: &mut Vec<u8>) {
    match value {
        StorageValue::S(value) => write_tagged_string(output, b"S", value),
        StorageValue::N(value) => write_tagged_string(output, b"N", value),
        StorageValue::B(value) => write_tagged_string(output, b"B", value),
        StorageValue::SS(values) => write_tagged_string_array(output, b"SS", values),
        StorageValue::NS(values) => write_tagged_string_array(output, b"NS", values),
        StorageValue::BS(values) => write_tagged_string_array(output, b"BS", values),
        StorageValue::BOOL(value) => write_tagged_bool(output, b"BOOL", *value),
        StorageValue::NULL(value) => write_tagged_bool(output, b"NULL", *value),
        StorageValue::L(values) => write_tagged_list(output, values),
        StorageValue::M(values) => {
            output.push(b'{');
            write_object_field(output, b"M");
            write_storage_item(values, output);
            output.push(b'}');
        }
    }
}

fn write_tagged_string(output: &mut Vec<u8>, tag: &[u8], value: &str) {
    output.push(b'{');
    write_object_field(output, tag);
    write_json_string(output, value);
    output.push(b'}');
}

fn write_tagged_string_array(output: &mut Vec<u8>, tag: &[u8], values: &[String]) {
    output.push(b'{');
    write_object_field(output, tag);
    output.push(b'[');
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            output.push(b',');
        }
        write_json_string(output, value);
    }
    output.extend_from_slice(b"]}");
}

fn write_tagged_bool(output: &mut Vec<u8>, tag: &[u8], value: bool) {
    output.push(b'{');
    write_object_field(output, tag);
    output.extend_from_slice(if value { b"true" } else { b"false" });
    output.push(b'}');
}

fn write_tagged_list(output: &mut Vec<u8>, values: &[StorageValue]) {
    output.push(b'{');
    write_object_field(output, b"L");
    output.push(b'[');
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            output.push(b',');
        }
        write_storage_value(value, output);
    }
    output.extend_from_slice(b"]}");
}

fn write_object_field(output: &mut Vec<u8>, key: &[u8]) {
    output.push(b'"');
    output.extend_from_slice(key);
    output.extend_from_slice(b"\":");
}

fn write_json_string(output: &mut Vec<u8>, value: &str) {
    serde_json::to_writer(output, value).expect("storage contract strings are serializable");
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
    #[schemars(with = "HashMap<String, StorageValue>")]
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
}
