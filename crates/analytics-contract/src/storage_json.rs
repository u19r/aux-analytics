use serde::{Serialize, Serializer, ser::SerializeMap};

use super::{StorageItem, StorageValue, StorageValueJsonError};

impl StorageValue {
    pub fn to_json_value(&self) -> Result<serde_json::Value, StorageValueJsonError> {
        serde_json::to_value(JsonValue(self))
            .map_err(|_| StorageValueJsonError::UnsupportedNullFalse)
    }

    /// Serializes the plain JSON representation without constructing a JSON
    /// tree.
    pub fn to_json_text(&self) -> Result<String, StorageValueJsonError> {
        serde_json::to_string(&JsonValue(self))
            .map_err(|_| StorageValueJsonError::UnsupportedNullFalse)
    }

    /// Appends plain JSON to an existing buffer. Only NULL(false) is
    /// unsupported. On error the caller must discard the partially appended
    /// representation.
    pub fn append_json(&self, output: &mut Vec<u8>) -> Result<(), StorageValueJsonError> {
        serde_json::to_writer(output, &JsonValue(self))
            .map_err(|_| StorageValueJsonError::UnsupportedNullFalse)
    }
}

struct JsonValue<'value>(&'value StorageValue);
struct JsonItem<'item>(&'item StorageItem);
struct JsonNumber<'number>(&'number str);

impl Serialize for JsonValue<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            StorageValue::S(value) | StorageValue::B(value) => serializer.serialize_str(value),
            StorageValue::N(value) => JsonNumber(value).serialize(serializer),
            StorageValue::SS(values) | StorageValue::BS(values) => values.serialize(serializer),
            StorageValue::NS(values) => {
                serializer.collect_seq(values.iter().map(|value| JsonNumber(value)))
            }
            StorageValue::BOOL(value) => serializer.serialize_bool(*value),
            StorageValue::NULL(value) => serialize_null(*value, serializer),
            StorageValue::L(values) => serializer.collect_seq(values.iter().map(JsonValue)),
            StorageValue::M(values) => JsonItem(values).serialize(serializer),
        }
    }
}

impl Serialize for JsonItem<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut entries = self.0.iter().collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(key, _)| *key);
        let mut map = serializer.serialize_map(Some(entries.len()))?;
        for (key, value) in entries {
            map.serialize_entry(key, &JsonValue(value))?;
        }
        map.end()
    }
}

impl Serialize for JsonNumber<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0.parse::<serde_json::Number>() {
            Ok(number) => number.serialize(serializer),
            Err(_) => serializer.serialize_str(self.0),
        }
    }
}

fn serialize_null<S: Serializer>(value: bool, serializer: S) -> Result<S::Ok, S::Error> {
    if value {
        return serializer.serialize_unit();
    }
    Err(serde::ser::Error::custom(
        StorageValueJsonError::UnsupportedNullFalse,
    ))
}
