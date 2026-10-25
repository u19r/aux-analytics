use analytics_contract::{StorageItem, StorageStreamRecord, StorageValue};
use storage_types::{AttributeMap, AttributeValue, StreamRecord};

#[must_use]
pub(crate) fn convert_stream_record(record: StreamRecord) -> StorageStreamRecord {
    StorageStreamRecord {
        keys: convert_wire_attributes(
            record
                .keys
                .into_iter()
                .map(|attribute| (attribute.name, attribute.value)),
        ),
        sequence_number: record.sequence_number,
        old_image: record.old_image.map(convert_attribute_map),
        new_image: record.new_image.map(convert_attribute_map),
    }
}

#[must_use]
fn convert_attribute_map(item: AttributeMap) -> StorageItem {
    convert_wire_attributes(
        item.into_iter()
            .map(|attribute| (attribute.name, attribute.value)),
    )
}

#[must_use]
fn convert_wire_attributes(
    item: impl IntoIterator<Item = (String, AttributeValue)>,
) -> StorageItem {
    item.into_iter()
        .map(|(name, value)| (name, convert_wire_value(value)))
        .collect()
}

#[must_use]
fn convert_wire_value(value: AttributeValue) -> StorageValue {
    match value {
        AttributeValue::S(value) => StorageValue::S(value),
        AttributeValue::N(value) => StorageValue::N(value),
        AttributeValue::B(value) => StorageValue::B(value),
        AttributeValue::SS(value) => StorageValue::SS(value),
        AttributeValue::NS(value) => StorageValue::NS(value),
        AttributeValue::BS(value) => StorageValue::BS(value),
        AttributeValue::BOOL(value) => StorageValue::BOOL(value),
        AttributeValue::NULL(value) => StorageValue::NULL(value),
        AttributeValue::L(values) => StorageValue::L(
            values
                .into_iter()
                .map(convert_wire_value)
                .collect::<Vec<_>>(),
        ),
        AttributeValue::M(values) => StorageValue::M(convert_wire_attributes(values)),
    }
}
