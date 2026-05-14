use std::{collections::HashMap, hash::BuildHasher};

use analytics_contract::{StorageItem, StorageStreamRecord, StorageValue};
use storage_types::{AttributeValue, StreamRecord};

#[must_use]
pub(crate) fn storage_stream_record_from_facade(record: StreamRecord) -> StorageStreamRecord {
    StorageStreamRecord {
        keys: storage_item_from_facade(record.keys),
        sequence_number: record.sequence_number,
        old_image: record.old_image.map(storage_item_from_facade),
        new_image: record.new_image.map(storage_item_from_facade),
    }
}

#[must_use]
fn storage_item_from_facade<S: BuildHasher>(
    item: HashMap<String, AttributeValue, S>,
) -> StorageItem {
    item.into_iter()
        .map(|(name, value)| (name, storage_value_from_facade(value)))
        .collect()
}

#[must_use]
fn storage_value_from_facade(value: AttributeValue) -> StorageValue {
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
                .map(storage_value_from_facade)
                .collect::<Vec<_>>(),
        ),
        AttributeValue::M(values) => StorageValue::M(storage_item_from_facade(values)),
    }
}
