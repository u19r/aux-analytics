use analytics_contract::{RowIdentity, StorageItem, TableRegistration};

use super::materialization::{RowSource, storage_item_to_json_text, string_attribute};
use crate::{
    cache::EngineCaches,
    engine::{AnalyticsEngineError, AnalyticsEngineResult},
};

pub(super) fn row_id_from_record(
    table: &TableRegistration,
    source: RowSource<'_>,
    item: &StorageItem,
    caches: &mut EngineCaches,
) -> AnalyticsEngineResult<String> {
    match &table.row_identity {
        RowIdentity::RecordKey => Ok(row_id_from_key_bytes(source.record_key)),
        RowIdentity::StreamKeys => {
            if let Some(digest) = source.stream_key_digest {
                return Ok(digest.to_string());
            }
            let key_json = storage_item_to_json_text(source.record_keys)?;
            Ok(row_id_from_key_bytes(key_json.as_bytes()))
        }
        RowIdentity::Attribute { attribute_name } => Ok(row_id_from_key_bytes(
            string_attribute(item, attribute_name)?.as_bytes(),
        )),
        RowIdentity::Attributes { attribute_names } => {
            let values = attribute_names
                .iter()
                .map(|attribute_name| {
                    item.get(attribute_name)
                        .map(|value| (attribute_name, value))
                        .ok_or_else(|| {
                            AnalyticsEngineError::MissingAttribute(attribute_name.clone())
                        })
                })
                .collect::<AnalyticsEngineResult<Vec<_>>>()?;
            Ok(row_id_from_key_bytes(
                serde_json::to_vec(&values)?.as_slice(),
            ))
        }
        RowIdentity::AttributeRegex {
            attribute_name,
            regex,
            capture,
        } => {
            let value = caches.regexes.capture(
                attribute_name,
                string_attribute(item, attribute_name)?,
                regex,
                capture,
            )?;
            Ok(row_id_from_key_bytes(value.as_bytes()))
        }
    }
}

fn row_id_from_key_bytes(bytes: &[u8]) -> String {
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(char::from(
            crate::constants::HEX_DIGITS[usize::from(byte >> 4)],
        ));
        value.push(char::from(
            crate::constants::HEX_DIGITS[usize::from(byte & 0x0f)],
        ));
    }
    value
}
