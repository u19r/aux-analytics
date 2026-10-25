use analytics_contract::{ProjectionColumn, StorageItem};
use storage_types::VecMap;

use super::projection::{ProjectionResult, extract_from_item, parse_attribute_path};

/// Materializes selected values for formal adapter observations. Production
/// lowering visits these values directly rather than retaining this map.
pub(crate) fn project_item(
    item: &StorageItem,
    columns: &[ProjectionColumn],
    names: Option<&VecMap<String, String>>,
) -> ProjectionResult<StorageItem> {
    let mut projected = StorageItem::with_capacity(columns.len());
    for column in columns {
        let segments = parse_attribute_path(&column.attribute_path, names)?;
        if let Some(value) = extract_from_item(item, &segments) {
            projected.insert(column.column_name.clone(), value.into_owned());
        }
    }
    Ok(projected)
}
