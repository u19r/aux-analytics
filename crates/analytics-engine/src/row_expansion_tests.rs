use analytics_contract::{RowExpansion, RowPathSegment, RowProjectionField, StorageValue};
use storage_types::VecMap;

use crate::row_expansion::expand_source_item;

#[test]
fn given_source_list_and_declared_projection_when_rows_are_expanded_then_only_declared_fields_and_ordinal_are_copied()
 {
    let source = VecMap::from_iter([
        (
            "tenant".to_string(),
            StorageValue::S("tenant-a".to_string()),
        ),
        ("ignored".to_string(), StorageValue::S("large".to_string())),
        (
            "payload".to_string(),
            StorageValue::L(vec![StorageValue::L(vec![
                StorageValue::S("series-a".to_string()),
                StorageValue::N("42".to_string()),
            ])]),
        ),
    ]);
    let expansion = RowExpansion {
        list_path: vec![attribute("payload")],
        source_fields: vec![field("tenant_id", vec![attribute("tenant")])],
        element_fields: vec![
            field("series_name", vec![index(0)]),
            field("value_i64", vec![index(1)]),
        ],
        ordinal_attribute_name: Some("ordinal".to_string()),
    };

    let rows = expand_source_item(&source, &expansion).unwrap();

    assert_eq!(
        rows,
        vec![VecMap::from_iter([
            (
                "tenant_id".to_string(),
                StorageValue::S("tenant-a".to_string())
            ),
            (
                "series_name".to_string(),
                StorageValue::S("series-a".to_string())
            ),
            ("value_i64".to_string(), StorageValue::N("42".to_string())),
            ("ordinal".to_string(), StorageValue::N("0".to_string())),
        ])]
    );
    assert!(!rows[0].contains_key("ignored"));
    assert!(!rows[0].contains_key("payload"));
}

fn field(output_attribute_name: &str, path: Vec<RowPathSegment>) -> RowProjectionField {
    RowProjectionField {
        output_attribute_name: output_attribute_name.to_string(),
        path,
    }
}

fn attribute(name: &str) -> RowPathSegment {
    RowPathSegment::Attribute {
        name: name.to_string(),
    }
}

const fn index(index: usize) -> RowPathSegment {
    RowPathSegment::Index { index }
}
