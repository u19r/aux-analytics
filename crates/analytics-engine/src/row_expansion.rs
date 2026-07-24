use analytics_contract::{RowExpansion, RowPathSegment, StorageItem, StorageValue};

use crate::engine::{AnalyticsEngineError, AnalyticsEngineResult};

pub(crate) fn expand_source_item(
    source: &StorageItem,
    expansion: &RowExpansion,
) -> AnalyticsEngineResult<Vec<StorageItem>> {
    let list = resolve_item_path(source, &expansion.list_path)?;
    let StorageValue::L(elements) = list else {
        return Err(AnalyticsEngineError::InvalidRowExpansionRecord(
            "list_path did not resolve to a list",
        ));
    };

    let source_values = expansion
        .source_fields
        .iter()
        .map(|field| {
            resolve_item_path(source, &field.path)
                .cloned()
                .map(|value| (field.output_attribute_name.clone(), value))
        })
        .collect::<AnalyticsEngineResult<Vec<_>>>()?;
    let mut rows = Vec::with_capacity(elements.len());
    for (ordinal, element) in elements.iter().enumerate() {
        let mut row = StorageItem::with_capacity(
            source_values.len()
                + expansion.element_fields.len()
                + usize::from(expansion.ordinal_attribute_name.is_some()),
        );
        row.extend(source_values.iter().cloned());
        for field in &expansion.element_fields {
            row.insert(
                field.output_attribute_name.clone(),
                resolve_value_path(element, &field.path)?.clone(),
            );
        }
        if let Some(attribute_name) = expansion.ordinal_attribute_name.as_ref() {
            row.insert(attribute_name.clone(), StorageValue::N(ordinal.to_string()));
        }
        rows.push(row);
    }
    Ok(rows)
}

fn resolve_item_path<'item>(
    item: &'item StorageItem,
    path: &[RowPathSegment],
) -> AnalyticsEngineResult<&'item StorageValue> {
    let (first, remainder) =
        path.split_first()
            .ok_or(AnalyticsEngineError::InvalidRowExpansionRecord(
                "path must not be empty",
            ))?;
    let RowPathSegment::Attribute { name } = first else {
        return Err(AnalyticsEngineError::InvalidRowExpansionRecord(
            "source item path must begin with an attribute",
        ));
    };
    let value = item
        .get(name)
        .ok_or(AnalyticsEngineError::MissingRowExpansionPath)?;
    resolve_value_path(value, remainder)
}

fn resolve_value_path<'value>(
    mut value: &'value StorageValue,
    path: &[RowPathSegment],
) -> AnalyticsEngineResult<&'value StorageValue> {
    for segment in path {
        value = match (segment, value) {
            (RowPathSegment::Attribute { name }, StorageValue::M(values)) => values
                .get(name)
                .ok_or(AnalyticsEngineError::MissingRowExpansionPath)?,
            (RowPathSegment::Index { index }, StorageValue::L(values)) => values
                .get(*index)
                .ok_or(AnalyticsEngineError::MissingRowExpansionPath)?,
            (RowPathSegment::Attribute { .. }, _) => {
                return Err(AnalyticsEngineError::InvalidRowExpansionRecord(
                    "attribute path segment did not resolve against a map",
                ));
            }
            (RowPathSegment::Index { .. }, _) => {
                return Err(AnalyticsEngineError::InvalidRowExpansionRecord(
                    "index path segment did not resolve against a list",
                ));
            }
        };
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use analytics_contract::{RowExpansion, RowPathSegment, RowProjectionField, StorageValue};

    use super::expand_source_item;

    #[test]
    fn expansion_copies_only_declared_source_and_element_fields() {
        let source = HashMap::from([
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
            vec![HashMap::from([
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
}
