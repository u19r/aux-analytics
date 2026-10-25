use std::collections::HashSet;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::{
    RetentionPolicy, RetentionTimestamp, RowIdentity, TableRegistration, TenantSelector,
    validation::validate_non_empty,
};
use crate::ManifestValidationError;

/// Manifest-controlled projection from a source list to analytical rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct RowExpansion {
    /// Path from the source item to the list of row elements.
    #[schema(min_items = 1)]
    pub list_path: Vec<RowPathSegment>,
    /// Fields copied from the source item into every expanded row.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schema(default = json!([]), min_items = 0)]
    pub source_fields: Vec<RowProjectionField>,
    /// Fields copied from each list element into its expanded row.
    #[schema(min_items = 1)]
    pub element_fields: Vec<RowProjectionField>,
    /// Optional output attribute populated with the zero-based list ordinal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(nullable = true, default = json!(null), min_length = 1, max_length = 255)]
    pub ordinal_attribute_name: Option<String>,
}

/// One field copied into an expanded source row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct RowProjectionField {
    /// Attribute name presented to tenant, identity, retention, and column
    /// projections after expansion.
    #[schema(min_length = 1, max_length = 255)]
    pub output_attribute_name: String,
    /// Path relative to the source item or current list element. An empty
    /// element path copies the whole element; source paths must name an
    /// attribute.
    #[schema(min_items = 0)]
    pub path: Vec<RowPathSegment>,
}

/// One typed segment in a source-item row-expansion path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RowPathSegment {
    Attribute {
        #[schema(min_length = 1, max_length = 255)]
        name: String,
    },
    Index {
        index: usize,
    },
}

impl RowExpansion {
    pub(super) fn validate(&self, table: &str) -> Result<(), ManifestValidationError> {
        if self.list_path.is_empty() {
            return Err(ManifestValidationError::InvalidRowExpansion {
                table: table.to_string(),
                reason: "list_path must not be empty",
            });
        }
        if self.element_fields.is_empty() {
            return Err(ManifestValidationError::InvalidRowExpansion {
                table: table.to_string(),
                reason: "element_fields must not be empty",
            });
        }
        validate_row_path(table, &self.list_path)?;
        validate_item_row_path(table, &self.list_path)?;
        let mut output_names = HashSet::new();
        for field in self.source_fields.iter().chain(&self.element_fields) {
            field.validate(table)?;
            if !output_names.insert(field.output_attribute_name.as_str()) {
                return Err(ManifestValidationError::DuplicateRowExpansionAttribute {
                    table: table.to_string(),
                    attribute: field.output_attribute_name.clone(),
                });
            }
        }
        for field in &self.source_fields {
            validate_item_row_path(table, &field.path)?;
        }
        if let Some(ordinal_attribute_name) = self.ordinal_attribute_name.as_deref() {
            validate_non_empty(
                "row_expansion ordinal_attribute_name",
                ordinal_attribute_name,
            )?;
            if !output_names.insert(ordinal_attribute_name) {
                return Err(ManifestValidationError::DuplicateRowExpansionAttribute {
                    table: table.to_string(),
                    attribute: ordinal_attribute_name.to_string(),
                });
            }
        }
        Ok(())
    }
}
impl RowProjectionField {
    fn validate(&self, table: &str) -> Result<(), ManifestValidationError> {
        validate_non_empty(
            "row_expansion output_attribute_name",
            self.output_attribute_name.as_str(),
        )?;
        validate_row_path(table, &self.path)?;
        Ok(())
    }
}

fn validate_row_path(table: &str, path: &[RowPathSegment]) -> Result<(), ManifestValidationError> {
    if path.iter().any(
        |segment| matches!(segment, RowPathSegment::Attribute { name } if name.trim().is_empty()),
    ) {
        return Err(ManifestValidationError::InvalidRowExpansion {
            table: table.to_string(),
            reason: "attribute path segments must not be empty",
        });
    }
    Ok(())
}

fn validate_item_row_path(
    table: &str,
    path: &[RowPathSegment],
) -> Result<(), ManifestValidationError> {
    if !matches!(path.first(), Some(RowPathSegment::Attribute { .. })) {
        return Err(ManifestValidationError::InvalidRowExpansion {
            table: table.to_string(),
            reason: "source item paths must begin with an attribute",
        });
    }
    Ok(())
}

impl TableRegistration {
    pub(super) fn validate_expansion_consumers(
        &self,
        expansion: &RowExpansion,
    ) -> Result<(), ManifestValidationError> {
        let table = self;
        let outputs = expansion
            .source_fields
            .iter()
            .chain(&expansion.element_fields)
            .map(|field| field.output_attribute_name.as_str())
            .chain(expansion.ordinal_attribute_name.as_deref())
            .collect::<HashSet<_>>();
        let required = self.collect_expansion_inputs()?;
        if let Some(attribute) = required
            .into_iter()
            .find(|attribute| !outputs.contains(attribute))
        {
            return Err(ManifestValidationError::UnknownRowExpansionOutput {
                table: table.analytics_table_name.clone(),
                attribute: attribute.to_string(),
            });
        }
        Ok(())
    }
}

impl TableRegistration {
    fn collect_expansion_inputs(&self) -> Result<Vec<&str>, ManifestValidationError> {
        let table = self;
        let mut required = Vec::new();
        match &table.tenant_selector {
            TenantSelector::Attribute { attribute_name }
            | TenantSelector::AttributeRegex { attribute_name, .. }
            | TenantSelector::PartitionKeyPrefix { attribute_name } => {
                required.push(attribute_name.as_str());
            }
            TenantSelector::None
            | TenantSelector::TableName
            | TenantSelector::TableNameOrPartitionKeyPrefix { .. } => {}
        }
        self.row_identity
            .collect_expansion_inputs(&mut required, &self.analytics_table_name)?;
        if let Some(attribute_names) = table.projection_attribute_names.as_deref() {
            required.extend(attribute_names.iter().map(String::as_str));
        }
        if let Some(columns) = table.projection_columns.as_deref() {
            required.extend(
                columns
                    .iter()
                    .filter_map(|column| self.projection_input_attribute(&column.attribute_path)),
            );
        }
        if let Some(RetentionPolicy {
            timestamp: RetentionTimestamp::Attribute { attribute_path },
            ..
        }) = table.retention.as_ref()
            && let Some(root) = attribute_path.split('.').next()
        {
            required.push(root);
        }
        Ok(required)
    }

    fn projection_input_attribute<'table>(&'table self, path: &'table str) -> Option<&'table str> {
        let root = path.split('.').next()?;
        let root = root.strip_suffix("[]").unwrap_or(root);
        Some(
            self.expression_attribute_names
                .as_ref()
                .and_then(|names| names.get(root))
                .map_or(root, String::as_str),
        )
    }
}

impl RowIdentity {
    fn collect_expansion_inputs<'a>(
        &'a self,
        required: &mut Vec<&'a str>,
        table: &str,
    ) -> Result<(), ManifestValidationError> {
        match self {
            RowIdentity::Attribute { attribute_name } => required.push(attribute_name.as_str()),
            RowIdentity::Attributes { attribute_names } => {
                required.extend(attribute_names.iter().map(String::as_str));
            }
            RowIdentity::AttributeRegex { attribute_name, .. } => {
                required.push(attribute_name.as_str());
            }
            RowIdentity::RecordKey | RowIdentity::StreamKeys => {
                return Err(ManifestValidationError::InvalidRowExpansion {
                    table: table.to_string(),
                    reason: "row_identity must depend on expanded output",
                });
            }
        }
        Ok(())
    }
}
