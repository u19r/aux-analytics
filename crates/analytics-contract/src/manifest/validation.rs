use std::collections::HashSet;

use super::*;
use crate::{
    ManifestValidationError,
    constants::{
        BUILTIN_ROW_ID_COLUMN, BUILTIN_TABLE_NAME_COLUMN, BUILTIN_TENANT_ID_COLUMN,
        INTERNAL_EXPIRY_COLUMN, INTERNAL_INGESTED_AT_COLUMN, INTERNAL_MISSING_RETENTION_COLUMN,
        MANIFEST_VERSION,
    },
};

impl AnalyticsManifest {
    #[must_use]
    pub fn new(tables: Vec<TableRegistration>) -> Self {
        Self {
            version: MANIFEST_VERSION,
            tables,
        }
    }

    pub fn validate(&self) -> Result<(), ManifestValidationError> {
        if self.version != MANIFEST_VERSION {
            return Err(ManifestValidationError::UnsupportedVersion {
                actual: self.version,
                expected: MANIFEST_VERSION,
            });
        }

        let mut table_names = HashSet::new();
        for table in &self.tables {
            table.validate()?;
            if !table_names.insert(table.analytics_table_name.as_str()) {
                return Err(ManifestValidationError::DuplicateAnalyticsTableName(
                    table.analytics_table_name.clone(),
                ));
            }
        }

        Ok(())
    }
}
impl TableRegistration {
    pub fn validate(&self) -> Result<(), ManifestValidationError> {
        validate_non_empty("source_table_name", self.source_table_name.as_str())?;
        validate_non_empty("analytics_table_name", self.analytics_table_name.as_str())?;
        if let Some(document_column) = self.document_column.as_deref() {
            validate_non_empty("document_column", document_column)?;
            validate_custom_output_column(self.analytics_table_name.as_str(), document_column)?;
        }
        validate_tenant_selector(&self.tenant_selector)?;
        validate_row_identity(&self.row_identity)?;
        if let Some(row_expansion) = self.row_expansion.as_ref() {
            row_expansion.validate(self.analytics_table_name.as_str())?;
            self.validate_expansion_consumers(row_expansion)?;
        }
        if let Some(retention) = self.retention.as_ref() {
            retention.validate(self.analytics_table_name.as_str())?;
        }
        validate_table_scope(self)?;
        self.join_policy
            .validate(self.analytics_table_name.as_str(), &self.table_scope)?;

        let output_column_names = self.collect_output_columns()?;
        self.validate_layout(&output_column_names)
    }

    fn collect_output_columns(&self) -> Result<HashSet<&str>, ManifestValidationError> {
        let mut output_column_names = HashSet::new();
        if let Some(document_column) = self.document_column.as_deref() {
            insert_output_column_or_duplicate_error(
                self.analytics_table_name.as_str(),
                &mut output_column_names,
                document_column,
            )?;
        }
        for column in &self.columns {
            column.validate()?;
            validate_custom_output_column(
                self.analytics_table_name.as_str(),
                column.column_name.as_str(),
            )?;
            insert_output_column_or_duplicate_error(
                self.analytics_table_name.as_str(),
                &mut output_column_names,
                column.column_name.as_str(),
            )?;
        }

        self.insert_projection_columns(&mut output_column_names)?;
        self.insert_projection_attributes(&mut output_column_names)?;
        Ok(output_column_names)
    }

    fn insert_projection_columns<'a>(
        &'a self,
        output_column_names: &mut HashSet<&'a str>,
    ) -> Result<(), ManifestValidationError> {
        if let Some(projection_columns) = self.projection_columns.as_deref() {
            for column in projection_columns {
                validate_non_empty("projection column_name", column.column_name.as_str())?;
                validate_non_empty("projection attribute_path", column.attribute_path.as_str())?;
                validate_custom_output_column(
                    self.analytics_table_name.as_str(),
                    column.column_name.as_str(),
                )?;
                insert_output_column_or_duplicate_error(
                    self.analytics_table_name.as_str(),
                    output_column_names,
                    column.column_name.as_str(),
                )?;
            }
        }

        Ok(())
    }

    fn insert_projection_attributes<'a>(
        &'a self,
        output_column_names: &mut HashSet<&'a str>,
    ) -> Result<(), ManifestValidationError> {
        if let Some(attribute_names) = self.projection_attribute_names.as_deref() {
            for column_name in attribute_names {
                validate_non_empty("projection attribute name", column_name.as_str())?;
                validate_custom_output_column(
                    self.analytics_table_name.as_str(),
                    column_name.as_str(),
                )?;
                insert_output_column_or_duplicate_error(
                    self.analytics_table_name.as_str(),
                    output_column_names,
                    column_name.as_str(),
                )?;
            }
        }

        Ok(())
    }

    fn validate_layout(
        &self,
        output_column_names: &HashSet<&str>,
    ) -> Result<(), ManifestValidationError> {
        for partition_key in &self.partition_keys {
            validate_non_empty("partition column_name", partition_key.column_name.as_str())?;
            validate_declared_output_column(
                self.analytics_table_name.as_str(),
                partition_key.column_name.as_str(),
                output_column_names,
            )?;
        }

        for clustering_key in &self.clustering_keys {
            validate_non_empty(
                "clustering column_name",
                clustering_key.column_name.as_str(),
            )?;
            validate_declared_output_column(
                self.analytics_table_name.as_str(),
                clustering_key.column_name.as_str(),
                output_column_names,
            )?;
        }

        Ok(())
    }
}

impl JoinPolicy {
    fn validate(
        &self,
        table: &str,
        table_scope: &TableScope,
    ) -> Result<(), ManifestValidationError> {
        if matches!(self.max_join_rows_hint, Some(0)) {
            return Err(ManifestValidationError::InvalidJoinPolicy {
                table: table.to_string(),
                reason: "max_join_rows_hint must be greater than zero when set",
            });
        }
        if matches!(table_scope, TableScope::GlobalReference { .. })
            && self.allowed_as_join
            && self.max_join_rows_hint.is_none()
        {
            return Err(ManifestValidationError::InvalidJoinPolicy {
                table: table.to_string(),
                reason: "global reference joins require max_join_rows_hint",
            });
        }
        Ok(())
    }
}

fn validate_table_scope(table: &TableRegistration) -> Result<(), ManifestValidationError> {
    match &table.table_scope {
        TableScope::TenantScoped => Ok(()),
        TableScope::GlobalReference { reference_class } => {
            validate_non_empty("table_scope reference_class", reference_class.as_str())?;
            if table.tenant_id.is_some() || table.tenant_selector != TenantSelector::None {
                return Err(ManifestValidationError::GlobalReferenceHasTenantScope {
                    table: table.analytics_table_name.clone(),
                });
            }
            Ok(())
        }
    }
}

fn validate_tenant_selector(selector: &TenantSelector) -> Result<(), ManifestValidationError> {
    match selector {
        TenantSelector::None | TenantSelector::TableName => Ok(()),
        TenantSelector::Attribute { attribute_name }
        | TenantSelector::TableNameOrPartitionKeyPrefix { attribute_name }
        | TenantSelector::PartitionKeyPrefix { attribute_name } => {
            validate_non_empty("tenant_selector attribute_name", attribute_name)
        }
        TenantSelector::AttributeRegex {
            attribute_name,
            regex,
            capture,
        } => {
            validate_non_empty("tenant_selector attribute_name", attribute_name)?;
            validate_non_empty("tenant_selector regex", regex)?;
            validate_non_empty("tenant_selector capture", capture)
        }
    }
}

fn validate_row_identity(identity: &RowIdentity) -> Result<(), ManifestValidationError> {
    match identity {
        RowIdentity::RecordKey | RowIdentity::StreamKeys => Ok(()),
        RowIdentity::Attribute { attribute_name } => {
            validate_non_empty("row_identity attribute_name", attribute_name)
        }
        RowIdentity::Attributes { attribute_names } => {
            if attribute_names.is_empty() {
                return Err(ManifestValidationError::EmptyField(
                    "row_identity attribute_names",
                ));
            }
            let mut unique = HashSet::new();
            for attribute_name in attribute_names {
                validate_non_empty("row_identity attribute_name", attribute_name)?;
                if !unique.insert(attribute_name) {
                    return Err(ManifestValidationError::InvalidRowIdentity {
                        reason: "attribute_names must be unique",
                    });
                }
            }
            Ok(())
        }
        RowIdentity::AttributeRegex {
            attribute_name,
            regex,
            capture,
        } => {
            validate_non_empty("row_identity attribute_name", attribute_name)?;
            validate_non_empty("row_identity regex", regex)?;
            validate_non_empty("row_identity capture", capture)
        }
    }
}

pub(super) fn validate_non_empty(
    field: &'static str,
    value: &str,
) -> Result<(), ManifestValidationError> {
    if value.trim().is_empty() {
        return Err(ManifestValidationError::EmptyField(field));
    }
    Ok(())
}

fn validate_declared_output_column(
    table: &str,
    column: &str,
    output_column_names: &HashSet<&str>,
) -> Result<(), ManifestValidationError> {
    if output_column_names.contains(column) || is_builtin_column(column) {
        return Ok(());
    }
    Err(ManifestValidationError::UnknownLayoutColumn {
        table: table.to_string(),
        column: column.to_string(),
    })
}

fn insert_output_column_or_duplicate_error<'a>(
    table: &str,
    output_column_names: &mut HashSet<&'a str>,
    column: &'a str,
) -> Result<(), ManifestValidationError> {
    if output_column_names.insert(column) {
        return Ok(());
    }
    Err(ManifestValidationError::DuplicateColumnName {
        table: table.to_string(),
        column: column.to_string(),
    })
}

fn validate_custom_output_column(table: &str, column: &str) -> Result<(), ManifestValidationError> {
    if is_builtin_column(column) {
        return Err(ManifestValidationError::ReservedOutputColumn {
            table: table.to_string(),
            column: column.to_string(),
        });
    }
    Ok(())
}

fn is_builtin_column(column: &str) -> bool {
    matches!(
        column,
        BUILTIN_TENANT_ID_COLUMN
            | BUILTIN_ROW_ID_COLUMN
            | BUILTIN_TABLE_NAME_COLUMN
            | INTERNAL_INGESTED_AT_COLUMN
            | INTERNAL_EXPIRY_COLUMN
            | INTERNAL_MISSING_RETENTION_COLUMN
    )
}

impl RetentionPolicy {
    fn validate(&self, table: &str) -> Result<(), ManifestValidationError> {
        if self.period_ms == 0 {
            return Err(ManifestValidationError::InvalidRetentionPeriod {
                table: table.to_string(),
            });
        }
        validate_retention_timestamp(&self.timestamp)
    }
}

fn validate_retention_timestamp(
    timestamp: &RetentionTimestamp,
) -> Result<(), ManifestValidationError> {
    match timestamp {
        RetentionTimestamp::Attribute { attribute_path } => {
            validate_non_empty("retention timestamp attribute_path", attribute_path)
        }
        RetentionTimestamp::IngestedAt => Ok(()),
    }
}

impl AnalyticsColumn {
    pub fn validate(&self) -> Result<(), ManifestValidationError> {
        validate_non_empty("column_name", self.column_name.as_str())
    }
}
