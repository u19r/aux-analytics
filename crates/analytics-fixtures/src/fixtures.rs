use std::collections::HashMap;

use analytics_contract::{
    AnalyticsColumnType, AnalyticsManifest, PrimitiveColumnType, ProjectionColumn, RowIdentity,
    StorageItem, StorageValue, TableRegistration, TenantSelector,
};

#[must_use]
pub fn users_manifest() -> AnalyticsManifest {
    AnalyticsManifest::new(vec![TableRegistration {
        source_table_name: "tenant_01".to_string(),
        analytics_table_name: "users".to_string(),
        source_table_name_prefix: None,
        tenant_id: Some("tenant_01".to_string()),
        tenant_selector: TenantSelector::TableName,
        row_identity: RowIdentity::RecordKey,
        document_column: Some("item".to_string()),
        skip_delete: false,
        retention: None,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        projection_attribute_names: None,
        projection_columns: Some(vec![
            projection_column("user_id", "user_id"),
            projection_column("email", "profile.email"),
            projection_column("org_id", "organization.id"),
        ]),
        columns: Vec::new(),
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
    }])
}

#[must_use]
pub fn generic_items_manifest() -> AnalyticsManifest {
    AnalyticsManifest::new(vec![TableRegistration {
        source_table_name: "legacy_items".to_string(),
        analytics_table_name: "legacy_items".to_string(),
        source_table_name_prefix: None,
        tenant_id: None,
        tenant_selector: TenantSelector::None,
        row_identity: RowIdentity::StreamKeys,
        document_column: Some("item".to_string()),
        skip_delete: false,
        retention: None,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        projection_attribute_names: None,
        projection_columns: None,
        columns: Vec::new(),
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
    }])
}

#[must_use]
pub fn storage_key(entity_name: &str, id: &str) -> StorageItem {
    HashMap::from([(
        "pk".to_string(),
        StorageValue::S(format!("{entity_name}#{id}")),
    )])
}

#[must_use]
pub fn user_item(user_id: &str, email: &str, org_id: &str) -> StorageItem {
    HashMap::from([
        ("user_id".to_string(), StorageValue::S(user_id.to_string())),
        (
            "profile".to_string(),
            StorageValue::M(HashMap::from([(
                "email".to_string(),
                StorageValue::S(email.to_string()),
            )])),
        ),
        (
            "organization".to_string(),
            StorageValue::M(HashMap::from([(
                "id".to_string(),
                StorageValue::S(org_id.to_string()),
            )])),
        ),
    ])
}

#[must_use]
pub fn legacy_item(id: &str, email: &str) -> StorageItem {
    HashMap::from([
        ("pk".to_string(), StorageValue::S(format!("USER#{id}"))),
        (
            "profile".to_string(),
            StorageValue::M(HashMap::from([(
                "email".to_string(),
                StorageValue::S(email.to_string()),
            )])),
        ),
    ])
}

fn projection_column(column_name: &str, attribute_path: &str) -> ProjectionColumn {
    ProjectionColumn {
        column_name: column_name.to_string(),
        attribute_path: attribute_path.to_string(),
        column_type: Some(AnalyticsColumnType::Primitive {
            primitive: PrimitiveColumnType::VarChar,
        }),
    }
}
