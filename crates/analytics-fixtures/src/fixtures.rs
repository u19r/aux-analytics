use std::collections::HashMap;

use analytics_contract::{
    AnalyticsColumn, AnalyticsColumnType, AnalyticsManifest, JoinPolicy, PrimitiveColumnType,
    ProjectionColumn, RowIdentity, SortOrder, StorageItem, StorageValue, TableRegistration,
    TableScope, TenantSelector,
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
        table_scope: analytics_contract::TableScope::default(),
        join_policy: analytics_contract::JoinPolicy::default(),
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
        table_scope: analytics_contract::TableScope::default(),
        join_policy: analytics_contract::JoinPolicy::default(),
    }])
}

#[must_use]
pub fn metric_points_manifest() -> AnalyticsManifest {
    AnalyticsManifest::new(vec![metric_points_table(), billing_rate_class_map_table()])
}

#[must_use]
pub fn metric_points_table() -> TableRegistration {
    TableRegistration {
        source_table_name: "metric_points_v1_stream".to_string(),
        analytics_table_name: "metric_points_v1".to_string(),
        source_table_name_prefix: None,
        tenant_id: None,
        tenant_selector: TenantSelector::Attribute {
            attribute_name: "tenant_id".to_string(),
        },
        row_identity: RowIdentity::Attribute {
            attribute_name: "event_id".to_string(),
        },
        document_column: None,
        skip_delete: false,
        retention: None,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        projection_attribute_names: None,
        projection_columns: None,
        columns: metric_points_columns(),
        partition_keys: vec![analytics_contract::PartitionKey {
            column_name: "tenant_id".to_string(),
            bucket: None,
        }],
        clustering_keys: vec![
            analytics_contract::ClusteringKey {
                column_name: "series_name".to_string(),
                order: Some(SortOrder::Asc),
            },
            analytics_contract::ClusteringKey {
                column_name: "customer_account_id".to_string(),
                order: Some(SortOrder::Asc),
            },
            analytics_contract::ClusteringKey {
                column_name: "occurred_at_ms".to_string(),
                order: Some(SortOrder::Asc),
            },
        ],
        table_scope: TableScope::TenantScoped,
        join_policy: JoinPolicy {
            allowed_as_primary: true,
            allowed_as_join: false,
            max_join_rows_hint: None,
        },
    }
}

#[must_use]
pub fn billing_rate_class_map_table() -> TableRegistration {
    TableRegistration {
        source_table_name: "billing_rate_class_map_v1_stream".to_string(),
        analytics_table_name: "billing_rate_class_map_v1".to_string(),
        source_table_name_prefix: None,
        tenant_id: None,
        tenant_selector: TenantSelector::Attribute {
            attribute_name: "tenant_id".to_string(),
        },
        row_identity: RowIdentity::Attribute {
            attribute_name: "model_name".to_string(),
        },
        document_column: None,
        skip_delete: false,
        retention: None,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        projection_attribute_names: None,
        projection_columns: None,
        columns: vec![
            analytics_column("model_name", PrimitiveColumnType::VarChar),
            analytics_column("rate_class", PrimitiveColumnType::VarChar),
            analytics_column("effective_from_ms", PrimitiveColumnType::BigInt),
            analytics_column("effective_to_ms", PrimitiveColumnType::BigInt),
        ],
        partition_keys: vec![analytics_contract::PartitionKey {
            column_name: "tenant_id".to_string(),
            bucket: None,
        }],
        clustering_keys: vec![
            analytics_contract::ClusteringKey {
                column_name: "model_name".to_string(),
                order: Some(SortOrder::Asc),
            },
            analytics_contract::ClusteringKey {
                column_name: "effective_from_ms".to_string(),
                order: Some(SortOrder::Asc),
            },
        ],
        table_scope: TableScope::TenantScoped,
        join_policy: JoinPolicy {
            allowed_as_primary: false,
            allowed_as_join: true,
            max_join_rows_hint: Some(100_000),
        },
    }
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

fn metric_points_columns() -> Vec<AnalyticsColumn> {
    vec![
        analytics_column("series_name", PrimitiveColumnType::VarChar),
        analytics_column("customer_account_id", PrimitiveColumnType::VarChar),
        analytics_column("org_id", PrimitiveColumnType::VarChar),
        analytics_column("occurred_at_ms", PrimitiveColumnType::BigInt),
        analytics_column("ingested_at_ms", PrimitiveColumnType::BigInt),
        analytics_column("event_id", PrimitiveColumnType::VarChar),
        analytics_column("value_i64", PrimitiveColumnType::BigInt),
        analytics_column("value_decimal", PrimitiveColumnType::Decimal),
        analytics_column("dim_1", PrimitiveColumnType::VarChar),
        analytics_column("dim_2", PrimitiveColumnType::VarChar),
        analytics_column("dim_3", PrimitiveColumnType::VarChar),
        analytics_column("source", PrimitiveColumnType::VarChar),
        analytics_column("schema_version", PrimitiveColumnType::BigInt),
    ]
}

fn analytics_column(column_name: &str, primitive: PrimitiveColumnType) -> AnalyticsColumn {
    AnalyticsColumn {
        column_name: column_name.to_string(),
        column_type: AnalyticsColumnType::Primitive { primitive },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_point_fixtures_validate_with_typed_layout_columns() {
        let manifest = metric_points_manifest();

        manifest
            .validate()
            .expect("metric point fixture manifest should validate");
        let metric_table = manifest
            .tables
            .iter()
            .find(|table| table.analytics_table_name == "metric_points_v1")
            .expect("metric points table exists");
        assert!(
            metric_table
                .columns
                .iter()
                .any(|column| column.column_name == "series_name")
        );
        assert!(
            metric_table
                .columns
                .iter()
                .any(|column| column.column_name == "value_decimal")
        );
        assert_eq!(metric_table.partition_keys[0].column_name, "tenant_id");
    }
}
