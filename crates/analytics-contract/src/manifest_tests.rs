use std::{collections::HashMap, error::Error, fs};

use crate::*;

#[test]
fn manifest_serializes_to_stable_snake_case_contract() {
    let manifest = AnalyticsManifest::new(vec![TableRegistration {
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
        projection_columns: Some(vec![ProjectionColumn {
            column_name: "email".to_string(),
            attribute_path: "profile.email".to_string(),
            column_type: Some(AnalyticsColumnType::Primitive {
                primitive: PrimitiveColumnType::VarChar,
            }),
        }]),
        columns: Vec::new(),
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
    }]);

    let encoded = serde_json::to_value(&manifest).expect("manifest should serialize");
    assert_eq!(encoded["version"], MANIFEST_VERSION);
    assert_eq!(encoded["tables"][0]["analytics_table_name"], "users");
    assert_eq!(
        encoded["tables"][0]["projection_columns"][0]["column_type"]["primitive"],
        "var_char"
    );
    assert!(
        encoded["tables"][0].get("retention").is_none(),
        "retention should be omitted when no static policy is configured"
    );
}

#[test]
fn manifest_deserializes_table_without_retention() {
    let manifest: AnalyticsManifest = serde_json::from_value(serde_json::json!({
        "version": MANIFEST_VERSION,
        "tables": [{
            "source_table_name": "tenant_01",
            "analytics_table_name": "users",
            "projection_attribute_names": ["email"]
        }]
    }))
    .expect("manifest without retention should deserialize");

    assert_eq!(manifest.tables[0].retention, None);
    manifest
        .validate()
        .expect("manifest without retention should validate");
}

#[test]
fn manifest_schema_does_not_require_table_retention() {
    let schema = serde_json::to_value(schemars::schema_for!(AnalyticsManifest))
        .expect("manifest schema should serialize");
    let required = schema
        .pointer("/$defs/TableRegistration/required")
        .and_then(serde_json::Value::as_array)
        .expect("table required fields");

    assert!(
        !required
            .iter()
            .any(|field| field.as_str() == Some("retention")),
        "retention must remain optional in the manifest schema"
    );
}

#[test]
fn stream_record_uses_storage_wire_shape_without_external_types() {
    let record = StorageStreamRecord {
        keys: StorageItem::new(),
        sequence_number: "1".to_string(),
        old_image: None,
        new_image: Some(HashMap::from([(
            "profile".to_string(),
            StorageValue::M(HashMap::from([(
                "email".to_string(),
                StorageValue::S("a@example.com".to_string()),
            )])),
        )])),
    };

    let encoded = serde_json::to_value(&record).expect("record should serialize");
    assert_eq!(encoded["SequenceNumber"], "1");
    assert_eq!(
        encoded["NewImage"]["profile"]["M"]["email"]["S"],
        "a@example.com"
    );
}

#[test]
fn manifest_validation_rejects_duplicate_output_tables() {
    let table = TableRegistration {
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
        projection_columns: None,
        columns: Vec::new(),
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
    };
    let manifest = AnalyticsManifest::new(vec![table.clone(), table]);

    assert!(matches!(
        manifest.validate(),
        Err(ManifestValidationError::DuplicateAnalyticsTableName(name)) if name == "users"
    ));
}

#[test]
fn manifest_validation_rejects_layout_columns_that_are_not_projected() {
    let manifest = AnalyticsManifest::new(vec![TableRegistration {
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
        projection_attribute_names: Some(vec!["email".to_string()]),
        projection_columns: None,
        columns: Vec::new(),
        partition_keys: vec![PartitionKey {
            column_name: "org_id".to_string(),
            bucket: None,
        }],
        clustering_keys: Vec::new(),
    }]);

    assert!(matches!(
        manifest.validate(),
        Err(ManifestValidationError::UnknownLayoutColumn { column, .. }) if column == "org_id"
    ));
}

#[test]
fn manifest_validation_rejects_reserved_projection_column_names() {
    for reserved_column in ["tenant_id", "table_name", "__id"] {
        let manifest = AnalyticsManifest::new(vec![TableRegistration {
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
            projection_columns: Some(vec![ProjectionColumn {
                column_name: reserved_column.to_string(),
                attribute_path: reserved_column.to_string(),
                column_type: Some(AnalyticsColumnType::Primitive {
                    primitive: PrimitiveColumnType::VarChar,
                }),
            }]),
            columns: Vec::new(),
            partition_keys: Vec::new(),
            clustering_keys: Vec::new(),
        }]);

        assert!(matches!(
            manifest.validate(),
            Err(ManifestValidationError::ReservedOutputColumn { column, .. })
                if column == reserved_column
        ));
    }
}

#[test]
fn manifest_validation_rejects_duplicate_output_columns_across_sources() {
    let mut manifest = AnalyticsManifest::new(vec![TableRegistration {
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
        projection_columns: Some(vec![ProjectionColumn {
            column_name: "item".to_string(),
            attribute_path: "profile.item".to_string(),
            column_type: Some(AnalyticsColumnType::Primitive {
                primitive: PrimitiveColumnType::VarChar,
            }),
        }]),
        columns: Vec::new(),
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
    }]);

    assert!(matches!(
        manifest.validate(),
        Err(ManifestValidationError::DuplicateColumnName { column, .. }) if column == "item"
    ));

    manifest.tables[0].projection_columns = None;
    manifest.tables[0].columns = vec![AnalyticsColumn {
        column_name: "email".to_string(),
        column_type: AnalyticsColumnType::Primitive {
            primitive: PrimitiveColumnType::VarChar,
        },
    }];
    manifest.tables[0].projection_attribute_names = Some(vec!["email".to_string()]);

    assert!(matches!(
        manifest.validate(),
        Err(ManifestValidationError::DuplicateColumnName { column, .. }) if column == "email"
    ));
}

#[test]
fn manifest_validation_allows_reserved_columns_as_layout_references() {
    let manifest = AnalyticsManifest::new(vec![TableRegistration {
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
        projection_attribute_names: Some(vec!["email".to_string()]),
        projection_columns: None,
        columns: Vec::new(),
        partition_keys: vec![PartitionKey {
            column_name: "tenant_id".to_string(),
            bucket: None,
        }],
        clustering_keys: Vec::new(),
    }]);

    manifest
        .validate()
        .expect("built-in columns can be used for layout");
}

#[test]
fn manifest_validation_accepts_static_retention() {
    let manifest = AnalyticsManifest::new(vec![TableRegistration {
        source_table_name: "tenant_01".to_string(),
        analytics_table_name: "users".to_string(),
        source_table_name_prefix: None,
        tenant_id: Some("tenant_01".to_string()),
        tenant_selector: TenantSelector::TableName,
        row_identity: RowIdentity::RecordKey,
        document_column: Some("item".to_string()),
        skip_delete: false,
        retention: Some(RetentionPolicy {
            period_ms: 1_000,
            timestamp: RetentionTimestamp::Attribute {
                attribute_path: "created_at_ms".to_string(),
            },
        }),
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        projection_attribute_names: Some(vec!["email".to_string()]),
        projection_columns: None,
        columns: Vec::new(),
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
    }]);

    manifest.validate().expect("static retention manifest");
}

#[test]
fn manifest_validation_rejects_zero_retention_period() {
    let mut manifest = AnalyticsManifest::new(vec![TableRegistration {
        source_table_name: "tenant_01".to_string(),
        analytics_table_name: "users".to_string(),
        source_table_name_prefix: None,
        tenant_id: Some("tenant_01".to_string()),
        tenant_selector: TenantSelector::TableName,
        row_identity: RowIdentity::RecordKey,
        document_column: Some("item".to_string()),
        skip_delete: false,
        retention: Some(RetentionPolicy {
            period_ms: 0,
            timestamp: RetentionTimestamp::IngestedAt,
        }),
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        projection_attribute_names: Some(vec!["email".to_string()]),
        projection_columns: None,
        columns: Vec::new(),
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
    }]);

    assert!(matches!(
        manifest.validate(),
        Err(ManifestValidationError::InvalidRetentionPeriod { .. })
    ));
    manifest.tables[0].retention = Some(RetentionPolicy {
        period_ms: 1,
        timestamp: RetentionTimestamp::Attribute {
            attribute_path: " ".to_string(),
        },
    });
    assert!(matches!(
        manifest.validate(),
        Err(ManifestValidationError::EmptyField(
            "retention timestamp attribute_path"
        ))
    ));
}

#[test]
fn manifest_validation_rejects_internal_retention_columns() {
    for reserved_column in [
        INTERNAL_INGESTED_AT_COLUMN,
        INTERNAL_EXPIRY_COLUMN,
        INTERNAL_MISSING_RETENTION_COLUMN,
    ] {
        let manifest = AnalyticsManifest::new(vec![TableRegistration {
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
            projection_attribute_names: Some(vec![reserved_column.to_string()]),
            projection_columns: None,
            columns: Vec::new(),
            partition_keys: Vec::new(),
            clustering_keys: Vec::new(),
        }]);

        assert!(matches!(
            manifest.validate(),
            Err(ManifestValidationError::ReservedOutputColumn { column, .. })
                if column == reserved_column
        ));
    }
}

#[test]
fn read_manifest_rejects_invalid_json_file() {
    let file = write_manifest_file("{ not json");

    let error = read_manifest(file.path()).expect_err("invalid JSON should fail manifest loading");

    assert_eq!(error.kind(), ManifestLoadErrorKind::Json);
    assert!(
        error
            .to_string()
            .contains("invalid analytics manifest JSON"),
        "{error}"
    );
}

#[test]
fn read_manifest_rejects_schema_invalid_manifest_file() {
    let file = write_manifest_file(
        r#"{
            "version": 1,
            "tables": "not an array"
        }"#,
    );

    let error =
        read_manifest(file.path()).expect_err("schema invalid JSON should fail manifest loading");

    assert_eq!(error.kind(), ManifestLoadErrorKind::SchemaValidation);
    assert!(
        error.to_string().contains("is not of type \"array\""),
        "{error}"
    );
}

#[test]
fn read_manifest_rejects_semantically_invalid_manifest_file() {
    let file = write_manifest_file(
        r#"{
            "version": 1,
            "tables": [
                {
                    "source_table_name": "source_a",
                    "analytics_table_name": "users",
                    "tenant_selector": { "kind": "none" },
                    "row_identity": { "kind": "stream_keys" },
                    "columns": []
                },
                {
                    "source_table_name": "source_b",
                    "analytics_table_name": "users",
                    "tenant_selector": { "kind": "none" },
                    "row_identity": { "kind": "stream_keys" },
                    "columns": []
                }
            ]
        }"#,
    );

    let error =
        read_manifest(file.path()).expect_err("semantic validation should fail manifest loading");

    assert_eq!(error.kind(), ManifestLoadErrorKind::ManifestValidation);
    assert!(
        error
            .source()
            .is_some_and(|source| source.to_string().contains("users")),
        "{error}"
    );
}

#[test]
fn manifest_supports_generic_document_tables_without_tenant_layout() {
    let manifest = AnalyticsManifest::new(vec![TableRegistration {
        source_table_name: "existing_table".to_string(),
        analytics_table_name: "existing_items".to_string(),
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
    }]);

    manifest.validate().expect("generic manifest");
}

#[test]
fn structured_query_serializes_to_stable_contract() {
    let query = StructuredQuery {
        analytics_table_name: "users".to_string(),
        select: vec![
            QuerySelect::Column {
                column_name: "email".to_string(),
                alias: None,
            },
            QuerySelect::Count {
                alias: "count".to_string(),
            },
        ],
        filters: vec![QueryPredicate::Eq {
            expression: QueryExpression::Column {
                column_name: "org_id".to_string(),
            },
            value: serde_json::Value::String("org-a".to_string()),
        }],
        group_by: vec![QueryExpression::Column {
            column_name: "email".to_string(),
        }],
        order_by: vec![QueryOrder {
            expression: QueryExpression::Column {
                column_name: "email".to_string(),
            },
            direction: Some(SortOrder::Asc),
        }],
        limit: Some(10),
    };

    query.validate_shape().expect("valid query");
    let encoded = serde_json::to_value(&query).expect("query should serialize");
    assert_eq!(encoded["analytics_table_name"], "users");
    assert_eq!(encoded["select"][0]["kind"], "column");
    assert_eq!(encoded["filters"][0]["kind"], "eq");
}

#[test]
fn structured_query_rejects_empty_select() {
    let query = StructuredQuery {
        analytics_table_name: "users".to_string(),
        select: Vec::new(),
        filters: Vec::new(),
        group_by: Vec::new(),
        order_by: Vec::new(),
        limit: None,
    };

    assert!(matches!(
        query.validate_shape(),
        Err(StructuredQueryValidationError::EmptySelect)
    ));
}

fn write_manifest_file(contents: &str) -> tempfile::NamedTempFile {
    let file = tempfile::NamedTempFile::new().expect("manifest temp file");
    fs::write(file.path(), contents).expect("write manifest temp file");
    file
}
