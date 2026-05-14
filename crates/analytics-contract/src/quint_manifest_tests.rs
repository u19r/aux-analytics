use analytics_quint_test_support::map_result;
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::{
    AnalyticsColumn, AnalyticsColumnType, AnalyticsManifest, ClusteringKey,
    ManifestValidationError, PartitionKey, PrimitiveColumnType, ProjectionColumn, RetentionPolicy,
    RetentionTimestamp, RowIdentity, SortOrder, TableRegistration, TenantSelector,
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum ValidationResult {
    NotChecked,
    Valid,
    UnsupportedVersion,
    DuplicateAnalyticsTableName,
    EmptyField,
    ReservedOutputColumn,
    DuplicateColumnName,
    UnknownLayoutColumn,
    InvalidRetentionPeriod,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct ManifestState {
    #[serde(rename = "lastResult")]
    last_result: ValidationResult,
}

impl State<ManifestDriver> for ManifestState {
    fn from_driver(driver: &ManifestDriver) -> Result<Self> {
        Ok(Self {
            last_result: driver.last_result.clone(),
        })
    }
}

struct ManifestDriver {
    last_result: ValidationResult,
}

impl Default for ManifestDriver {
    fn default() -> Self {
        Self {
            last_result: ValidationResult::NotChecked,
        }
    }
}

impl Driver for ManifestDriver {
    type State = ManifestState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.last_result = ValidationResult::NotChecked,
            CheckValidSimpleManifest => self.check(&valid_simple_manifest()),
            CheckValidGenericDocumentManifest => self.check(&valid_generic_document_manifest()),
            CheckValidRetentionManifest => self.check(&valid_retention_manifest()),
            CheckUnsupportedVersion => {
                let mut manifest = valid_simple_manifest();
                manifest.version = 999;
                self.check(&manifest);
            },
            CheckDuplicateAnalyticsTableName => {
                let table = base_table("source_a", "users");
                self.check(&AnalyticsManifest::new(vec![table.clone(), table]));
            },
            CheckEmptySourceTableName => {
                let mut table = base_table("", "users");
                table.source_table_name = " ".to_string();
                self.check(&AnalyticsManifest::new(vec![table]));
            },
            CheckEmptyAnalyticsTableName => {
                self.check(&AnalyticsManifest::new(vec![base_table("source_a", " ")]));
            },
            CheckReservedProjectionColumn => {
                let mut table = base_table("source_a", "users");
                table.projection_columns = Some(vec![projection_column("tenant_id", "tenant_id")]);
                self.check(&AnalyticsManifest::new(vec![table]));
            },
            CheckReservedInternalRetentionColumn => {
                let mut table = base_table("source_a", "users");
                table.projection_attribute_names = Some(vec!["__expiry".to_string()]);
                table.projection_columns = None;
                self.check(&AnalyticsManifest::new(vec![table]));
            },
            CheckDuplicateProjectionColumn => {
                let mut table = base_table("source_a", "users");
                table.projection_columns = Some(vec![
                    projection_column("email", "profile.email"),
                    projection_column("email", "contact.email"),
                ]);
                self.check(&AnalyticsManifest::new(vec![table]));
            },
            CheckUnknownPartitionColumn => {
                let mut table = base_table("source_a", "users");
                table.partition_keys = vec![PartitionKey {
                    column_name: "org_id".to_string(),
                    bucket: None,
                }];
                self.check(&AnalyticsManifest::new(vec![table]));
            },
            CheckZeroRetentionPeriod => {
                let mut table = base_table("source_a", "users");
                table.retention = Some(RetentionPolicy {
                    period_ms: 0,
                    timestamp: RetentionTimestamp::IngestedAt,
                });
                self.check(&AnalyticsManifest::new(vec![table]));
            },
            CheckEmptyRetentionTimestamp => {
                let mut table = base_table("source_a", "users");
                table.retention = Some(RetentionPolicy {
                    period_ms: 1,
                    timestamp: RetentionTimestamp::Attribute {
                        attribute_path: " ".to_string(),
                    },
                });
                self.check(&AnalyticsManifest::new(vec![table]));
            },
        })
    }
}

impl ManifestDriver {
    fn check(&mut self, manifest: &AnalyticsManifest) {
        self.last_result = map_result(
            manifest.validate(),
            |()| ValidationResult::Valid,
            |error| match error {
                ManifestValidationError::UnsupportedVersion { .. } => {
                    ValidationResult::UnsupportedVersion
                }
                ManifestValidationError::DuplicateAnalyticsTableName(_) => {
                    ValidationResult::DuplicateAnalyticsTableName
                }
                ManifestValidationError::EmptyField(_) => ValidationResult::EmptyField,
                ManifestValidationError::ReservedOutputColumn { .. } => {
                    ValidationResult::ReservedOutputColumn
                }
                ManifestValidationError::DuplicateColumnName { .. } => {
                    ValidationResult::DuplicateColumnName
                }
                ManifestValidationError::UnknownLayoutColumn { .. } => {
                    ValidationResult::UnknownLayoutColumn
                }
                ManifestValidationError::InvalidRetentionPeriod { .. } => {
                    ValidationResult::InvalidRetentionPeriod
                }
            },
        );
    }
}
#[quint_connect::quint_run(
    spec = "../../specs/manifest.qnt",
    max_samples = 50,
    max_steps = 8,
    seed = "0x2"
)]
fn quint_manifest_model_matches_contract_validation() -> impl Driver {
    ManifestDriver::default()
}

fn valid_simple_manifest() -> AnalyticsManifest {
    AnalyticsManifest::new(vec![base_table("source_a", "users")])
}

fn valid_generic_document_manifest() -> AnalyticsManifest {
    let mut table = base_table("legacy_items", "legacy_items");
    table.tenant_id = None;
    table.tenant_selector = TenantSelector::None;
    table.row_identity = RowIdentity::StreamKeys;
    table.projection_columns = None;
    table.columns = Vec::new();
    table.partition_keys = Vec::new();
    table.clustering_keys = Vec::new();
    AnalyticsManifest::new(vec![table])
}

fn valid_retention_manifest() -> AnalyticsManifest {
    let mut table = base_table("source_a", "users");
    table.retention = Some(RetentionPolicy {
        period_ms: 1_000,
        timestamp: RetentionTimestamp::Attribute {
            attribute_path: "created_at_ms".to_string(),
        },
    });
    AnalyticsManifest::new(vec![table])
}

fn base_table(source_table_name: &str, analytics_table_name: &str) -> TableRegistration {
    TableRegistration {
        source_table_name: source_table_name.to_string(),
        analytics_table_name: analytics_table_name.to_string(),
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
        projection_columns: Some(vec![projection_column("email", "profile.email")]),
        columns: Vec::new(),
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
    }
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

#[allow(dead_code)]
fn explicit_column(column_name: &str) -> AnalyticsColumn {
    AnalyticsColumn {
        column_name: column_name.to_string(),
        column_type: AnalyticsColumnType::Primitive {
            primitive: PrimitiveColumnType::VarChar,
        },
    }
}

#[allow(dead_code)]
fn clustering_key(column_name: &str) -> ClusteringKey {
    ClusteringKey {
        column_name: column_name.to_string(),
        order: Some(SortOrder::Asc),
    }
}
