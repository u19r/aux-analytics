use analytics_quint_test_support::map_result;
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::{
    AnalyticsColumnType, AnalyticsManifest, JoinPolicy, ManifestValidationError, PartitionKey,
    PrimitiveColumnType, ProjectionColumn, RetentionPolicy, RetentionTimestamp, RowExpansion,
    RowIdentity, TableRegistration, TableScope, TenantSelector,
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
    InvalidRowExpansion,
    GlobalReferenceHasTenantScope,
    InvalidJoinPolicy,
    InvalidRowIdentity,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum ManifestCase {
    #[serde(rename = "ValidSimpleManifest")]
    ValidSimple,
    #[serde(rename = "ValidGenericDocumentManifest")]
    ValidGenericDocument,
    #[serde(rename = "ValidRetentionManifest")]
    ValidRetention,
    #[serde(rename = "UnsupportedVersionManifest")]
    UnsupportedVersion,
    #[serde(rename = "DuplicateAnalyticsTableManifest")]
    DuplicateAnalyticsTable,
    #[serde(rename = "EmptySourceTableManifest")]
    EmptySourceTable,
    #[serde(rename = "EmptyAnalyticsTableManifest")]
    EmptyAnalyticsTable,
    #[serde(rename = "ReservedProjectionColumnManifest")]
    ReservedProjectionColumn,
    #[serde(rename = "ReservedInternalRetentionColumnManifest")]
    ReservedInternalRetentionColumn,
    #[serde(rename = "DuplicateProjectionColumnManifest")]
    DuplicateProjectionColumn,
    #[serde(rename = "UnknownPartitionColumnManifest")]
    UnknownPartitionColumn,
    #[serde(rename = "ZeroRetentionPeriodManifest")]
    ZeroRetentionPeriod,
    #[serde(rename = "EmptyRetentionTimestampManifest")]
    EmptyRetentionTimestamp,
    #[serde(rename = "InvalidRowExpansionManifest")]
    InvalidRowExpansion,
    #[serde(rename = "GlobalReferenceTenantScopeManifest")]
    GlobalReferenceTenantScope,
    #[serde(rename = "InvalidJoinPolicyManifest")]
    InvalidJoinPolicy,
    #[serde(rename = "InvalidRowIdentityManifest")]
    InvalidRowIdentity,
    #[serde(rename = "NotCheckedManifest")]
    NotChecked,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct ManifestState {
    #[serde(rename = "lastResult")]
    last_result: ValidationResult,
    #[serde(rename = "lastCase")]
    last_case: ManifestCase,
}

impl State<ManifestDriver> for ManifestState {
    fn from_driver(driver: &ManifestDriver) -> Result<Self> {
        Ok(Self {
            last_result: driver.last_result.clone(),
            last_case: driver.last_case,
        })
    }
}

struct ManifestDriver {
    last_result: ValidationResult,
    last_case: ManifestCase,
}

impl Default for ManifestDriver {
    fn default() -> Self {
        Self {
            last_result: ValidationResult::NotChecked,
            last_case: ManifestCase::NotChecked,
        }
    }
}

impl Driver for ManifestDriver {
    type State = ManifestState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                self.last_result = ValidationResult::NotChecked;
                self.last_case = ManifestCase::NotChecked;
            },
            CheckValidSimpleManifest => self.check(ManifestCase::ValidSimple, &valid_simple_manifest()),
            CheckValidGenericDocumentManifest => self.check(ManifestCase::ValidGenericDocument, &valid_generic_document_manifest()),
            CheckValidRetentionManifest => self.check(ManifestCase::ValidRetention, &valid_retention_manifest()),
            CheckUnsupportedVersion => {
                let mut manifest = valid_simple_manifest();
                manifest.version = 999;
                self.check(ManifestCase::UnsupportedVersion, &manifest);
            },
            CheckDuplicateAnalyticsTableName => {
                let table = base_table("source_a", "users");
                self.check(ManifestCase::DuplicateAnalyticsTable, &AnalyticsManifest::new(vec![table.clone(), table]));
            },
            CheckEmptySourceTableName => {
                let mut table = base_table("", "users");
                table.source_table_name = " ".to_string();
                self.check(ManifestCase::EmptySourceTable, &AnalyticsManifest::new(vec![table]));
            },
            CheckEmptyAnalyticsTableName => {
                self.check(ManifestCase::EmptyAnalyticsTable, &AnalyticsManifest::new(vec![base_table("source_a", " ")]));
            },
            CheckReservedProjectionColumn => {
                let mut table = base_table("source_a", "users");
                table.projection_columns = Some(vec![projection_column("tenant_id", "tenant_id")]);
                self.check(ManifestCase::ReservedProjectionColumn, &AnalyticsManifest::new(vec![table]));
            },
            CheckReservedInternalRetentionColumn => {
                let mut table = base_table("source_a", "users");
                table.projection_attribute_names = Some(vec!["__expiry".to_string()]);
                table.projection_columns = None;
                self.check(ManifestCase::ReservedInternalRetentionColumn, &AnalyticsManifest::new(vec![table]));
            },
            CheckDuplicateProjectionColumn => {
                let mut table = base_table("source_a", "users");
                table.projection_columns = Some(vec![
                    projection_column("email", "profile.email"),
                    projection_column("email", "contact.email"),
                ]);
                self.check(ManifestCase::DuplicateProjectionColumn, &AnalyticsManifest::new(vec![table]));
            },
            CheckUnknownPartitionColumn => {
                let mut table = base_table("source_a", "users");
                table.partition_keys = vec![PartitionKey {
                    column_name: "org_id".to_string(),
                    bucket: None,
                }];
                self.check(ManifestCase::UnknownPartitionColumn, &AnalyticsManifest::new(vec![table]));
            },
            CheckZeroRetentionPeriod => {
                let mut table = base_table("source_a", "users");
                table.retention = Some(RetentionPolicy {
                    period_ms: 0,
                    timestamp: RetentionTimestamp::IngestedAt,
                });
                self.check(ManifestCase::ZeroRetentionPeriod, &AnalyticsManifest::new(vec![table]));
            },
            CheckEmptyRetentionTimestamp => {
                let mut table = base_table("source_a", "users");
                table.retention = Some(RetentionPolicy {
                    period_ms: 1,
                    timestamp: RetentionTimestamp::Attribute {
                        attribute_path: " ".to_string(),
                    },
                });
                self.check(ManifestCase::EmptyRetentionTimestamp, &AnalyticsManifest::new(vec![table]));
            },
            CheckInvalidRowExpansion => {
                let mut table = base_table("source_a", "users");
                table.row_expansion = Some(RowExpansion {
                    list_path: Vec::new(),
                    source_fields: Vec::new(),
                    element_fields: Vec::new(),
                    ordinal_attribute_name: None,
                });
                self.check(ManifestCase::InvalidRowExpansion, &AnalyticsManifest::new(vec![table]));
            },
            CheckGlobalReferenceTenantScope => {
                let mut table = base_table("source_a", "users");
                table.table_scope = TableScope::GlobalReference { reference_class: "catalog".to_string() };
                self.check(ManifestCase::GlobalReferenceTenantScope, &AnalyticsManifest::new(vec![table]));
            },
            CheckInvalidJoinPolicy => {
                let mut table = base_table("source_a", "users");
                table.join_policy = JoinPolicy { allowed_as_primary: true, allowed_as_join: false, max_join_rows_hint: Some(0) };
                self.check(ManifestCase::InvalidJoinPolicy, &AnalyticsManifest::new(vec![table]));
            },
            CheckInvalidRowIdentity => {
                let mut table = base_table("source_a", "users");
                table.row_identity = RowIdentity::Attributes { attribute_names: vec!["id".to_string(), "id".to_string()] };
                self.check(ManifestCase::InvalidRowIdentity, &AnalyticsManifest::new(vec![table]));
            },
        })
    }
}

impl ManifestDriver {
    fn check(&mut self, case: ManifestCase, manifest: &AnalyticsManifest) {
        self.last_case = case;
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
                ManifestValidationError::InvalidRowExpansion { .. }
                | ManifestValidationError::DuplicateRowExpansionAttribute { .. }
                | ManifestValidationError::UnknownRowExpansionOutput { .. } => {
                    ValidationResult::InvalidRowExpansion
                }
                ManifestValidationError::GlobalReferenceHasTenantScope { .. } => {
                    ValidationResult::GlobalReferenceHasTenantScope
                }
                ManifestValidationError::InvalidJoinPolicy { .. } => {
                    ValidationResult::InvalidJoinPolicy
                }
                ManifestValidationError::InvalidRowIdentity { .. } => {
                    ValidationResult::InvalidRowIdentity
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
fn given_quint_manifest_model_when_replayed_then_contract_validation_matches() -> impl Driver {
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
        row_expansion: None,
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
        table_scope: crate::TableScope::default(),
        join_policy: crate::JoinPolicy::default(),
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
