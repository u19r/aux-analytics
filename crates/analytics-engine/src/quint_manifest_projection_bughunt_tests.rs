use std::collections::HashSet;

use analytics_contract::{
    AnalyticsColumnType, AnalyticsManifest, PartitionKey, PrimitiveColumnType, ProjectionColumn,
    RetentionPolicy, RetentionTimestamp, RowIdentity, StorageValue, TableRegistration,
    TenantSelector,
};
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;
use storage_types::VecMap;

use crate::projection_test_support::project_item;

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct ManifestProjectionBughuntState {
    accepted_manifest: bool,
    layout_resolution_count: i64,
    reserved_column_projected: bool,
    output_sources_unique: bool,
    alias_configuration_valid: bool,
    retention_configuration_valid: bool,
}

impl State<ManifestProjectionBughuntDriver> for ManifestProjectionBughuntState {
    fn from_driver(driver: &ManifestProjectionBughuntDriver) -> Result<Self> {
        Ok(Self {
            accepted_manifest: driver.accepted_manifest,
            layout_resolution_count: driver.layout_resolution_count,
            reserved_column_projected: driver.reserved_column_projected,
            output_sources_unique: driver.output_sources_unique,
            alias_configuration_valid: driver.alias_configuration_valid,
            retention_configuration_valid: driver.retention_configuration_valid,
        })
    }
}

struct ManifestProjectionBughuntDriver {
    accepted_manifest: bool,
    layout_resolution_count: i64,
    reserved_column_projected: bool,
    output_sources_unique: bool,
    alias_configuration_valid: bool,
    retention_configuration_valid: bool,
}

impl Default for ManifestProjectionBughuntDriver {
    fn default() -> Self {
        Self {
            accepted_manifest: false,
            layout_resolution_count: 0,
            reserved_column_projected: false,
            output_sources_unique: true,
            alias_configuration_valid: true,
            retention_configuration_valid: true,
        }
    }
}

impl Driver for ManifestProjectionBughuntDriver {
    type State = ManifestProjectionBughuntState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            GenerateManifestProjectionCombination(
                has_projection_column,
                has_document_column,
                has_builtin_column,
                column_is_reserved,
                uses_alias,
                has_name_map,
                alias_is_mapped,
                has_retention,
                period_positive,
                timestamp_path_non_empty,
            ) => {
                self.check_combination(ManifestProjectionCombination {
                    has_projection_column,
                    has_document_column,
                    has_builtin_column,
                    column_is_reserved,
                    uses_alias,
                    has_name_map,
                    alias_is_mapped,
                    has_retention,
                    period_positive,
                    timestamp_path_non_empty,
                })?;
            },
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/manifest_projection_bughunt.qnt",
    max_samples = 100,
    max_steps = 12,
    seed = "0x6d"
)]
fn given_quint_manifest_projection_bughunt_model_when_replayed_then_manifest_and_projection_boundaries_matches()
-> impl Driver {
    ManifestProjectionBughuntDriver::default()
}

#[derive(Clone, Copy)]
struct ManifestProjectionCombination {
    has_projection_column: bool,
    has_document_column: bool,
    has_builtin_column: bool,
    column_is_reserved: bool,
    uses_alias: bool,
    has_name_map: bool,
    alias_is_mapped: bool,
    has_retention: bool,
    period_positive: bool,
    timestamp_path_non_empty: bool,
}

impl ManifestProjectionBughuntDriver {
    fn check_combination(&mut self, combination: ManifestProjectionCombination) -> Result {
        let manifest = manifest_for_combination(&combination);
        let validation = manifest.validate();
        let manifest_valid = validation.is_ok();
        let alias_valid = alias_configuration_valid(&combination);
        let table = &manifest.tables[0];
        let projection_result = project_item(
            &VecMap::from_iter([(
                "profile".to_string(),
                StorageValue::M(VecMap::from_iter([(
                    "email".to_string(),
                    StorageValue::S("probe@example.com".to_string()),
                )])),
            )]),
            table.projection_columns.as_deref().unwrap_or_default(),
            table.expression_attribute_names.as_ref(),
        );
        if combination.has_projection_column {
            match (alias_valid, projection_result) {
                (true, Ok(_)) | (false, Err(_)) => {}
                (true, Err(error)) => {
                    return Err(anyhow::anyhow!(
                        "production projection rejected a mapped alias: {error}"
                    ));
                }
                (false, Ok(_)) => {
                    return Err(anyhow::anyhow!(
                        "production projection accepted an unmapped alias"
                    ));
                }
            }
        }
        self.accepted_manifest = manifest_valid && alias_valid;
        let builtin_layout = table
            .partition_keys
            .iter()
            .any(|key| key.column_name == "tenant_id");
        let projection_source_count = table
            .projection_columns
            .as_deref()
            .unwrap_or_default()
            .iter()
            .filter(|column| column.column_name != "tenant_id")
            .count();
        self.layout_resolution_count = if builtin_layout {
            1
        } else {
            i64::try_from(projection_source_count).map_err(|error| {
                anyhow::anyhow!("projection source count overflowed model state: {error}")
            })? + i64::from(table.document_column.is_some())
        };
        self.reserved_column_projected = table.projection_columns.as_ref().is_some_and(|columns| {
            columns.iter().any(|column| {
                matches!(
                    column.column_name.as_str(),
                    "tenant_id" | "__id" | "table_name"
                )
            })
        });
        let mut output_sources = HashSet::new();
        self.output_sources_unique = table
            .projection_columns
            .as_deref()
            .unwrap_or_default()
            .iter()
            .all(|column| output_sources.insert(column.column_name.clone()));
        if let Some(document_column) = table.document_column.as_deref() {
            self.output_sources_unique &= output_sources.insert(document_column.to_string());
        }
        self.alias_configuration_valid = alias_valid;
        self.retention_configuration_valid = retention_manifest_valid(table);
        Ok(())
    }
}

fn retention_manifest_valid(table: &TableRegistration) -> bool {
    let mut retention_table = base_table();
    retention_table.retention = table.retention.clone();
    retention_table.partition_keys = vec![PartitionKey {
        column_name: "tenant_id".to_string(),
        bucket: None,
    }];
    AnalyticsManifest::new(vec![retention_table])
        .validate()
        .is_ok()
}

fn alias_configuration_valid(combination: &ManifestProjectionCombination) -> bool {
    let column = ProjectionColumn {
        column_name: "alias_probe".to_string(),
        attribute_path: if combination.uses_alias {
            "profile.#field".to_string()
        } else {
            "profile.email".to_string()
        },
        column_type: None,
    };
    let names = combination.has_name_map.then(|| {
        if combination.alias_is_mapped {
            VecMap::from_iter([("#field".to_string(), "email".to_string())])
        } else {
            VecMap::new()
        }
    });
    project_item(&VecMap::new(), &[column], names.as_ref()).is_ok()
}

fn manifest_for_combination(combination: &ManifestProjectionCombination) -> AnalyticsManifest {
    let mut table = base_table();
    table.document_column = combination
        .has_document_column
        .then(|| "layout_col".to_string());
    table.projection_columns = projection_columns_for_combination(combination);
    table.expression_attribute_names = combination.has_name_map.then(|| {
        if combination.alias_is_mapped {
            VecMap::from_iter([("#field".to_string(), "email".to_string())])
        } else {
            VecMap::new()
        }
    });
    table.partition_keys = vec![PartitionKey {
        column_name: if combination.has_builtin_column {
            "tenant_id".to_string()
        } else {
            "layout_col".to_string()
        },
        bucket: None,
    }];
    table.retention = combination.has_retention.then(|| RetentionPolicy {
        period_ms: if combination.period_positive {
            1_000
        } else {
            0
        },
        timestamp: RetentionTimestamp::Attribute {
            attribute_path: if combination.timestamp_path_non_empty {
                "created_at_ms".to_string()
            } else {
                " ".to_string()
            },
        },
    });
    AnalyticsManifest::new(vec![table])
}

fn projection_columns_for_combination(
    combination: &ManifestProjectionCombination,
) -> Option<Vec<ProjectionColumn>> {
    let mut columns = Vec::new();
    if combination.has_projection_column {
        columns.push(projection_column(
            "layout_col",
            projection_attribute_path(combination),
        ));
    }
    if combination.column_is_reserved {
        columns.push(projection_column("tenant_id", "tenant_id"));
    }
    (!columns.is_empty()).then_some(columns)
}

fn projection_attribute_path(combination: &ManifestProjectionCombination) -> &'static str {
    if combination.uses_alias {
        "profile.#field"
    } else {
        "profile.email"
    }
}

fn base_table() -> TableRegistration {
    TableRegistration {
        source_table_name: "source_a".to_string(),
        analytics_table_name: "events".to_string(),
        source_table_name_prefix: None,
        row_expansion: None,
        tenant_id: Some("tenant_01".to_string()),
        tenant_selector: TenantSelector::TableName,
        row_identity: RowIdentity::RecordKey,
        document_column: None,
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
