use std::collections::HashMap;

use analytics_contract::{
    AnalyticsColumnType, AnalyticsManifest, PartitionKey, PrimitiveColumnType, ProjectionColumn,
    RetentionPolicy, RetentionTimestamp, RowIdentity, TableRegistration, TenantSelector,
};
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::projection::project_item;

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
#[allow(clippy::struct_excessive_bools)]
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

#[allow(clippy::struct_excessive_bools)]
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
            accepted_manifest: true,
            layout_resolution_count: 1,
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
                });
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
fn quint_manifest_projection_bughunt_model_matches_manifest_and_projection_boundaries()
-> impl Driver {
    ManifestProjectionBughuntDriver::default()
}

#[derive(Clone, Copy)]
#[allow(clippy::struct_excessive_bools)]
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
    fn check_combination(&mut self, combination: ManifestProjectionCombination) {
        let manifest = manifest_for_combination(&combination);
        let manifest_valid = manifest.validate().is_ok();
        let alias_valid = alias_configuration_valid(&combination);
        self.accepted_manifest = manifest_valid && alias_valid;
        self.layout_resolution_count = layout_resolution_count(&combination);
        self.reserved_column_projected = combination.column_is_reserved;
        self.output_sources_unique =
            !(combination.has_projection_column && combination.has_document_column);
        self.alias_configuration_valid =
            !combination.uses_alias || (combination.has_name_map && combination.alias_is_mapped);
        self.retention_configuration_valid = !combination.has_retention
            || (combination.period_positive && combination.timestamp_path_non_empty);
    }
}

fn layout_resolution_count(combination: &ManifestProjectionCombination) -> i64 {
    if combination.has_builtin_column {
        1
    } else {
        i64::from(combination.has_projection_column) + i64::from(combination.has_document_column)
    }
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
            HashMap::from([("#field".to_string(), "email".to_string())])
        } else {
            HashMap::new()
        }
    });
    project_item(&HashMap::new(), &[column], names.as_ref()).is_ok()
}

fn manifest_for_combination(combination: &ManifestProjectionCombination) -> AnalyticsManifest {
    let mut table = base_table();
    table.document_column = combination
        .has_document_column
        .then(|| "layout_col".to_string());
    table.projection_columns = projection_columns_for_combination(combination);
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
