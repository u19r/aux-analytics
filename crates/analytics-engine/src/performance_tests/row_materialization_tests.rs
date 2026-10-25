use std::{
    collections::BTreeMap,
    hint::black_box,
    io::{self, Write},
};

use analytics_contract::{
    AnalyticsManifest, INTERNAL_EXPIRY_COLUMN, INTERNAL_INGESTED_AT_COLUMN,
    INTERNAL_MISSING_RETENTION_COLUMN,
};
use storage_types::{SystemStreamCheckpointToken, SystemStreamPosition, SystemStreamScope, VecMap};

use super::*;
use crate::SourceBatchRecord;

const PROBE_BATCH_SIZES: [usize; 4] = [1, 25, 100, 500];
const FULL_BATCH_PROBE_SIZES: [usize; 2] = [100, 500];

#[test]
#[ignore = "debug-only row materialization allocation regression"]
fn given_registered_rows_when_parameters_and_table_resolution_are_measured_then_outputs_match_and_each_has_a_cpu_or_allocation_win()
 {
    let workload = super::realistic_workload(super::REALISTIC_RECORD_COUNT);
    assert_realistic_shape(&workload);
    let table = &workload.manifest.tables[0];
    let rows = super::realistic_batch_rows(&workload, super::REALISTIC_RECORD_COUNT);
    let document_table = document_table(table);
    let query = super::realistic_query(super::REALISTIC_QUERY_LIMITS[3]);

    println!(
        "row-materialization-fixture | records={} | attributes={} | key_bytes=100 | \
         payload_bytes=1200 | gsi_families=2 | ttl=true | sizes=1,25,100,500 | paired_samples={}",
        workload.records.len(),
        super::REALISTIC_ATTRIBUTE_COUNT,
        super::PRODUCTION_PAIRED_SAMPLES,
    );

    let results = vec![
        super::measure_production_candidate(
            "row value capacity",
            "engine.rs::update_row::parameter_values_capacity",
            || update_values_probe(&rows, table, false),
            || update_values_probe(&rows, table, true),
        ),
        super::measure_production_candidate(
            "table resolution scan",
            "structured_query.rs::resolve_table_for_(column|document_path)::bounded_match_scan",
            || query_resolution_probe(&workload.manifest, &document_table, &query, false),
            || query_resolution_probe(&workload.manifest, &document_table, &query, true),
        ),
    ];
    super::print_production_candidate_results("row-materialization-candidate", &results);
    let retained = results
        .iter()
        .filter(|result| result.kept)
        .map(|result| result.name)
        .collect::<Vec<_>>();
    assert_eq!(retained, ["row value capacity", "table resolution scan"]);
    super::print_production_summary("row-materialization-candidate", &results);
    assert_eq!(results.len(), 2);
}

fn assert_realistic_shape(workload: &super::RealisticWorkload) {
    let record = &workload.records[0];
    let item = record.record.new_image.as_ref().expect("fixture image");
    assert_eq!(item.len(), super::REALISTIC_ATTRIBUTE_COUNT);
    assert_eq!(record.record_key.len(), 100);
    let payload_bytes = ["payload_a", "payload_b", "payload_c"]
        .into_iter()
        .map(|name| match item.get(name).expect("payload") {
            StorageValue::S(value) => value.len(),
            _ => 0,
        })
        .sum::<usize>();
    assert_eq!(payload_bytes, super::REALISTIC_PAYLOAD_BYTES);
}

#[test]
fn given_document_attributes_when_json_materialized_then_keys_have_canonical_order() {
    let first = VecMap::from_iter([
        ("z".to_string(), StorageValue::S("last".to_string())),
        ("a".to_string(), StorageValue::S("first".to_string())),
    ]);
    let second = VecMap::from_iter([
        ("a".to_string(), StorageValue::S("first".to_string())),
        ("z".to_string(), StorageValue::S("last".to_string())),
    ]);
    let first_json =
        crate::row::performance_storage_item_to_json(&first).expect("first item converts");
    let second_json =
        crate::row::performance_storage_item_to_json(&second).expect("second item converts");
    assert_eq!(first_json, second_json);
    assert_eq!(
        serde_json::to_string(&first_json).expect("first JSON serializes"),
        r#"{"a":"first","z":"last"}"#
    );
}

#[test]
fn given_registration_shapes_when_batch_rows_materialize_then_values_match_json_bridge() {
    let workload = super::realistic_workload(1);
    let record = &workload.records[0];
    let item = record.record.new_image.as_ref().expect("fixture image");
    let engine = super::realistic_engine(&workload);
    let mut projection_attributes = workload.manifest.tables[0].clone();
    projection_attributes.projection_columns = None;
    projection_attributes.projection_attribute_names =
        Some(vec!["status".to_owned(), "ttl".to_owned()]);
    projection_attributes.columns.clear();

    let mut retention_table = super::realistic_row_table(&workload);
    retention_table.retention = Some(analytics_contract::RetentionPolicy {
        period_ms: 60_000,
        timestamp: analytics_contract::RetentionTimestamp::IngestedAt,
    });

    let cases = [
        (
            "explicit columns",
            super::realistic_row_table(&workload),
            None,
        ),
        (
            "document column",
            super::realistic_document_row_table(&workload),
            None,
        ),
        (
            "projection columns",
            workload.manifest.tables[0].clone(),
            None,
        ),
        ("projection attributes", projection_attributes, None),
        (
            "retention",
            retention_table.clone(),
            Some(crate::engine::IngestRetention {
                period_ms: Some(60_000),
                timestamp: analytics_contract::RetentionTimestamp::IngestedAt,
                missing_retention: false,
            }),
        ),
        ("retention delete", retention_table, None),
    ];

    for (label, table, retention) in cases {
        let source = RowSource {
            stream_key_digest: None,
            source_table_name: &record.source_table_name,
            record_key: &record.record_key,
            record_keys: &record.record.keys,
        };
        let position = analytics_contract::SourcePosition::AuxStorageStreamCursor {
            stream_name: "fixture".to_owned(),
            cursor: "1".to_owned(),
        };
        let json_row = engine
            .row_from_item(&table, source, item, 1_900_000_000, retention.as_ref())
            .unwrap_or_else(|error| panic!("{label}: JSON row materialization failed: {error}"));
        let expected = baseline_batch_row_values(&table, &json_row, Some(&position))
            .unwrap_or_else(|error| panic!("{label}: JSON bridge failed: {error}"));
        let actual = engine
            .batch_row_from_item(&table, source, item, 1_900_000_000, retention.as_ref())
            .unwrap_or_else(|error| panic!("{label}: direct row materialization failed: {error}"))
            .into_values(Some(&position))
            .expect("direct values materialize");
        if label == "retention" {
            let retention_start = actual.len() - 3;
            assert_eq!(
                &actual[retention_start..],
                &[
                    duckdb::types::Value::BigInt(1_900_000_000),
                    duckdb::types::Value::BigInt(1_900_060_000),
                    duckdb::types::Value::Boolean(false),
                ],
                "retention values must follow ingested, expiry, missing order",
            );
            let mut expected = expected;
            expected[retention_start] = duckdb::types::Value::BigInt(1_900_000_000);
            expected[retention_start + 1] = duckdb::types::Value::BigInt(1_900_060_000);
            assert_eq!(actual, expected, "batch row values differ for {label}");
        } else {
            assert_eq!(actual, expected, "batch row values differ for {label}");
        }
    }
}

#[test]
fn given_typed_and_nested_attributes_when_batch_row_materializes_then_json_bridge_is_exact() {
    let workload = super::realistic_workload(1);
    let record = &workload.records[0];
    let mut item = record
        .record
        .new_image
        .as_ref()
        .expect("fixture image")
        .clone();
    item.insert("decimal".to_owned(), StorageValue::N("12.3400".to_owned()));
    item.insert(
        "occurred_at".to_owned(),
        StorageValue::N("1900000000000".to_owned()),
    );
    item.insert(
        "labels".to_owned(),
        StorageValue::L(vec![
            StorageValue::S("blue".to_owned()),
            StorageValue::N("2".to_owned()),
        ]),
    );
    item.insert(
        "metadata".to_owned(),
        StorageValue::M(VecMap::from_iter([
            ("z".to_owned(), StorageValue::BOOL(true)),
            ("a".to_owned(), StorageValue::SS(vec!["x".to_owned()])),
        ])),
    );
    item.insert(
        "number_set".to_owned(),
        StorageValue::NS(vec!["1.0".to_owned(), "2".to_owned()]),
    );
    let mut table = super::realistic_row_table(&workload);
    table.columns = vec![
        analytics_contract::AnalyticsColumn {
            column_name: "decimal".to_owned(),
            column_type: AnalyticsColumnType::Primitive {
                primitive: PrimitiveColumnType::Decimal,
            },
        },
        analytics_contract::AnalyticsColumn {
            column_name: "occurred_at".to_owned(),
            column_type: AnalyticsColumnType::Primitive {
                primitive: PrimitiveColumnType::Timestamp,
            },
        },
        analytics_contract::AnalyticsColumn {
            column_name: "labels".to_owned(),
            column_type: AnalyticsColumnType::List {
                primitive: PrimitiveColumnType::VarChar,
            },
        },
        analytics_contract::AnalyticsColumn {
            column_name: "metadata".to_owned(),
            column_type: AnalyticsColumnType::Map {
                key_type: "VARCHAR".to_owned(),
                value_type: Box::new(AnalyticsColumnType::Primitive {
                    primitive: PrimitiveColumnType::Json,
                }),
            },
        },
        analytics_contract::AnalyticsColumn {
            column_name: "number_set".to_owned(),
            column_type: AnalyticsColumnType::Primitive {
                primitive: PrimitiveColumnType::Json,
            },
        },
    ];
    table.projection_columns = None;
    let engine = super::realistic_engine(&workload);
    let source = RowSource {
        stream_key_digest: None,
        source_table_name: &record.source_table_name,
        record_key: &record.record_key,
        record_keys: &record.record.keys,
    };
    let json_row = engine
        .row_from_item(&table, source, &item, 1_900_000_000, None)
        .expect("JSON row materializes");
    let expected =
        baseline_batch_row_values(&table, &json_row, None).expect("JSON bridge materializes");
    let actual = engine
        .batch_row_from_item(&table, source, &item, 1_900_000_000, None)
        .expect("direct row materializes")
        .into_values(None)
        .expect("direct values materialize");
    assert_eq!(actual, expected);
}

#[test]
fn given_unsupported_null_when_batch_row_materializes_then_attribute_name_is_preserved() {
    let workload = super::realistic_workload(1);
    let record = &workload.records[0];
    let mut item = record
        .record
        .new_image
        .as_ref()
        .expect("fixture image")
        .clone();
    item.insert("invalid".to_owned(), StorageValue::NULL(false));
    let table = super::realistic_row_table(&workload);
    let engine = super::realistic_engine(&workload);
    let source = RowSource {
        stream_key_digest: None,
        source_table_name: &record.source_table_name,
        record_key: &record.record_key,
        record_keys: &record.record.keys,
    };
    let json_error = engine
        .row_from_item(&table, source, &item, 1_900_000_000, None)
        .expect_err("JSON path rejects unsupported null");
    let direct_error = match engine.batch_row_from_item(&table, source, &item, 1_900_000_000, None)
    {
        Ok(_) => panic!("direct path accepts unsupported null"),
        Err(error) => error,
    };
    assert!(matches!(
        json_error,
        crate::AnalyticsEngineError::AttributeConversion(ref name) if name == "invalid"
    ));
    assert!(matches!(
        direct_error,
        crate::AnalyticsEngineError::AttributeConversion(ref name) if name == "invalid"
    ));
}

fn document_table(table: &TableRegistration) -> TableRegistration {
    let mut table = table.clone();
    table.document_column = Some("item".to_owned());
    table.columns = super::columns_for_registration(&table);
    table
}

fn update_values_probe(
    rows: &[BTreeMap<String, serde_json::Value>],
    table: &TableRegistration,
    optimized: bool,
) -> ProductionProbe {
    super::combine_probes(PROBE_BATCH_SIZES.into_iter().map(|size| {
        let mut digest = 1469598103934665603_u64;
        let mut count = 0_u64;
        for row in &rows[..size] {
            let columns = row
                .keys()
                .filter(|column| !matches!(column.as_str(), "tenant_id" | "__id" | "table_name"))
                .map(String::as_str)
                .collect::<Vec<_>>();
            let mut values = if optimized {
                AnalyticsEngine::performance_update_values(table, row, &columns)
            } else {
                baseline_update_values(table, row, &columns)
            };
            values.push(crate::engine::json_value_to_duckdb_value(
                row.get("table_name").expect("table name"),
            ));
            values.push(crate::engine::json_value_to_duckdb_value(
                row.get("tenant_id").expect("tenant id"),
            ));
            values.push(crate::engine::json_value_to_duckdb_value(
                row.get("__id").expect("row id"),
            ));
            count = count.saturating_add(values.len() as u64);
            digest = super::digest_duckdb_values(&values, digest);
            black_box(values);
        }
        ProductionProbe {
            output_digest: digest,
            serialized_bytes: count,
        }
    }))
}

fn baseline_update_values(
    table: &TableRegistration,
    row: &BTreeMap<String, serde_json::Value>,
    columns: &[&str],
) -> Vec<duckdb::types::Value> {
    let mut values = columns
        .iter()
        .map(|column| {
            crate::engine::json_value_to_duckdb_value_for_column(
                row.get(*column).unwrap_or(&serde_json::Value::Null),
                crate::engine::column_type_for_name(table, column).as_ref(),
            )
        })
        .collect::<Vec<_>>();
    values.reserve(3);
    values
}

fn query_resolution_probe(
    manifest: &AnalyticsManifest,
    document_table: &TableRegistration,
    query: &StructuredQuery,
    optimized: bool,
) -> ProductionProbe {
    let manifest = if document_table.document_column.is_some() {
        AnalyticsManifest::new(vec![document_table.clone()])
    } else {
        manifest.clone()
    };
    super::combine_probes(super::REALISTIC_QUERY_LIMITS.into_iter().map(|size| {
        let mut digest = 1469598103934665603_u64;
        let names = [
            "tenant_id",
            "gsi1_pk",
            "gsi1_sk",
            "gsi2_pk",
            "gsi2_sk",
            "payload_a",
            "payload_b",
            "payload_c",
            "status",
            "ttl",
        ];
        for column in names.into_iter().cycle().take(size) {
            let alias = crate::structured_query::performance_resolve_table_for_column(
                &manifest, query, column, optimized,
            )
            .expect("column resolution");
            digest = super::digest_bytes_from(digest, alias.as_bytes());
        }
        let document_alias = crate::structured_query::performance_resolve_table_for_document_path(
            &manifest, query, "item", optimized,
        )
        .expect("document resolution");
        digest = super::digest_bytes_from(digest, document_alias.as_bytes());
        ProductionProbe {
            output_digest: digest,
            serialized_bytes: size as u64,
        }
    }))
}

#[test]
#[ignore = "debug-only paired row conversion allocation/CPU regression"]
fn given_borrowed_items_when_rows_are_materialized_then_outputs_match_and_a_cpu_or_allocation_win_is_measured()
 {
    let workload = super::realistic_workload(super::REALISTIC_RECORD_COUNT);
    let table = super::realistic_row_table(&workload);
    let candidate_engine = super::realistic_engine(&workload);
    let result = super::measure_production_candidate(
        "row item conversion",
        "row.rs::row_from_item::borrowed_item",
        || baseline_row_probe(&table, &workload),
        || production_row_probe(&candidate_engine, &table, &workload),
    );
    super::print_production_candidate_results(
        "row-item-conversion-candidate",
        std::slice::from_ref(&result),
    );
    assert_eq!(result.name, "row item conversion");
    assert_eq!(result.owner, "row.rs::row_from_item::borrowed_item");
    assert!(result.kept);
}

#[test]
#[ignore = "debug-only paired batch row conversion allocation/CPU regression"]
fn given_items_when_batch_rows_are_converted_then_outputs_match_and_a_cpu_or_allocation_win_is_measured()
 {
    let workload = super::realistic_workload(super::REALISTIC_RECORD_COUNT);
    let table = super::realistic_row_table(&workload);
    let baseline_engine = super::realistic_engine(&workload);
    let candidate_engine = super::realistic_engine(&workload);
    let result = super::measure_production_candidate(
        "batch row conversion",
        "row.rs::batch_row_from_item::direct_storage_values",
        || baseline_batch_row_probe(&baseline_engine, &table, &workload),
        || production_batch_row_probe(&candidate_engine, &table, &workload),
    );
    super::print_production_candidate_results(
        "batch-row-conversion-candidate",
        std::slice::from_ref(&result),
    );
    assert_eq!(result.name, "batch row conversion");
    assert_eq!(
        result.owner,
        "row.rs::batch_row_from_item::direct_storage_values"
    );
    assert!(result.kept);
}

#[test]
#[ignore = "debug-only paired projection-attribute batch row allocation regression"]
fn given_projected_attributes_when_batch_rows_are_converted_then_outputs_match_and_a_cpu_or_allocation_win_is_measured()
 {
    let workload = super::realistic_workload(super::REALISTIC_RECORD_COUNT);
    let mut table = workload.manifest.tables[0].clone();
    table.projection_columns = None;
    table.projection_attribute_names = Some(vec!["status".to_owned(), "ttl".to_owned()]);
    table.columns.clear();
    let baseline_engine = super::realistic_engine(&workload);
    let candidate_engine = super::realistic_engine(&workload);
    let result = super::measure_production_candidate(
        "projection attribute batch row",
        "row/batch.rs::projection_attribute_names::borrowed_validation",
        || baseline_batch_row_probe(&baseline_engine, &table, &workload),
        || production_batch_row_probe(&candidate_engine, &table, &workload),
    );
    super::print_production_candidate_results(
        "projection-attribute-batch-row-candidate",
        std::slice::from_ref(&result),
    );
    assert_eq!(result.name, "projection attribute batch row");
    assert_eq!(
        result.owner,
        "row/batch.rs::projection_attribute_names::borrowed_validation"
    );
    assert!(result.kept);
}

#[test]
#[ignore = "debug-only paired full batch ingestion allocation/CPU regression"]
fn given_record_batches_when_ingested_then_outputs_match_and_each_batch_has_a_cpu_or_allocation_win()
 {
    let mut results = Vec::with_capacity(FULL_BATCH_PROBE_SIZES.len());
    for count in FULL_BATCH_PROBE_SIZES {
        let workload = super::realistic_workload(count);
        let table = workload.manifest.tables[0].clone();
        let baseline_engine = super::realistic_engine(&workload);
        let candidate_engine = super::realistic_engine(&workload);
        let records = workload.records.as_slice();
        let result = super::measure_production_candidate(
            "full batch ingestion",
            "engine/conversion.rs::batch_sql_executor::append_flush_query",
            || full_batch_probe(&baseline_engine, &table, records, false),
            || full_batch_probe(&candidate_engine, &table, records, true),
        );
        results.push(result);
    }
    super::print_production_candidate_results("full-batch-ingestion-candidate", &results);
    assert!(results.iter().all(|result| result.kept));
}

#[test]
#[ignore = "debug-only paired direct/SystemStream immediate engine insertion/query/frontier \
            allocation/CPU regression"]
fn given_direct_and_system_stream_records_when_immediate_ingestion_is_measured_then_outputs_match_and_both_have_a_cpu_or_allocation_win()
 {
    let workload = super::realistic_workload(100);
    let table = workload.manifest.tables[0].clone();
    let direct_baseline_engine = super::realistic_engine(&workload);
    let direct_candidate_engine = super::realistic_engine(&workload);
    let direct_records = workload.records.clone();
    let direct_result = super::measure_production_candidate(
        "direct immediate engine insertion/query",
        "engine/immediate_ingest.rs::typed_immediate_writer",
        || {
            direct_ingestion_probe(
                &direct_baseline_engine,
                &workload.manifest,
                &table,
                &direct_records,
                false,
            )
        },
        || {
            direct_ingestion_probe(
                &direct_candidate_engine,
                &workload.manifest,
                &table,
                &direct_records,
                true,
            )
        },
    );
    super::print_production_candidate_results(
        "direct-immediate-engine-candidate",
        std::slice::from_ref(&direct_result),
    );
    let system_baseline_engine = super::realistic_engine(&workload);
    let system_candidate_engine = super::realistic_engine(&workload);
    let scope = SystemStreamScope::try_new("perf-system", 1).expect("system scope");
    let checkpoint = SystemStreamCheckpointToken::try_from(format!("v1.{}", "a".repeat(32)))
        .expect("system checkpoint");
    let system_records = system_records(&workload);
    let system_result = super::measure_production_candidate(
        "SystemStream immediate engine insertion/query/frontier",
        "engine/source_ingest_checkpoints.rs::typed_immediate_writer",
        || {
            system_ingestion_probe(
                &system_baseline_engine,
                &workload.manifest,
                &table,
                &system_records,
                &scope,
                &checkpoint,
                false,
            )
        },
        || {
            system_ingestion_probe(
                &system_candidate_engine,
                &workload.manifest,
                &table,
                &system_records,
                &scope,
                &checkpoint,
                true,
            )
        },
    );
    super::print_production_candidate_results(
        "system-immediate-engine-candidate",
        std::slice::from_ref(&system_result),
    );
    assert!(direct_result.kept);
    assert!(system_result.kept);
}

#[test]
#[ignore = "debug-only document JSON allocation regression"]
fn given_document_items_when_json_materialization_is_measured_then_outputs_match_and_costs_are_recorded()
 {
    let workload = super::realistic_workload(super::REALISTIC_RECORD_COUNT);
    let items = workload
        .records
        .iter()
        .map(|record| record.record.new_image.as_ref().expect("fixture image"))
        .collect::<Vec<_>>();
    let result = super::measure_production_candidate(
        "document JSON conversion",
        "row.rs::storage_item_to_json::direct_sorted_map",
        || baseline_document_json_probe(&items),
        || production_document_json_probe(&items),
    );
    super::print_production_candidate_results(
        "document-json-candidate",
        std::slice::from_ref(&result),
    );
    assert_eq!(result.name, "document JSON conversion");
    assert_eq!(
        result.owner,
        "row.rs::storage_item_to_json::direct_sorted_map"
    );
}

fn production_row_probe(
    engine: &AnalyticsEngine,
    table: &TableRegistration,
    workload: &super::RealisticWorkload,
) -> super::ProductionProbe {
    let rows = workload.records.iter().map(|record| {
        let item = record.record.new_image.as_ref().expect("fixture image");
        engine
            .row_from_item(
                table,
                RowSource {
                    stream_key_digest: None,
                    source_table_name: &record.source_table_name,
                    record_key: &record.record_key,
                    record_keys: &record.record.keys,
                },
                item,
                1_900_000_000,
                None,
            )
            .expect("fixture row materializes")
    });
    digest_rows(rows)
}

fn baseline_batch_row_values(
    table: &TableRegistration,
    row: &BTreeMap<String, serde_json::Value>,
    source_position: Option<&analytics_contract::SourcePosition>,
) -> crate::AnalyticsEngineResult<Vec<duckdb::types::Value>> {
    let mut values = vec![
        crate::engine::json_value_to_duckdb_value(
            row.get("table_name").unwrap_or(&serde_json::Value::Null),
        ),
        crate::engine::json_value_to_duckdb_value(
            row.get("tenant_id").unwrap_or(&serde_json::Value::Null),
        ),
        crate::engine::json_value_to_duckdb_value(
            row.get("__id").unwrap_or(&serde_json::Value::Null),
        ),
        source_position
            .map(|position| serde_json::to_string(position).map(duckdb::types::Value::Text))
            .transpose()?
            .unwrap_or(duckdb::types::Value::Null),
    ];
    let mut registered = vec![
        "table_name".to_owned(),
        "tenant_id".to_owned(),
        "__id".to_owned(),
        crate::constants::SOURCE_POSITION_COLUMN.to_owned(),
    ];
    for column in crate::engine::columns_for_registration(table) {
        if registered.iter().any(|name| name == &column.column_name) {
            continue;
        }
        registered.push(column.column_name.clone());
        values.push(crate::engine::json_value_to_duckdb_value_for_column(
            row.get(&column.column_name)
                .unwrap_or(&serde_json::Value::Null),
            Some(&column.column_type),
        ));
    }
    if table.retention.is_some() {
        values.extend([
            crate::engine::json_value_to_duckdb_value(
                row.get(INTERNAL_INGESTED_AT_COLUMN)
                    .unwrap_or(&serde_json::Value::Null),
            ),
            crate::engine::json_value_to_duckdb_value(
                row.get(INTERNAL_EXPIRY_COLUMN)
                    .unwrap_or(&serde_json::Value::Null),
            ),
            crate::engine::json_value_to_duckdb_value(
                row.get(INTERNAL_MISSING_RETENTION_COLUMN)
                    .unwrap_or(&serde_json::Value::Null),
            ),
        ]);
    }
    Ok(values)
}

fn baseline_batch_row_probe(
    engine: &AnalyticsEngine,
    table: &TableRegistration,
    workload: &super::RealisticWorkload,
) -> super::ProductionProbe {
    let rows = workload.records.iter().map(|record| {
        let item = record.record.new_image.as_ref().expect("fixture image");
        let row = engine
            .row_from_item(
                table,
                RowSource {
                    stream_key_digest: None,
                    source_table_name: &record.source_table_name,
                    record_key: &record.record_key,
                    record_keys: &record.record.keys,
                },
                item,
                1_900_000_000,
                None,
            )
            .expect("fixture row materializes");
        baseline_batch_row_values(table, &row, None).expect("JSON bridge materializes")
    });
    digest_batch_rows(rows)
}

fn production_batch_row_probe(
    engine: &AnalyticsEngine,
    table: &TableRegistration,
    workload: &super::RealisticWorkload,
) -> super::ProductionProbe {
    let rows = workload.records.iter().map(|record| {
        let item = record.record.new_image.as_ref().expect("fixture image");
        engine
            .batch_row_from_item(
                table,
                RowSource {
                    stream_key_digest: None,
                    source_table_name: &record.source_table_name,
                    record_key: &record.record_key,
                    record_keys: &record.record.keys,
                },
                item,
                1_900_000_000,
                None,
            )
            .expect("fixture row materializes")
            .into_values(None)
            .expect("direct values materialize")
    });
    digest_batch_rows(rows)
}

fn full_batch_probe(
    engine: &AnalyticsEngine,
    table: &TableRegistration,
    records: &[super::StreamRecordBatchItem],
    direct_values: bool,
) -> super::ProductionProbe {
    engine
        .performance_batch_ingest_for_test(table, records, direct_values)
        .expect("full batch ingestion succeeds");
    let rows = engine
        .query_unscoped_sql_json_without_timeout(
            "SELECT * FROM \"realistic_items\" ORDER BY \"table_name\", \"tenant_id\", \"__id\"",
        )
        .expect("full batch query succeeds");
    let serialized = serde_json::to_vec(&rows).expect("full batch result serializes");
    super::ProductionProbe {
        output_digest: super::digest_bytes(&serialized),
        serialized_bytes: serialized.len() as u64,
    }
}

fn direct_ingestion_probe(
    engine: &AnalyticsEngine,
    manifest: &AnalyticsManifest,
    table: &TableRegistration,
    records: &[StreamRecordBatchItem],
    direct_values: bool,
) -> super::ProductionProbe {
    engine
        .performance_reset_source_destination_for_test(table.analytics_table_name.as_str())
        .expect("reset direct destination");
    let mut outcomes = Vec::with_capacity(records.len());
    for (index, item) in records.iter().enumerate() {
        let source_position = analytics_contract::SourcePosition::AuxStorageStreamCursor {
            stream_name: "perf-direct".to_string(),
            cursor: index.to_string(),
        };
        let outcome = engine
            .performance_immediate_ingest_for_test(
                manifest,
                item,
                Some(&source_position),
                direct_values,
            )
            .expect("direct ingestion");
        outcomes.push(outcome);
    }
    ingestion_probe_output(engine, table, count_outcomes(&outcomes), None)
}

fn system_records(workload: &super::RealisticWorkload) -> Vec<SourceBatchRecord> {
    workload
        .records
        .iter()
        .enumerate()
        .map(|(index, item)| {
            let position =
                SystemStreamPosition::try_new("logical-shard", "lineage-a", index.to_string())
                    .expect("system position");
            let mut item = item.clone();
            item.shard_id = Some("logical-shard".to_string());
            item.system_position = Some(position);
            SourceBatchRecord {
                item,
                retention: None,
            }
        })
        .collect()
}

fn system_ingestion_probe(
    engine: &AnalyticsEngine,
    manifest: &AnalyticsManifest,
    table: &TableRegistration,
    records: &[SourceBatchRecord],
    scope: &SystemStreamScope,
    checkpoint: &SystemStreamCheckpointToken,
    direct_values: bool,
) -> super::ProductionProbe {
    engine
        .performance_reset_source_destination_for_test(table.analytics_table_name.as_str())
        .expect("reset SystemStream destination");
    let disposition = engine
        .performance_system_ingest_for_test(manifest, records, scope, checkpoint, direct_values)
        .expect("actual fenced SystemStream ingestion");
    let crate::SourceEffectDisposition::Applied { summary } = disposition else {
        panic!("fresh SystemStream performance batch must apply");
    };
    assert_eq!(summary.inserted(), records.len() as u64);
    ingestion_probe_output(
        engine,
        table,
        [
            summary.inserted(),
            summary.updated(),
            summary.deleted(),
            summary.skipped(),
        ],
        Some((scope, checkpoint)),
    )
}

fn ingestion_probe_output(
    engine: &AnalyticsEngine,
    table: &TableRegistration,
    outcome_counts: [u64; 4],
    system_frontier: Option<(&SystemStreamScope, &SystemStreamCheckpointToken)>,
) -> super::ProductionProbe {
    let rows = engine
        .query_unscoped_sql_json_without_timeout(
            format!(
                "SELECT * FROM {} ORDER BY table_name, tenant_id, __id",
                crate::sql::quote_identifier(table.analytics_table_name.as_str())
            )
            .as_str(),
        )
        .expect("ingestion query");
    let positions = engine
        .query_unscoped_sql_json_without_timeout(
            "SELECT analytics_table_name, source_table_name, tenant_id,
                    source_row_id, source_position_json, row_visible
             FROM __analytics_row_source_positions
             ORDER BY analytics_table_name, source_table_name, tenant_id, source_row_id",
        )
        .expect("source-position query");
    let frontier = system_frontier.and_then(|_| {
        engine
            .load_system_stream_checkpoint()
            .expect("system frontier")
            .map(|(scope, checkpoint)| {
                format!(
                    "{}:{}:{}:{}",
                    scope.scope_id().as_str(),
                    scope.generation(),
                    scope.version(),
                    checkpoint.as_str()
                )
            })
    });
    let owners = engine
        .query_unscoped_sql_json_without_timeout(
            "SELECT analytics_table_name, source_table_name, source_key_digest,
                source_position_json, child_identities_json
         FROM __analytics_source_record_states ORDER BY source_key_digest",
        )
        .expect("physical owner snapshot");
    let authorities = engine
        .query_unscoped_sql_json_without_timeout(
            "SELECT authority_version, scope_version, scope_id, generation, fence, \
             hex(execution_id) AS execution_id
         FROM __analytics_system_source_effect_authority ORDER BY scope_id",
        )
        .expect("destination authority snapshot");
    let receipts = engine
        .query_unscoped_sql_json_without_timeout(
            "SELECT receipt_version, hex(execution_id) AS execution_id, scope_version, scope_id, \
             generation, \"checkpoint\",
                fence, inserted_count, updated_count, deleted_count, skipped_count,
                dropped_fields, checkpoint_count, hex(content_digest) AS content_digest
         FROM __analytics_system_source_effect_receipts ORDER BY execution_id",
        )
        .expect("effect receipt snapshot");
    if system_frontier.is_some() {
        assert_eq!(owners.len() as u64, outcome_counts[0]);
        assert_eq!(authorities.len(), 1);
        assert_eq!(receipts.len(), 1);
    }
    let serialized = serde_json::to_vec(&serde_json::json!({
        "rows": rows,
        "positions": positions,
        "frontier": frontier,
        "outcome_counts": outcome_counts,
        "owners": owners,
        "authorities": authorities,
        "receipts": receipts,
    }))
    .expect("ingestion probe serializes");
    super::ProductionProbe {
        output_digest: super::digest_bytes(&serialized),
        serialized_bytes: serialized.len() as u64,
    }
}

fn count_outcomes(outcomes: &[IngestOutcome]) -> [u64; 4] {
    outcomes.iter().fold([0_u64; 4], |mut counts, outcome| {
        counts[match outcome {
            IngestOutcome::Inserted => 0,
            IngestOutcome::Updated => 1,
            IngestOutcome::Deleted => 2,
            IngestOutcome::Skipped => 3,
        }] += 1;
        counts
    })
}

fn digest_batch_rows(
    rows: impl Iterator<Item = Vec<duckdb::types::Value>>,
) -> super::ProductionProbe {
    let mut output_digest = 1469598103934665603_u64;
    let mut serialized_bytes = 0_u64;
    for values in rows {
        output_digest = super::digest_duckdb_values(&values, output_digest);
        serialized_bytes = serialized_bytes.saturating_add(values.len() as u64);
    }
    super::ProductionProbe {
        output_digest,
        serialized_bytes,
    }
}

fn baseline_row_probe(
    table: &TableRegistration,
    workload: &super::RealisticWorkload,
) -> super::ProductionProbe {
    let rows = workload.records.iter().map(|record| {
        let item = record.record.new_image.as_ref().expect("fixture image");
        baseline_row_from_item(table, record, item)
    });
    digest_rows(rows)
}

fn baseline_document_json_probe(items: &[&StorageItem]) -> super::ProductionProbe {
    let mut digest = 1469598103934665603_u64;
    let mut serialized_bytes = 0_u64;
    for item in items {
        let value = super::realistic_storage_item_to_json(item);
        let bytes = serde_json::to_vec(&value).expect("fixture JSON serializes");
        digest = super::digest_bytes_from(digest, &bytes);
        serialized_bytes = serialized_bytes.saturating_add(bytes.len() as u64);
        black_box(value);
    }
    super::ProductionProbe {
        output_digest: digest,
        serialized_bytes,
    }
}

fn production_document_json_probe(items: &[&StorageItem]) -> super::ProductionProbe {
    let mut digest = 1469598103934665603_u64;
    let mut serialized_bytes = 0_u64;
    for item in items {
        let value =
            crate::row::performance_storage_item_to_json(item).expect("fixture JSON converts");
        let bytes = serde_json::to_vec(&value).expect("fixture JSON serializes");
        digest = super::digest_bytes_from(digest, &bytes);
        serialized_bytes = serialized_bytes.saturating_add(bytes.len() as u64);
        black_box(value);
    }
    super::ProductionProbe {
        output_digest: digest,
        serialized_bytes,
    }
}

fn baseline_row_from_item(
    table: &TableRegistration,
    record: &super::StreamRecordBatchItem,
    item: &StorageItem,
) -> BTreeMap<String, serde_json::Value> {
    let projected_item = if table.columns.is_empty() {
        StorageItem::new()
    } else {
        item.clone()
    };
    let mut row = projected_item
        .into_iter()
        .map(|(name, value)| (name, value.to_json_value().expect("fixture value converts")))
        .collect::<BTreeMap<_, _>>();
    let tenant_id = match item.get("tenant_id").expect("fixture tenant") {
        StorageValue::S(value) => value.clone(),
        _ => panic!("fixture tenant is a string"),
    };
    row.insert(
        "table_name".to_string(),
        serde_json::Value::String(record.source_table_name.clone()),
    );
    row.insert(
        "tenant_id".to_string(),
        serde_json::Value::String(tenant_id),
    );
    row.insert(
        "__id".to_string(),
        serde_json::Value::String(super::hex_key(&record.record_key)),
    );
    row
}

fn digest_rows(
    rows: impl Iterator<Item = BTreeMap<String, serde_json::Value>>,
) -> super::ProductionProbe {
    let mut writer = DigestWriter::default();
    for row in rows {
        for (name, value) in row {
            writer.write_all(name.as_bytes()).expect("digest writer");
            serde_json::to_writer(&mut writer, &value).expect("row value serializes");
        }
    }
    super::ProductionProbe {
        output_digest: writer.digest,
        serialized_bytes: writer.bytes,
    }
}

#[derive(Default)]
struct DigestWriter {
    digest: u64,
    bytes: u64,
}

impl Write for DigestWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.digest = super::digest_bytes_from(self.digest, bytes);
        self.bytes = self.bytes.saturating_add(bytes.len() as u64);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
