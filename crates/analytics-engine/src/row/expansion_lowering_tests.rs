//! Complete expansion-to-native-value ownership comparison. Native execution
//! is proved separately through public ingestion/query; it is outside timing.
use std::{hint::black_box, time::Instant};

use alloc_counter::AllocationGuard;
use analytics_contract::{
    AnalyticsColumn, AnalyticsColumnType, AnalyticsManifest, PrimitiveColumnType, ProjectionColumn,
    RowExpansion, RowIdentity, RowPathSegment, RowProjectionField, StorageItem,
    StorageStreamRecord, StorageValue, TableRegistration,
};
use analytics_fixtures::users_manifest;
use duckdb::types::Value;
use storage_types::VecMap;

use crate::{
    AnalyticsEngine, AnalyticsEngineError, AnalyticsEngineResult, StreamRecordBatchItem,
    batch_writer::batch_columns, constants::PRODUCTION_PAIRED_SAMPLES, row::RowSource,
    row_expansion::expand_source_item,
};

#[derive(Debug, Clone, Copy)]
enum LoweringPath {
    CopiedJsonBridge,
    BorrowedNative,
}
#[derive(Debug, Clone, Copy)]
enum RegistrationShape {
    Projection,
    Declared,
}
#[derive(Debug, Clone, Copy)]
enum DocumentOutput {
    Absent,
    Present,
}

struct ExpandedRowWorkload {
    source: StorageItem,
    table: TableRegistration,
    metadata_table: TableRegistration,
}

impl ExpandedRowWorkload {
    fn new(count: usize, shape: RegistrationShape, document: DocumentOutput) -> Self {
        let mut source = StorageItem::from_iter([
            (
                "source_profile".to_string(),
                StorageValue::M(StorageItem::from_iter([
                    ("body".to_string(), StorageValue::S("w".repeat(1024))),
                    (
                        "tags".to_string(),
                        StorageValue::L(vec![
                            StorageValue::M(StorageItem::from_iter([(
                                "label".to_string(),
                                StorageValue::S("blue".to_string()),
                            )])),
                            StorageValue::M(StorageItem::from_iter([(
                                "label".to_string(),
                                StorageValue::S("green".to_string()),
                            )])),
                        ]),
                    ),
                    (
                        "metadata".to_string(),
                        StorageValue::M(StorageItem::from_iter([(
                            "active".to_string(),
                            StorageValue::BOOL(true),
                        )])),
                    ),
                ])),
            ),
            (
                "points".to_string(),
                StorageValue::L(
                    (0..count)
                        .map(|index| {
                            StorageValue::L(vec![
                                StorageValue::N("12.3400".to_string()),
                                StorageValue::S(format!("point-{index}")),
                            ])
                        })
                        .collect(),
                ),
            ),
        ]);
        source.insert(
            "source_empty".to_string(),
            StorageValue::M(StorageItem::new()),
        );
        let mut expansion = RowExpansion {
            list_path: vec![attribute("points")],
            source_fields: vec![RowProjectionField {
                output_attribute_name: "source_payload".to_string(),
                path: vec![attribute("source_profile")],
            }],
            element_fields: vec![
                RowProjectionField {
                    output_attribute_name: "amount".to_string(),
                    path: vec![RowPathSegment::Index { index: 0 }],
                },
                RowProjectionField {
                    output_attribute_name: "point_id".to_string(),
                    path: vec![RowPathSegment::Index { index: 1 }],
                },
            ],
            ordinal_attribute_name: Some("ordinal".to_string()),
        };
        expansion.source_fields.push(RowProjectionField {
            output_attribute_name: "missing_container".to_string(),
            path: vec![attribute("source_empty")],
        });
        let mut table = users_manifest().tables.remove(0);
        table.row_identity = RowIdentity::Attribute {
            attribute_name: "point_id".to_string(),
        };
        table.document_column = match document {
            DocumentOutput::Absent => None,
            DocumentOutput::Present => Some("item".to_string()),
        };
        table.expression_attribute_names = Some(VecMap::from_iter([
            ("#payload".to_string(), "source_payload".to_string()),
            ("#tags".to_string(), "tags".to_string()),
        ]));
        let mut columns = vec![
            projection("amount", "amount", PrimitiveColumnType::Decimal),
            projection("point_id", "point_id", PrimitiveColumnType::VarChar),
            projection("ordinal", "ordinal", PrimitiveColumnType::BigInt),
            projection(
                "labels",
                "#payload.#tags[].label",
                PrimitiveColumnType::VarChar,
            ),
            projection(
                "labels_again",
                "#payload.#tags[].label",
                PrimitiveColumnType::VarChar,
            ),
            projection("nested_payload", "#payload", PrimitiveColumnType::Json),
            projection(
                "missing_value",
                "missing_container.absent",
                PrimitiveColumnType::VarChar,
            ),
        ];
        for column in &mut columns {
            if matches!(column.column_name.as_str(), "labels" | "labels_again") {
                column.column_type = Some(AnalyticsColumnType::List {
                    primitive: PrimitiveColumnType::VarChar,
                });
            }
        }
        columns.extend((0..16).map(|index| {
            projection(
                &format!("wide_{index}"),
                "#payload.body",
                PrimitiveColumnType::VarChar,
            )
        }));
        match shape {
            RegistrationShape::Projection => table.projection_columns = Some(columns),
            RegistrationShape::Declared => {
                table.columns = columns
                    .iter()
                    .map(|column| AnalyticsColumn {
                        column_name: column.column_name.clone(),
                        column_type: column.column_type.clone().unwrap(),
                    })
                    .collect();
                table.projection_columns = None;
                table.expression_attribute_names = None;
                source.insert(
                    "source_labels".to_string(),
                    StorageValue::L(vec![
                        StorageValue::S("blue".to_string()),
                        StorageValue::S("green".to_string()),
                    ]),
                );
                source.insert("source_missing".to_string(), StorageValue::NULL(true));
                for name in ["labels", "labels_again"] {
                    expansion.source_fields.push(RowProjectionField {
                        output_attribute_name: name.to_string(),
                        path: vec![attribute("source_labels")],
                    });
                }
                expansion.source_fields.push(RowProjectionField {
                    output_attribute_name: "nested_payload".to_string(),
                    path: vec![attribute("source_profile")],
                });
                expansion.source_fields.push(RowProjectionField {
                    output_attribute_name: "missing_value".to_string(),
                    path: vec![attribute("source_missing")],
                });
                expansion
                    .source_fields
                    .extend((0..16).map(|index| RowProjectionField {
                        output_attribute_name: format!("wide_{index}"),
                        path: vec![attribute("source_profile"), attribute("body")],
                    }));
            }
        }
        table.row_expansion = Some(expansion);
        let mut metadata_table = table.clone();
        metadata_table.columns.clear();
        metadata_table.projection_columns = None;
        metadata_table.projection_attribute_names = None;
        metadata_table.row_expansion = None;
        Self {
            source,
            table,
            metadata_table,
        }
    }

    fn lower_rows(&self, engine: &AnalyticsEngine, path: LoweringPath) -> Vec<Vec<Value>> {
        let expansion = self.table.row_expansion.as_ref().unwrap();
        let rows = match path {
            LoweringPath::CopiedJsonBridge => copied_source_expansion(&self.source, expansion),
            LoweringPath::BorrowedNative => expand_source_item(&self.source, expansion),
        }
        .unwrap();
        rows.into_iter()
            .map(|item| {
                let source = RowSource {
                    source_table_name: "tenant_01",
                    record_key: b"batch",
                    record_keys: &self.source,
                    stream_key_digest: None,
                };
                match path {
                    LoweringPath::BorrowedNative => engine
                        .batch_row_from_item(&self.table, source, &item, 0, None)
                        .unwrap()
                        .into_values(None)
                        .unwrap(),
                    LoweringPath::CopiedJsonBridge => {
                        // Share current identity/document/cache owners.
                        // Reconstruct only the former selected map and JSON
                        // bridge, using its values for lowering.
                        let mut selected = StorageItem::new();
                        engine
                            .for_each_projected_attribute(&self.table, &item, |name, value| {
                                selected.insert(name.to_string(), value.clone());
                                Ok(())
                            })
                            .unwrap();
                        let mut row = engine
                            .row_from_item(&self.metadata_table, source, &item, 0, None)
                            .unwrap();
                        for (name, value) in selected {
                            let json = match (name.as_str(), value) {
                                ("amount", StorageValue::N(value)) => {
                                    serde_json::Value::String(value)
                                }
                                (_, value) => value.to_json_value().unwrap(),
                            };
                            row.insert(name, json);
                        }
                        engine
                            .batch_row_from_json(&self.table, &row)
                            .unwrap()
                            .into_values(None)
                            .unwrap()
                    }
                }
            })
            .collect()
    }

    fn expected_payload() -> serde_json::Value {
        serde_json::json!({"body": "w".repeat(1024), "metadata": {"active": true}, "tags": [{"label":"blue"}, {"label":"green"}]})
    }

    fn expected_document(&self, index: usize) -> serde_json::Value {
        let mut document = serde_json::json!({"missing_container":{},"source_payload":Self::expected_payload(),"amount":12.34,"point_id":format!("point-{index}"),"ordinal":index});
        if !self.table.columns.is_empty() {
            document["labels"] = serde_json::json!(["blue", "green"]);
            document["labels_again"] = serde_json::json!(["blue", "green"]);
            document["nested_payload"] = Self::expected_payload();
            document["missing_value"] = serde_json::Value::Null;
            for index in 0..16 {
                document[format!("wide_{index}")] = serde_json::Value::String("w".repeat(1024));
            }
        }
        document
    }

    fn assert_native_rows(&self, rows: &[Vec<Value>]) {
        let StorageValue::L(points) = self.source.get("points").unwrap() else {
            panic!("points fixture")
        };
        assert_eq!(rows.len(), points.len());
        let columns = batch_columns(&self.table, self.table.retention.is_some());
        for (index, values) in rows.iter().enumerate() {
            assert_eq!(values.len(), columns.len());
            for (name, value) in columns.iter().zip(values) {
                match name.as_str() {
                    "table_name" | "tenant_id" => {
                        assert_eq!(value, &Value::Text("tenant_01".to_string()))
                    }
                    "__id" => {
                        let Value::Text(id) = value else {
                            panic!("native identity text")
                        };
                        assert_eq!(id.len() % 2, 0);
                        let decoded = id
                            .as_bytes()
                            .as_chunks::<2>()
                            .0
                            .iter()
                            .map(|pair| {
                                u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()
                            })
                            .collect::<Vec<_>>();
                        assert_eq!(decoded, format!("point-{index}").into_bytes());
                    }
                    "__source_position" | "missing_value" => assert_eq!(value, &Value::Null),
                    "amount" => assert_eq!(value, &Value::Text("12.3400".to_string())),
                    "point_id" => assert_eq!(value, &Value::Text(format!("point-{index}"))),
                    "ordinal" => assert_eq!(value, &Value::Text(index.to_string())),
                    "labels" | "labels_again" => {
                        assert_json_text(value, serde_json::json!(["blue", "green"]))
                    }
                    "nested_payload" => assert_json_text(value, Self::expected_payload()),
                    "item" => assert_json_text(value, self.expected_document(index)),
                    _ if name.starts_with("wide_") => {
                        assert_eq!(value, &Value::Text("w".repeat(1024)))
                    }
                    other => panic!("unexpected native column {other}"),
                }
            }
        }
    }

    fn measure_lowering(&self, engine: &AnalyticsEngine, path: LoweringPath) -> (u64, u64) {
        let guard = AllocationGuard::start(
            module_path!(),
            "expanded-native-lowering",
            file!(),
            line!(),
            None,
        );
        let start = Instant::now();
        let rows = black_box(self.lower_rows(engine, path));
        let elapsed = start.elapsed();
        let report = guard.finish();
        self.assert_native_rows(&rows);
        println!(
            "expanded-native-lowering | path={path:?} | rows={} | document={} | declared={} | \
             elapsed_ns={} | allocations={} | allocated_bytes={}",
            rows.len(),
            self.table.document_column.is_some(),
            !self.table.columns.is_empty(),
            elapsed.as_nanos(),
            report.allocation_count,
            report.allocated_bytes
        );
        (report.allocation_count, report.allocated_bytes)
    }

    fn ingest_batch(
        &self,
        engine: &AnalyticsEngine,
    ) -> AnalyticsEngineResult<Vec<crate::IngestOutcome>> {
        engine.ingest_stream_record_batch(
            &AnalyticsManifest::new(vec![self.table.clone()]),
            vec![StreamRecordBatchItem {
                analytics_table_name: "users".to_string(),
                source_table_name: "tenant_01".to_string(),
                shard_id: None,
                system_position: None,
                record_key: b"batch".to_vec(),
                record: StorageStreamRecord {
                    keys: StorageItem::new(),
                    sequence_number: "1".to_string(),
                    old_image: None,
                    new_image: Some(self.source.clone()),
                },
            }],
        )
    }
}

fn attribute(name: &str) -> RowPathSegment {
    RowPathSegment::Attribute {
        name: name.to_string(),
    }
}
fn projection(name: &str, path: &str, primitive: PrimitiveColumnType) -> ProjectionColumn {
    ProjectionColumn {
        column_name: name.to_string(),
        attribute_path: path.to_string(),
        column_type: Some(AnalyticsColumnType::Primitive { primitive }),
    }
}
fn assert_json_text(value: &Value, expected: serde_json::Value) {
    let Value::Text(text) = value else {
        panic!("expected native JSON text: {value:?}")
    };
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(text).unwrap(),
        expected
    );
}

#[test]
fn given_wide_nested_expansion_when_projected_or_declared_rows_are_ingested_then_aliases_decimals_ordinals_and_documents_are_exact()
 {
    for shape in [RegistrationShape::Projection, RegistrationShape::Declared] {
        for document in [DocumentOutput::Absent, DocumentOutput::Present] {
            for count in [0, 1, 3] {
                let workload = ExpandedRowWorkload::new(count, shape, document);
                let engine = AnalyticsEngine::connect_duckdb(":memory:").unwrap();
                engine
                    .ensure_manifest(&AnalyticsManifest::new(vec![workload.table.clone()]))
                    .unwrap();
                workload.ingest_batch(&engine).unwrap();
                let rows = engine
                    .query_unscoped_sql_json(
                        "SELECT amount, point_id, ordinal, labels, labels_again, nested_payload, \
                         missing_value, wide_15 FROM users ORDER BY ordinal",
                    )
                    .unwrap();
                let expected=(0..count).map(|index| serde_json::json!({"amount":12.34,"point_id":format!("point-{index}"),"ordinal":index,
                    "labels":["blue","green"],"labels_again":["blue","green"],"nested_payload":ExpandedRowWorkload::expected_payload(),"missing_value":null,"wide_15":"w".repeat(1024)})).collect::<Vec<_>>();
                assert_eq!(rows, expected);
                if matches!(document, DocumentOutput::Present) {
                    let docs = engine
                        .query_unscoped_sql_json("SELECT item FROM users ORDER BY ordinal")
                        .unwrap();
                    let expected = (0..count)
                        .map(|index| serde_json::json!({"item":workload.expected_document(index)}))
                        .collect::<Vec<_>>();
                    assert_eq!(docs, expected);
                }
                let direct = workload.lower_rows(&engine, LoweringPath::BorrowedNative);
                let baseline = workload.lower_rows(&engine, LoweringPath::CopiedJsonBridge);
                workload.assert_native_rows(&direct);
                workload.assert_native_rows(&baseline);
                assert_eq!(direct, baseline);
            }
        }
    }
}

#[test]
#[ignore = "manual paired full expansion and native lowering ownership probe"]
fn given_wide_nested_source_when_expanded_and_lowered_then_owned_outputs_match_with_less_allocation()
 {
    for count in [1, 100, 1000] {
        for shape in [RegistrationShape::Projection, RegistrationShape::Declared] {
            for document in [DocumentOutput::Absent, DocumentOutput::Present] {
                let workload = ExpandedRowWorkload::new(count, shape, document);
                let baseline_engine = AnalyticsEngine::connect_duckdb(":memory:").unwrap();
                let candidate_engine = AnalyticsEngine::connect_duckdb(":memory:").unwrap();
                workload.lower_rows(&baseline_engine, LoweringPath::CopiedJsonBridge);
                workload.lower_rows(&candidate_engine, LoweringPath::BorrowedNative);
                for pair in 0..PRODUCTION_PAIRED_SAMPLES {
                    let (before, after) = if pair % 2 == 0 {
                        (
                            workload
                                .measure_lowering(&baseline_engine, LoweringPath::CopiedJsonBridge),
                            workload
                                .measure_lowering(&candidate_engine, LoweringPath::BorrowedNative),
                        )
                    } else {
                        let after = workload
                            .measure_lowering(&candidate_engine, LoweringPath::BorrowedNative);
                        (
                            workload
                                .measure_lowering(&baseline_engine, LoweringPath::CopiedJsonBridge),
                            after,
                        )
                    };
                    assert!(after.0 < before.0);
                    assert!(after.1 < before.1);
                }
            }
        }
    }
}

#[test]
fn given_valid_then_invalid_expanded_amount_when_ingested_then_the_entire_batch_is_rolled_back() {
    for shape in [RegistrationShape::Projection, RegistrationShape::Declared] {
        let mut workload = ExpandedRowWorkload::new(2, shape, DocumentOutput::Absent);
        let StorageValue::L(points) = workload.source.get_mut("points").unwrap() else {
            panic!("points fixture")
        };
        let StorageValue::L(second) = &mut points[1] else {
            panic!("second point fixture")
        };
        second[0] = StorageValue::NULL(false);
        let engine = AnalyticsEngine::connect_duckdb(":memory:").unwrap();
        engine
            .ensure_manifest(&AnalyticsManifest::new(vec![workload.table.clone()]))
            .unwrap();
        assert!(matches!(workload.ingest_batch(&engine).unwrap_err(),
            AnalyticsEngineError::AttributeConversion(name) if name == "amount"));
        assert_eq!(
            engine
                .query_unscoped_sql_json("SELECT count(*) AS count FROM users")
                .unwrap(),
            vec![serde_json::json!({"count":0})]
        );
    }
}

#[test]
fn given_unselected_invalid_nested_value_when_expanded_then_projection_ignores_it_and_document_output_rejects_it()
 {
    for document in [DocumentOutput::Absent, DocumentOutput::Present] {
        let mut workload = ExpandedRowWorkload::new(2, RegistrationShape::Projection, document);
        workload
            .table
            .projection_columns
            .as_mut()
            .unwrap()
            .retain(|column| column.column_name != "nested_payload");
        let StorageValue::M(profile) = workload.source.get_mut("source_profile").unwrap() else {
            panic!("profile fixture")
        };
        profile.insert("ignored".to_string(), StorageValue::NULL(false));
        let engine = AnalyticsEngine::connect_duckdb(":memory:").unwrap();
        engine
            .ensure_manifest(&AnalyticsManifest::new(vec![workload.table.clone()]))
            .unwrap();
        match document {
            DocumentOutput::Absent => {
                workload.ingest_batch(&engine).unwrap();
                assert_eq!(
                    engine
                        .query_unscoped_sql_json("SELECT point_id FROM users ORDER BY ordinal")
                        .unwrap(),
                    vec![
                        serde_json::json!({"point_id":"point-0"}),
                        serde_json::json!({"point_id":"point-1"})
                    ]
                );
            }
            DocumentOutput::Present => {
                assert!(matches!(workload.ingest_batch(&engine).unwrap_err(),
                    AnalyticsEngineError::AttributeConversion(name) if name == "source_payload"));
                assert_eq!(
                    engine
                        .query_unscoped_sql_json("SELECT count(*) AS count FROM users")
                        .unwrap(),
                    vec![serde_json::json!({"count":0})]
                );
            }
        }
    }
}

#[test]
fn given_cached_overlapping_aliases_when_the_alias_target_changes_then_both_columns_follow_the_new_target()
 {
    let mut workload =
        ExpandedRowWorkload::new(1, RegistrationShape::Projection, DocumentOutput::Absent);
    let StorageValue::M(profile) = workload.source.get_mut("source_profile").unwrap() else {
        panic!("profile fixture")
    };
    profile.insert(
        "other_tags".to_string(),
        StorageValue::L(vec![StorageValue::M(StorageItem::from_iter([(
            "label".to_string(),
            StorageValue::S("red".to_string()),
        )]))]),
    );
    let engine = AnalyticsEngine::connect_duckdb(":memory:").unwrap();
    engine
        .ensure_manifest(&AnalyticsManifest::new(vec![workload.table.clone()]))
        .unwrap();
    workload.ingest_batch(&engine).unwrap();
    assert_eq!(
        engine
            .query_unscoped_sql_json("SELECT labels, labels_again FROM users")
            .unwrap(),
        vec![serde_json::json!({"labels":["blue","green"],"labels_again":["blue","green"]})]
    );
    workload
        .table
        .expression_attribute_names
        .as_mut()
        .unwrap()
        .insert("#tags".to_string(), "other_tags".to_string());
    workload.ingest_batch(&engine).unwrap();
    assert_eq!(
        engine
            .query_unscoped_sql_json("SELECT labels, labels_again FROM users")
            .unwrap(),
        vec![serde_json::json!({"labels":["red"],"labels_again":["red"]})]
    );
}

#[test]
#[ignore = "manual separate-process expansion/lowering memory probe; requires explicit path"]
fn given_wide_expanded_documents_when_lowered_in_one_process_then_full_native_outputs_remain_owned()
{
    let path = match std::env::var("AUX_ANALYTICS_EXPANSION_LOWERING_PROBE").as_deref() {
        Ok("copied") => LoweringPath::CopiedJsonBridge,
        Ok("borrowed") => LoweringPath::BorrowedNative,
        other => panic!("explicit copied or borrowed lowering probe required: {other:?}"),
    };
    let workload =
        ExpandedRowWorkload::new(1000, RegistrationShape::Declared, DocumentOutput::Present);
    let engine = AnalyticsEngine::connect_duckdb(":memory:").unwrap();
    workload.lower_rows(&engine, path);
    workload.measure_lowering(&engine, path);
}

// Former expansion owner from HEAD. VecMap's current insert API replaces its
// former extend spelling; input fields, copies, path validation and outputs
// match.
fn copied_source_expansion(
    source: &StorageItem,
    expansion: &RowExpansion,
) -> AnalyticsEngineResult<Vec<StorageItem>> {
    let list = copied_resolve_item_path(source, &expansion.list_path)?;
    let StorageValue::L(elements) = list else {
        return Err(AnalyticsEngineError::InvalidRowExpansionRecord(
            "list_path did not resolve to a list",
        ));
    };

    let source_values = expansion
        .source_fields
        .iter()
        .map(|field| {
            copied_resolve_item_path(source, &field.path)
                .cloned()
                .map(|value| (field.output_attribute_name.clone(), value))
        })
        .collect::<AnalyticsEngineResult<Vec<_>>>()?;
    let mut rows = Vec::with_capacity(elements.len());
    for (ordinal, element) in elements.iter().enumerate() {
        let mut row = StorageItem::with_capacity(
            source_values.len()
                + expansion.element_fields.len()
                + usize::from(expansion.ordinal_attribute_name.is_some()),
        );
        for (name, value) in source_values.iter().cloned() {
            row.insert(name, value);
        }
        for field in &expansion.element_fields {
            row.insert(
                field.output_attribute_name.clone(),
                copied_resolve_value_path(element, &field.path)?.clone(),
            );
        }
        if let Some(attribute_name) = expansion.ordinal_attribute_name.as_ref() {
            row.insert(attribute_name.clone(), StorageValue::N(ordinal.to_string()));
        }
        rows.push(row);
    }
    Ok(rows)
}

fn copied_resolve_item_path<'item>(
    item: &'item StorageItem,
    path: &[RowPathSegment],
) -> AnalyticsEngineResult<&'item StorageValue> {
    let (first, remainder) =
        path.split_first()
            .ok_or(AnalyticsEngineError::InvalidRowExpansionRecord(
                "path must not be empty",
            ))?;
    let RowPathSegment::Attribute { name } = first else {
        return Err(AnalyticsEngineError::InvalidRowExpansionRecord(
            "source item path must begin with an attribute",
        ));
    };
    let value = item
        .get(name)
        .ok_or(AnalyticsEngineError::MissingRowExpansionPath)?;
    copied_resolve_value_path(value, remainder)
}

fn copied_resolve_value_path<'value>(
    mut value: &'value StorageValue,
    path: &[RowPathSegment],
) -> AnalyticsEngineResult<&'value StorageValue> {
    for segment in path {
        value = match (segment, value) {
            (RowPathSegment::Attribute { name }, StorageValue::M(values)) => values
                .get(name)
                .ok_or(AnalyticsEngineError::MissingRowExpansionPath)?,
            (RowPathSegment::Index { index }, StorageValue::L(values)) => values
                .get(*index)
                .ok_or(AnalyticsEngineError::MissingRowExpansionPath)?,
            (RowPathSegment::Attribute { .. }, _) => {
                return Err(AnalyticsEngineError::InvalidRowExpansionRecord(
                    "attribute path segment did not resolve against a map",
                ));
            }
            (RowPathSegment::Index { .. }, _) => {
                return Err(AnalyticsEngineError::InvalidRowExpansionRecord(
                    "index path segment did not resolve against a list",
                ));
            }
        };
    }
    Ok(value)
}
