use super::*;

const PROBE_VALUE_COUNT: usize = 1_000;
const END_TO_END_QUERY: &str = "SELECT range AS row_id, 'tenant-' || CAST(range % 16 AS VARCHAR) \
                                AS tenant_id, repeat('x', 900) || chr(10) || chr(92) || chr(34) \
                                AS payload, CASE WHEN range % 2 = 0 THEN 'quote \" slash \\\\ \
                                line' ELSE 'plain' END AS escaped FROM range(1000)";

#[test]
#[ignore = "manual paired query result serialization performance probe"]
fn given_query_values_when_serialized_length_is_measured_then_bytes_match_and_a_cpu_or_allocation_win_is_measured()
 {
    let values = realistic_query_values();
    println!(
        "query-result-serialization-fixture | values={} | escaped_strings=true | \
         nested_objects=true | numeric_values=true | paired_samples={}",
        values.len(),
        super::PRODUCTION_PAIRED_SAMPLES,
    );

    let result = super::measure_production_candidate(
        "query result serialized byte count",
        "engine/query.rs::serialized_json_length::counting_writer",
        || baseline_serialized_length_probe(&values),
        || candidate_serialized_length_probe(&values),
    );
    super::print_production_candidate_results(
        "query-result-serialization-candidate",
        std::slice::from_ref(&result),
    );
    assert!(result.kept);
    assert_eq!(result.serialized_bytes, expected_serialized_bytes(&values));
}

#[test]
#[ignore = "manual paired end-to-end query result performance probe"]
fn given_query_rows_when_materialization_is_measured_then_outputs_match_and_a_cpu_or_allocation_win_is_measured()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect memory DuckDB");
    assert_complete_query_rows(&engine);
    let result = super::measure_production_candidate(
        "end-to-end query result materialization",
        "engine/query.rs::query_sql_json_with_optional_timeout::fetch_parse_budget_retain",
        || query_probe(&engine, false),
        || query_probe(&engine, true),
    );
    super::print_production_candidate_results(
        "query-result-end-to-end-candidate",
        std::slice::from_ref(&result),
    );
    assert!(result.kept);
    assert!(result.serialized_bytes > 0);
}

#[test]
fn given_wide_escaped_query_rows_when_materialized_then_complete_literal_rows_and_encoded_bytes_match()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("memory database");
    assert_complete_query_rows(&engine);
}

fn assert_complete_query_rows(engine: &AnalyticsEngine) {
    let payload = "x".repeat(900) + "\n\\\"";
    let expected: Vec<_> = (0..1_000)
        .map(|index| {
            serde_json::json!({
                "row_id": index,
                "tenant_id": format!("tenant-{}", index % 16),
                "payload": payload,
                "escaped": if index % 2 == 0 { r#"quote " slash \\ line"# } else { "plain" },
            })
        })
        .collect();
    let expected_bytes = serde_json::to_vec(&expected)
        .expect("literal rows serialize")
        .len();
    for result in [
        engine.performance_query_sql_json_baseline(END_TO_END_QUERY),
        engine.performance_query_sql_json_candidate(END_TO_END_QUERY),
    ] {
        let (rows, encoded_bytes) = result.expect("native result materializes");
        assert_eq!(rows, expected);
        assert_eq!(encoded_bytes, expected_bytes);
    }
    assert_eq!(
        engine
            .query_unscoped_sql_json(END_TO_END_QUERY)
            .expect("public query"),
        expected
    );
}

fn query_probe(engine: &AnalyticsEngine, candidate: bool) -> ProductionProbe {
    let (values, encoded_bytes) = if candidate {
        engine
            .performance_query_sql_json_candidate(END_TO_END_QUERY)
            .expect("candidate query materializes")
    } else {
        engine
            .performance_query_sql_json_baseline(END_TO_END_QUERY)
            .expect("baseline query materializes")
    };
    let mut output_digest = 1469598103934665603_u64;
    for value in &values {
        let row_id = value
            .get("row_id")
            .and_then(serde_json::Value::as_i64)
            .expect("row id is numeric");
        let tenant_id = value
            .get("tenant_id")
            .and_then(serde_json::Value::as_str)
            .expect("tenant id is text");
        let payload = value
            .get("payload")
            .and_then(serde_json::Value::as_str)
            .expect("payload is text");
        let escaped = value
            .get("escaped")
            .and_then(serde_json::Value::as_str)
            .expect("escaped value is text");
        output_digest = super::digest_bytes_from(output_digest, &row_id.to_be_bytes());
        output_digest = super::digest_bytes_from(output_digest, tenant_id.as_bytes());
        output_digest =
            super::digest_bytes_from(output_digest, &(payload.len() as u64).to_be_bytes());
        output_digest = super::digest_bytes_from(output_digest, escaped.as_bytes());
    }
    ProductionProbe {
        output_digest,
        serialized_bytes: encoded_bytes as u64,
    }
}

fn realistic_query_values() -> Vec<serde_json::Value> {
    (0..PROBE_VALUE_COUNT)
        .map(|index| {
            serde_json::json!({
                "table_name": "source_items",
                "tenant_id": format!("tenant-{:02}", index % 16),
                "__id": format!("{index:032x}"),
                "escaped": "quote \" slash \\ newline\n snowman ☃",
                "number": (index as i64) * -17,
                "fraction": 1.2300,
                "nested": {
                    "z": format!("tail-{index}"),
                    "a": [index, true, serde_json::Value::Null],
                },
            })
        })
        .collect()
}

fn baseline_serialized_length_probe(values: &[serde_json::Value]) -> ProductionProbe {
    super::combine_probes(values.iter().map(|value| {
        let bytes = serde_json::to_vec(value).expect("query value serializes");
        let length = bytes.len() as u64;
        ProductionProbe {
            output_digest: super::digest_bytes(&length.to_le_bytes()),
            serialized_bytes: length,
        }
    }))
}

fn candidate_serialized_length_probe(values: &[serde_json::Value]) -> ProductionProbe {
    super::combine_probes(values.iter().map(|value| {
        let length = crate::engine::performance_serialized_json_length(value)
            .expect("query value serializes") as u64;
        ProductionProbe {
            output_digest: super::digest_bytes(&length.to_le_bytes()),
            serialized_bytes: length,
        }
    }))
}

fn expected_serialized_bytes(values: &[serde_json::Value]) -> u64 {
    values
        .iter()
        .map(|value| {
            serde_json::to_vec(value)
                .expect("query value serializes")
                .len() as u64
        })
        .sum()
}
