use super::query::performance_serialized_json_length;

#[test]
fn given_escaped_and_numeric_values_when_bytes_are_counted_then_canonical_lengths_match() {
    for text in [
        r#"{"escaped":"quote \" slash \\ newline\n","number":1.2300,"exponent":1e+3}"#,
        r#"{"escaped":"unicode \u2603","number":-0.0,"large":9007199254740993}"#,
    ] {
        let value = serde_json::from_str::<serde_json::Value>(text).expect("fixture parses");
        let canonical = serde_json::to_vec(&value).expect("fixture serializes");
        assert_eq!(
            performance_serialized_json_length(&value).expect("length counts"),
            canonical.len()
        );
    }
}

#[test]
fn given_result_named_columns_when_query_materializes_then_complete_rows_are_returned() {
    let engine = crate::AnalyticsEngine::connect_duckdb(":memory:").expect("memory database");
    let rows = engine
        .query_unscoped_sql_json(
            "SELECT 1 AS result, 2 AS result, '雪' AS \"odd name\", NULL AS missing, [1,2] AS \
             items",
        )
        .expect("query materializes");
    assert_eq!(
        rows,
        vec![serde_json::json!({
            "result": 1,
            "result_1": 2,
            "odd name": "雪",
            "missing": null,
            "items": [1,2]
        })]
    );
}

#[test]
fn given_control_characters_and_nested_values_when_bytes_are_counted_then_canonical_lengths_match()
{
    let controls = (0..32).map(char::from).collect::<String>();
    for width in [0, 8, 900, 65_536] {
        let value = serde_json::json!({
            controls.clone(): format!("{}{}☃\\\"", "x".repeat(width), controls),
            "nested": [null, true, false, -0.0, 1.23, {"empty": [], "object": {}}],
            "integer": 9_007_199_254_740_993_u64,
        });
        assert_eq!(
            performance_serialized_json_length(&value).expect("length counts"),
            serde_json::to_vec(&value).expect("canonical bytes").len()
        );
    }
}
