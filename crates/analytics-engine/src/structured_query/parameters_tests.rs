use analytics_contract::{QuerySelect, StructuredQuery};
use analytics_fixtures::users_manifest;
use serde_json::json;

use super::{QueryCompiler, parameter_fingerprint};

#[test]
fn given_parameter_fingerprint_collision_when_distinct_literals_are_bound_then_equality_controls_parameter_reuse()
 {
    let manifest = users_manifest();
    let query = count_query();
    let compiler = QueryCompiler::try_new(&manifest, &query, true).unwrap();
    assert_eq!(compiler.literal_sql(&json!("first")).unwrap(), "$1");
    let second = duckdb::types::Value::Text("second".to_string());
    compiler
        .parameter_indexes
        .borrow_mut()
        .insert(parameter_fingerprint(&second), 0);
    assert_eq!(compiler.literal_sql(&json!("second")).unwrap(), "$2");
    assert_eq!(compiler.literal_sql(&json!("second")).unwrap(), "$2");
    assert_eq!(
        compiler.parameters.into_inner(),
        vec![duckdb::types::Value::Text("first".to_string()), second,]
    );
}

#[test]
fn given_repeated_json_literal_when_bound_then_one_native_parameter_and_json_cast_are_preserved() {
    let manifest = users_manifest();
    let query = count_query();
    let compiler = QueryCompiler::try_new(&manifest, &query, true).unwrap();
    let value = json!({"z": [true, null], "a": "quote' and Unicode λ"});
    assert_eq!(compiler.literal_sql(&value).unwrap(), "$1::JSON");
    assert_eq!(compiler.literal_sql(&value).unwrap(), "$1::JSON");
    assert_eq!(
        compiler.parameters.into_inner(),
        vec![duckdb::types::Value::Text(
            serde_json::to_string(&value).unwrap()
        ),]
    );
}

fn count_query() -> StructuredQuery {
    StructuredQuery {
        analytics_table_name: "users".to_string(),
        table_alias: None,
        joins: Vec::new(),
        select: vec![QuerySelect::Count {
            alias: "count".to_string(),
        }],
        filters: Vec::new(),
        group_by: Vec::new(),
        order_by: Vec::new(),
        limit: None,
        offset: None,
    }
}
