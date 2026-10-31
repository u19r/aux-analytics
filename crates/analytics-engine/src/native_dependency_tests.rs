use super::AnalyticsEngine;

#[test]
fn given_cargo_selected_duckdb_when_querying_then_the_pinned_native_version_is_loaded() {
    let engine =
        AnalyticsEngine::connect_duckdb(":memory:").expect("connect to Cargo-selected DuckDB");
    let rows = engine
        .query_unscoped_sql_json("select version() as native_version")
        .expect("query the native runtime through the Rust Arrow adapter");
    let version = rows[0]["native_version"]
        .as_str()
        .expect("native version string");
    assert!(
        version == "v2.0.0" || version.starts_with("v2.0.0-dev"),
        "unexpected native version: {version}"
    );
}

#[test]
fn given_bundled_duckdb_when_writing_and_querying_parquet_then_json_rows_preserve_the_value() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect to bundled DuckDB");
    let directory = tempfile::tempdir().expect("create Parquet directory");
    let file = directory.path().join("rows.parquet");
    engine
        .connection()
        .execute(
            &format!(
                "COPY (SELECT 42 AS value) TO '{}' (FORMAT PARQUET)",
                file.display()
            ),
            [],
        )
        .expect("write Parquet with the bundled extension");
    let rows = engine
        .query_unscoped_sql_json(&format!(
            "SELECT value FROM read_parquet('{}')",
            file.display()
        ))
        .expect("read Parquet through the JSON query boundary");
    assert_eq!(rows[0]["value"], 42);
}
