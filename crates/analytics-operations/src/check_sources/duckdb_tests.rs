use super::{
    CheckRowProjection, CheckRowSource, CurrentRowStreamReader, DuckDbAnalyticalCheckRows,
    DuckDbTableScanCurrentRowReader, ProductionCurrentRowReader,
    row_conversion::projected_stream_to_check_rows,
};

#[test]
fn given_excluded_target_when_duckdb_rows_are_requested_then_database_is_not_opened() {
    let directory = tempfile::tempdir().expect("isolated database directory");
    let path = directory.path().join("absent.duckdb");
    let source = DuckDbAnalyticalCheckRows::try_new(
        path.to_string_lossy(),
        "entries",
        "key",
        vec!["value".to_owned()],
    )
    .expect("source");
    assert!(
        source
            .rows(&["other".to_owned()])
            .expect("excluded target")
            .is_empty()
    );
    assert!(
        source
            .row_stream(&[])
            .expect("empty targets")
            .next_row()
            .expect("stream")
            .is_none()
    );
    assert!(!path.exists());
}

#[test]
fn given_colliding_field_boundaries_and_nulls_when_duckdb_scans_then_full_and_paged_hashes_match_independent_framing()
 {
    let directory = tempfile::tempdir().expect("isolated database directory");
    let path = directory.path().join("rows.duckdb");
    let connection = duckdb::Connection::open(&path).expect("database");
    connection
        .execute_batch(
            "CREATE TABLE entries (key VARCHAR, first VARCHAR, second VARCHAR, private BOOLEAN);
         INSERT INTO entries VALUES ('k1', 'a', 'bc', false), ('k2', 'ab', 'c', false),
                                    ('k3', NULL, '', true), ('k4', '', NULL, NULL)",
        )
        .expect("seed rows");
    drop(connection);
    let projection = CheckRowProjection::try_new_with_optional_columns(
        "entries",
        "key",
        vec!["first".to_owned(), "second".to_owned()],
        None,
        Some("private".to_owned()),
    )
    .expect("projection");
    let source = DuckDbAnalyticalCheckRows::try_new_with_projection(
        path.to_string_lossy(),
        projection.clone(),
    )
    .expect("source");
    let full = source.rows(&["entries".to_owned()]).expect("full scan");
    let reader = ProductionCurrentRowReader::new(
        DuckDbTableScanCurrentRowReader::new(path.to_string_lossy()),
        1,
    );
    let stream = reader.current_row_stream(&projection).expect("paged scan");
    let paged = projected_stream_to_check_rows(&projection, stream).expect("paged rows");
    assert_eq!(full, paged);
    assert_eq!(
        full.iter().map(|row| row.row_hash).collect::<Vec<_>>(),
        [
            5_960_778_121_787_770_730,
            7_364_821_867_157_009_458,
            15_449_794_709_565_471_098,
            5_849_585_240_336_944_452
        ]
    );
    assert_eq!(
        full.iter()
            .map(|row| row.contains_private_data)
            .collect::<Vec<_>>(),
        [false, false, true, false]
    );
    assert!(
        full.iter()
            .all(|row| row.source_position == crate::CheckSourcePosition::default())
    );
}
