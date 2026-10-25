use std::time::Instant;

use sha2::{Digest, Sha256};

use super::{
    CheckRowProjection, CheckRowSource, CurrentRowStreamReader, DuckDbAnalyticalCheckRows,
    DuckDbTableScanCurrentRowReader, ProductionCurrentRowReader,
};
use crate::{CheckError, CheckRow};

#[test]
fn given_native_decoder_panics_after_delivery_when_scan_drains_then_worker_error_remains_typed() {
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE entries (key VARCHAR, value VARCHAR); INSERT INTO entries VALUES \
             ('first', 'value'), ('panic', 'value')",
        )
        .unwrap();
    let projection =
        CheckRowProjection::try_new("entries", "key", vec!["value".to_owned()]).unwrap();
    let mut scan = super::duckdb_scan::DuckDbScan::try_new(
        connection.try_clone().unwrap(),
        projection,
        1,
        read_row_or_panic,
    )
    .unwrap();
    assert_eq!(scan.next_batch().unwrap().unwrap()[0].key, "first");
    let error = scan.next_batch().unwrap_err();
    let mapped =
        crate::check::map_backend_read_error(error, super::CheckBackendSupport::Supported, true);
    assert!(matches!(mapped, CheckError::ScanWorkerPanicked));
    assert!(scan.next_batch().unwrap().is_none());
    connection
        .execute_batch("INSERT INTO entries VALUES ('after', 'value')")
        .unwrap();
    assert_eq!(
        connection
            .query_row("SELECT count(*) FROM entries", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        3
    );
}

fn read_row_or_panic(
    projection: &CheckRowProjection,
    row: &duckdb::Row<'_>,
) -> crate::CheckResult<super::ProjectedCurrentRow> {
    assert_ne!(
        row.get::<_, String>(0)?,
        "panic",
        "injected native decoder panic"
    );
    projection.read_projected_row(row)
}

#[test]
fn given_in_memory_writer_when_shared_scan_reopens_then_committed_changes_are_visible() {
    let writer = duckdb::Connection::open_in_memory().unwrap();
    writer
        .execute_batch("CREATE TABLE entries AS SELECT 'key' AS key, 'first' AS value")
        .unwrap();
    let projection =
        CheckRowProjection::try_new("entries", "key", vec!["value".to_owned()]).unwrap();
    let source = DuckDbAnalyticalCheckRows::try_from_connection(&writer, projection).unwrap();
    assert_eq!(source.rows(&["entries".to_owned()]).unwrap().len(), 1);
    writer
        .execute_batch("INSERT INTO entries VALUES ('second', 'value')")
        .unwrap();
    assert_eq!(source.rows(&["entries".to_owned()]).unwrap().len(), 2);
}

#[test]
fn given_shared_projected_scan_when_writer_changes_rows_then_snapshot_and_fresh_scan_are_distinct()
{
    let writer = duckdb::Connection::open_in_memory().unwrap();
    writer
        .execute_batch(
            "CREATE TABLE entries AS SELECT CAST(i AS VARCHAR) AS key, 'original' AS value FROM \
             range(5) rows(i)",
        )
        .unwrap();
    let reader = ProductionCurrentRowReader::new(
        DuckDbTableScanCurrentRowReader::try_from_connection(&writer).unwrap(),
        2,
    );
    let projection =
        CheckRowProjection::try_new("entries", "key", vec!["value".to_owned()]).unwrap();
    let mut snapshot = reader.current_row_stream(&projection).unwrap();
    writer
        .execute_batch(
            "UPDATE entries SET value = 'changed'; INSERT INTO entries VALUES ('new', 'value')",
        )
        .unwrap();
    let mut original = Vec::new();
    while let Some(row) = snapshot.next_row().unwrap() {
        assert_eq!(
            row.values.get("value").unwrap().as_deref(),
            Some("original")
        );
        original.push(row.key);
    }
    assert_eq!(original, ["0", "1", "2", "3", "4"]);
    assert_eq!(
        reader
            .current_row_stream(&projection)
            .unwrap()
            .into_rows()
            .unwrap()
            .len(),
        6
    );
    let mut partial = reader.current_row_stream(&projection).unwrap();
    assert!(partial.next_row().unwrap().is_some());
    drop(partial);
    writer.execute_batch("DROP TABLE entries").unwrap();
    assert!(matches!(
        reader.current_row_stream(&projection),
        Err(CheckError::DuckDb(_))
    ));
}

#[test]
fn given_zero_native_batch_size_when_projected_stream_opens_then_configuration_is_rejected() {
    let connection = duckdb::Connection::open_in_memory().unwrap();
    let reader = ProductionCurrentRowReader::new(
        DuckDbTableScanCurrentRowReader::try_from_connection(&connection).unwrap(),
        0,
    );
    let projection =
        CheckRowProjection::try_new("entries", "key", vec!["value".to_owned()]).unwrap();
    assert!(matches!(
        reader.current_row_stream(&projection),
        Err(CheckError::InvalidBackendConfiguration(_))
    ));
}

#[test]
fn given_view_over_attached_catalog_when_shared_scan_opens_then_native_catalog_remains_visible() {
    let connection = duckdb::Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "ATTACH ':memory:' AS imported; CREATE TABLE imported.main.rows (key VARCHAR, value \
             VARCHAR); INSERT INTO imported.main.rows VALUES ('key', 'value'); CREATE VIEW \
             entries AS SELECT * FROM imported.main.rows",
        )
        .unwrap();
    let projection =
        CheckRowProjection::try_new("entries", "key", vec!["value".to_owned()]).unwrap();
    let source = DuckDbAnalyticalCheckRows::try_from_connection(&connection, projection).unwrap();
    drop(connection);
    let rows = source.rows(&["entries".to_owned()]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].key, "key");
}

#[test]
fn given_duplicates_across_batches_when_writer_commits_then_scan_returns_original_snapshot_once() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("snapshot.duckdb");
    let writer = duckdb::Connection::open(&path).unwrap();
    writer
        .execute_batch(
            "CREATE TABLE entries AS SELECT CAST(i // 2 AS VARCHAR) AS key, 'original' AS value
         FROM range(6000) rows(i)",
        )
        .unwrap();
    let source = DuckDbAnalyticalCheckRows::try_from_connection(
        &writer,
        CheckRowProjection::try_new("entries", "key", vec!["value".to_owned()]).unwrap(),
    )
    .unwrap();
    let expected = source.rows(&["entries".to_owned()]).unwrap();
    let mut stream = source.row_stream(&["entries".to_owned()]).unwrap();
    let mut observed = vec![stream.next_row().unwrap().unwrap()];
    writer
        .execute_batch(
            "UPDATE entries SET value = 'changed'; INSERT INTO entries VALUES ('new', \
             'concurrent')",
        )
        .unwrap();
    while let Some(row) = stream.next_row().unwrap() {
        observed.push(row);
    }
    assert_eq!(observed, expected);
    assert_eq!(observed.len(), 6000);
    assert_eq!(source.rows(&["entries".to_owned()]).unwrap().len(), 6001);
}

#[test]
fn given_scan_with_more_batches_when_dropped_early_then_next_scan_and_writer_remain_usable() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("drop.duckdb");
    let writer = duckdb::Connection::open(&path).unwrap();
    writer
        .execute_batch(
            "CREATE TABLE entries AS SELECT CAST(i AS VARCHAR) AS key, 'value' AS value
         FROM range(10000) rows(i)",
        )
        .unwrap();
    let source = DuckDbAnalyticalCheckRows::try_from_connection(
        &writer,
        CheckRowProjection::try_new("entries", "key", vec!["value".to_owned()]).unwrap(),
    )
    .unwrap();
    for _ in 0..4 {
        let mut stream = source.row_stream(&["entries".to_owned()]).unwrap();
        assert!(stream.next_row().unwrap().is_some());
        drop(stream);
    }
    writer
        .execute_batch("INSERT INTO entries VALUES ('new', 'value')")
        .unwrap();
    assert_eq!(source.rows(&["entries".to_owned()]).unwrap().len(), 10001);
}

#[test]
fn given_invalid_position_after_a_batch_when_stream_drains_then_native_error_is_preserved() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("invalid.duckdb");
    let connection = duckdb::Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE entries AS SELECT lpad(CAST(i AS VARCHAR), 6, '0') AS key,
         'value' AS value, CASE WHEN i = 3000 THEN '{}' ELSE NULL END AS position
         FROM range(3001) rows(i)",
        )
        .unwrap();
    let projection = CheckRowProjection::try_new_with_optional_columns(
        "entries",
        "key",
        vec!["value".to_owned()],
        Some("position".to_owned()),
        None,
    )
    .unwrap();
    let source = DuckDbAnalyticalCheckRows::try_from_connection(&connection, projection).unwrap();
    let mut stream = source.row_stream(&["entries".to_owned()]).unwrap();
    for _ in 0..crate::constants::DUCKDB_CHECK_BATCH_ROWS {
        assert!(stream.next_row().unwrap().is_some());
    }
    assert!(matches!(
        stream.next_row(),
        Err(CheckError::InvalidBackendConfiguration(_))
    ));
    assert!(stream.next_row().unwrap().is_none());
}

#[test]
fn given_empty_or_invalid_native_table_when_scan_opens_then_terminal_or_preparation_error_is_returned()
 {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("empty.duckdb");
    let connection = duckdb::Connection::open(&path).unwrap();
    connection
        .execute_batch("CREATE TABLE entries (key VARCHAR, value VARCHAR)")
        .unwrap();
    let source = DuckDbAnalyticalCheckRows::try_from_connection(
        &connection,
        CheckRowProjection::try_new("entries", "key", vec!["value".to_owned()]).unwrap(),
    )
    .unwrap();
    assert!(
        source
            .row_stream(&["entries".to_owned()])
            .unwrap()
            .next_row()
            .unwrap()
            .is_none()
    );
    connection.execute_batch("DROP TABLE entries").unwrap();
    assert!(matches!(
        source.row_stream(&["entries".to_owned()]),
        Err(CheckError::DuckDb(_))
    ));
}

#[test]
#[ignore = "paired production scan ownership CPU and process memory experiment"]
fn given_equal_native_rows_when_worker_stream_replaces_full_collection_then_digest_matches() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("measurement.duckdb");
    let connection = duckdb::Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE entries AS SELECT lpad(CAST(i // 2 AS VARCHAR), 8, '0') AS key,
         CASE WHEN i // 2 % 3 = 0 THEN NULL ELSE 'quote''λ' END AS value
         FROM range(128000) rows(i)",
        )
        .unwrap();
    drop(connection);
    let projection =
        CheckRowProjection::try_new("entries", "key", vec!["value".to_owned()]).unwrap();
    let source = DuckDbAnalyticalCheckRows::try_new_with_projection(
        path.to_string_lossy(),
        projection.clone(),
    )
    .unwrap();
    let selected = std::env::var("ANALYTICS_CHECK_SCAN_MODE").ok();
    assert!(
        selected
            .as_deref()
            .is_none_or(|mode| matches!(mode, "buffered" | "worker"))
    );
    let mut expected = Sha256::new();
    for index in 0..64000 {
        let value = (index % 3 != 0).then_some("quote'λ");
        let mut field_bytes = vec![u8::from(value.is_some())];
        if let Some(value) = value {
            field_bytes.extend_from_slice(&(value.len() as u64).to_le_bytes());
            field_bytes.extend_from_slice(value.as_bytes());
        }
        let hash = field_bytes
            .into_iter()
            .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
                (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
            });
        for _ in 0..2 {
            append_digest(
                &mut expected,
                &CheckRow {
                    table: "entries".to_owned(),
                    key: format!("{index:08}"),
                    row_hash: hash,
                    source_position: crate::CheckSourcePosition::default(),
                    contains_private_data: false,
                },
            );
        }
    }
    let expected: [u8; 32] = expected.finalize().into();
    for pair in 0..5 {
        for worker in [pair % 2 == 0, pair % 2 != 0] {
            let mode = if worker { "worker" } else { "buffered" };
            if selected.as_deref().is_some_and(|selected| selected != mode) {
                continue;
            }
            let started = Instant::now();
            let mut digest = Sha256::new();
            let mut count = 0;
            if worker {
                let mut stream = source.row_stream(&["entries".to_owned()]).unwrap();
                while let Some(row) = stream.next_row().unwrap() {
                    append_digest(&mut digest, &row);
                    count += 1;
                }
            } else {
                let connection = duckdb::Connection::open(&path).unwrap();
                let mut statement = connection
                    .prepare(&projection.ordered_select_sql())
                    .unwrap();
                let mut rows = statement.query([]).unwrap();
                let mut buffered = Vec::new();
                while let Some(row) = rows.next().unwrap() {
                    buffered.push(projection.read_check_row(row).unwrap());
                }
                drop(rows);
                drop(statement);
                drop(connection);
                for row in buffered {
                    append_digest(&mut digest, &row);
                    count += 1;
                }
            }
            let elapsed = started.elapsed();
            assert_eq!(count, 128000);
            assert_eq!(<[u8; 32]>::from(digest.finalize()), expected);
            println!(
                "production-check-scan | mode={mode} | pair={pair} | rows={count} | elapsed_ns={}",
                elapsed.as_nanos()
            );
        }
    }
}

fn append_digest(digest: &mut Sha256, row: &CheckRow) {
    digest.update(serde_json::to_vec(row).unwrap());
}
