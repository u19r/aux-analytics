use std::{path::Path, time::Instant};

use sha2::{Digest, Sha256};

use super::{
    CheckRowProjection, CurrentRowPageRequest, CurrentRowStreamReader,
    DuckDbTableScanCurrentRowReader, ProductionCurrentRowPageReader, ProductionCurrentRowReader,
    ProjectedCurrentRow,
};

// All alternatives decode the same 128000 rows. The direct cursor is test-only;
// the worker exercises the production pull stream. Compare elapsed time without
// a caller-thread allocation guard, which would burden only two alternatives.
// Standalone stateless pages remain an explicit public operation.
#[test]
#[ignore = "F11 paired native scan experiment; run after implementation in the measurement batch"]
fn given_equal_rows_across_ten_hundred_and_thousand_pages_when_scanned_then_offset_and_owned_cursor_outputs_match()
 {
    let directory = tempfile::tempdir().expect("native database fixture");
    let path = directory.path().join("scan.duckdb");
    seed_scan_database(&path);
    let projection = CheckRowProjection::try_new_with_optional_columns(
        "entries",
        "key",
        vec!["value".to_owned()],
        None,
        Some("private".to_owned()),
    )
    .expect("projection");
    let expected = expected_scan_digest();
    let selected = std::env::var("ANALYTICS_SCAN_EXPERIMENT_MODE").ok();
    assert!(
        selected
            .as_deref()
            .is_none_or(|mode| matches!(mode, "offset" | "cursor" | "worker"))
    );
    for pages in [10, 100, 1000] {
        let page_size = 128000 / pages;
        for pair in 0..3 {
            let modes = if pair % 2 == 0 {
                ["cursor", "offset", "worker"]
            } else {
                ["worker", "offset", "cursor"]
            };
            for mode in modes {
                if selected.as_deref().is_some_and(|selected| selected != mode) {
                    continue;
                }
                let started = Instant::now();
                let digest = match mode {
                    "cursor" => scan_owned_cursor(&path, &projection, page_size),
                    "worker" => scan_production_worker(&path, &projection, page_size),
                    _ => scan_offset_pages(&path, &projection, page_size),
                };
                let elapsed = started.elapsed();
                assert_eq!(digest, expected, "mode={mode}, pages={pages}, pair={pair}");
                let native_statements = if mode == "offset" { pages + 1 } else { 1 };
                println!(
                    "duckdb-scan | mode={mode} | pages={pages} | pair={pair} | rows=128000 | \
                     page_size={page_size} | opens={native_statements} | \
                     prepares={native_statements} | elapsed_ns={} | output_sha256_bytes={digest:?}",
                    elapsed.as_nanos()
                );
            }
        }
        print_physical_scan_plans(&path, &projection, page_size);
    }
}

fn seed_scan_database(path: &Path) {
    let connection = duckdb::Connection::open(path).expect("database");
    connection
        .execute_batch(
            "CREATE TABLE entries AS
         SELECT lpad(CAST(i // 2 AS VARCHAR), 8, '0') AS key,
                CASE WHEN (i // 2) % 3 = 0 THEN NULL ELSE 'quote''λ' END AS value,
                (i // 2) % 5 = 0 AS private
         FROM range(128000) rows(i)",
        )
        .expect("seed duplicate keys, nulls and quoted Unicode");
}

fn scan_offset_pages(path: &Path, projection: &CheckRowProjection, page_size: usize) -> [u8; 32] {
    let reader = DuckDbTableScanCurrentRowReader::new(path.to_string_lossy());
    let mut request = CurrentRowPageRequest {
        projection: projection.clone(),
        cursor: None,
        page_size,
    };
    let mut digest = Sha256::new();
    let mut count = 0;
    loop {
        let page = reader.current_row_page(&request).expect("offset page");
        for row in page.rows {
            append_projected_row_digest(&mut digest, &row);
            count += 1;
        }
        request.cursor = page.next_cursor;
        if request.cursor.is_none() {
            break;
        }
    }
    assert_eq!(count, 128000);
    digest.finalize().into()
}

fn scan_owned_cursor(path: &Path, projection: &CheckRowProjection, page_size: usize) -> [u8; 32] {
    let connection = duckdb::Connection::open(path).expect("cursor database");
    let mut statement = connection
        .prepare(&projection.ordered_select_sql())
        .expect("cursor statement");
    let mut rows = statement.query([]).expect("ordered cursor");
    let mut page = Vec::with_capacity(page_size);
    let mut digest = Sha256::new();
    let mut count = 0;
    while let Some(row) = rows.next().expect("native row") {
        page.push(projection.read_projected_row(row).expect("projected row"));
        if page.len() == page_size {
            count += page.len();
            for row in page.drain(..) {
                append_projected_row_digest(&mut digest, &row);
            }
        }
    }
    count += page.len();
    for row in page {
        append_projected_row_digest(&mut digest, &row);
    }
    assert_eq!(count, 128000);
    digest.finalize().into()
}

fn scan_production_worker(
    path: &Path,
    projection: &CheckRowProjection,
    page_size: usize,
) -> [u8; 32] {
    let reader = ProductionCurrentRowReader::new(
        DuckDbTableScanCurrentRowReader::new(path.to_string_lossy()),
        page_size,
    );
    let mut rows = reader
        .current_row_stream(projection)
        .expect("production stream");
    let mut digest = Sha256::new();
    let mut count = 0;
    while let Some(row) = rows.next_row().expect("production row") {
        append_projected_row_digest(&mut digest, &row);
        count += 1;
    }
    assert_eq!(count, 128000);
    digest.finalize().into()
}

fn append_projected_row_digest(digest: &mut Sha256, row: &ProjectedCurrentRow) {
    assert_eq!(row.source_position, crate::CheckSourcePosition::default());
    append_row_digest(
        digest,
        &row.key,
        row.values.get("value").expect("value column").as_deref(),
        row.contains_private_data,
    );
}

fn append_row_digest(digest: &mut Sha256, key: &str, value: Option<&str>, private: bool) {
    digest.update((key.len() as u64).to_le_bytes());
    digest.update(key.as_bytes());
    digest.update([u8::from(value.is_some())]);
    if let Some(value) = value {
        digest.update((value.len() as u64).to_le_bytes());
        digest.update(value.as_bytes());
    }
    digest.update([u8::from(private)]);
}

fn expected_scan_digest() -> [u8; 32] {
    let mut digest = Sha256::new();
    for index in 0..64000 {
        let key = format!("{index:08}");
        let value = if index % 3 == 0 {
            None
        } else {
            Some("quote'λ")
        };
        for _ in 0..2 {
            append_row_digest(&mut digest, &key, value, index % 5 == 0);
        }
    }
    digest.finalize().into()
}

fn print_physical_scan_plans(path: &Path, projection: &CheckRowProjection, page_size: usize) {
    let connection = duckdb::Connection::open(path).expect("plan database");
    let sql = projection.ordered_select_sql();
    for cursor in [false, true] {
        let mut explain = String::from("EXPLAIN ANALYZE ");
        explain.push_str(&sql);
        if !cursor {
            explain.push_str(" LIMIT ? OFFSET ?");
        }
        let mut statement = connection.prepare(&explain).expect("explain statement");
        let parameters = if cursor {
            vec![]
        } else {
            vec![page_size as i64, (128000 - page_size) as i64]
        };
        let mut rows = statement
            .query(duckdb::params_from_iter(parameters))
            .expect("physical plan");
        while let Some(row) = rows.next().expect("plan row") {
            println!(
                "duckdb-physical-plan | cursor={cursor} | page_size={page_size}\n{}",
                row.get::<_, String>(1).expect("plan text")
            );
        }
    }
}

#[test]
fn given_duplicate_keys_when_owned_cursor_is_open_and_writer_commits_then_original_snapshot_rows_are_returned_once()
 {
    let directory = tempfile::tempdir().expect("native database fixture");
    let path = directory.path().join("snapshot.duckdb");
    let writer = duckdb::Connection::open(&path).expect("writer");
    writer
        .execute_batch(
            "CREATE TABLE entries (key VARCHAR, value VARCHAR);
        INSERT INTO entries VALUES ('a', 'first'), ('a', 'second'), ('c', 'last')",
        )
        .expect("original rows");
    let projection = CheckRowProjection::try_new("entries", "key", vec!["value".to_owned()])
        .expect("projection");
    let reader = duckdb::Connection::open(&path).expect("cursor connection");
    let mut statement = reader
        .prepare(&projection.ordered_select_sql())
        .expect("cursor statement");
    let mut rows = statement.query([]).expect("open snapshot cursor");
    let first = projection
        .read_projected_row(rows.next().expect("fetch first").expect("first row"))
        .expect("decode first");
    writer
        .execute_batch(
            "INSERT INTO entries VALUES ('b', 'concurrent');
        UPDATE entries SET value = 'changed' WHERE key = 'c'",
        )
        .expect("concurrent commit");
    let mut observed = vec![first];
    while let Some(row) = rows.next().expect("fetch original snapshot") {
        observed.push(
            projection
                .read_projected_row(row)
                .expect("decode original snapshot"),
        );
    }
    let mut actual = observed
        .iter()
        .map(|row| {
            (
                row.key.as_str(),
                row.values.get("value").expect("value column").as_deref(),
            )
        })
        .collect::<Vec<_>>();
    actual.sort_unstable();
    assert_eq!(
        actual,
        [
            ("a", Some("first")),
            ("a", Some("second")),
            ("c", Some("last"))
        ]
    );
    let committed: i64 = writer
        .query_row("SELECT count(*) FROM entries", [], |row| row.get(0))
        .expect("committed rows");
    assert_eq!(committed, 4);
}

#[test]
fn given_empty_native_table_when_offset_page_and_owned_cursor_are_opened_then_both_are_terminal() {
    let directory = tempfile::tempdir().expect("native database fixture");
    let path = directory.path().join("empty.duckdb");
    let connection = duckdb::Connection::open(&path).expect("database");
    connection
        .execute_batch("CREATE TABLE entries (key VARCHAR, value VARCHAR)")
        .expect("empty table");
    let projection = CheckRowProjection::try_new("entries", "key", vec!["value".to_owned()])
        .expect("projection");
    let reader = DuckDbTableScanCurrentRowReader::new(path.to_string_lossy());
    let page = reader
        .current_row_page(&CurrentRowPageRequest {
            projection: projection.clone(),
            cursor: None,
            page_size: 1,
        })
        .expect("empty offset page");
    assert!(page.rows.is_empty());
    assert_eq!(page.next_cursor, None);
    let mut statement = connection
        .prepare(&projection.ordered_select_sql())
        .expect("cursor statement");
    let mut rows = statement.query([]).expect("empty cursor");
    assert!(rows.next().expect("terminal cursor").is_none());
}
