use analytics_contract::PrivacyPolicy;

use super::{AnalyticsEngine, AnalyticsEngineError};

#[test]
fn given_more_rows_than_query_budget_when_privacy_dry_run_scans_then_all_candidates_are_reported_without_mutation()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("in-memory engine");
    let mut manifest = analytics_fixtures::users_manifest();
    manifest.tables[0].document_column = None;
    engine.ensure_manifest(&manifest).expect("manifest");
    let count = i64::from(analytics_contract::MAX_STRUCTURED_QUERY_BOUND) + 1;
    engine
        .connection()
        .execute(
            "INSERT INTO users (table_name, tenant_id, __id, email)
         SELECT 'source', 'tenant', CAST(i AS VARCHAR), 'private' FROM range(?) rows(i)",
            [count],
        )
        .expect("seed candidates");
    assert!(matches!(
        engine.query_unscoped_sql_json("SELECT __id FROM users"),
        Err(AnalyticsEngineError::QueryResultTooLarge { .. })
    ));
    let policy = PrivacyPolicy::try_new("privacy")
        .expect("policy")
        .with_denied_key_name("email");
    let report = engine
        .scrub_table_with_privacy_policy(&manifest.tables[0], &policy, true)
        .expect("scan beyond response budget");
    assert_eq!(report.scanned_rows, count as u64);
    assert_eq!(report.affected_rows, count as u64);
    assert_eq!(report.projected_values_scrubbed, count as u64);
    assert!(!report.destination_mutated);
    assert_eq!(
        engine
            .query_unscoped_sql_json("SELECT count(email) AS count FROM users")
            .expect("query"),
        [serde_json::json!({"count": count})]
    );
}

#[test]
fn given_documents_beyond_query_byte_budget_when_privacy_deletes_then_each_snapshot_row_is_counted_and_removed()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("in-memory engine");
    let manifest = analytics_fixtures::users_manifest();
    engine.ensure_manifest(&manifest).expect("manifest");
    let private_value = "private".repeat(500);
    engine
        .connection()
        .execute(
            "INSERT INTO users (table_name, tenant_id, __id, item)
         SELECT 'source', 'tenant', CAST(i AS VARCHAR), json_object('email', ?) FROM range(4097) \
             rows(i)",
            [&private_value],
        )
        .expect("seed documents");
    assert!(matches!(
        engine.query_unscoped_sql_json("SELECT item FROM users"),
        Err(AnalyticsEngineError::QueryResultTooLarge { .. })
    ));
    let policy = PrivacyPolicy::try_new("privacy")
        .expect("policy")
        .with_denied_key_name("email");
    let report = engine
        .remediate_table_with_privacy_policy(
            &manifest.tables[0],
            &policy,
            false,
            super::PrivacyTableRemediationMode::DeleteRows,
        )
        .expect("delete beyond response budget");
    assert_eq!(report.scanned_rows, 4097);
    assert_eq!(report.affected_rows, 4097);
    assert_eq!(report.document_values_scrubbed, 4097);
    assert!(report.destination_mutated);
    assert_eq!(
        engine
            .query_unscoped_sql_json("SELECT count(*) AS count FROM users")
            .expect("query"),
        [serde_json::json!({"count": 0})]
    );
}

#[test]
fn given_duplicate_identities_crossing_native_chunks_when_privacy_deletes_then_no_original_row_is_skipped()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("in-memory engine");
    let manifest = analytics_fixtures::users_manifest();
    engine.ensure_manifest(&manifest).expect("manifest");
    engine
        .connection()
        .execute_batch(
            "INSERT INTO users (table_name, tenant_id, __id, item)
         SELECT 'source', 'tenant', 'duplicate', json_object('email', 'private') FROM range(2049)",
        )
        .expect("seed duplicate identities");
    let policy = PrivacyPolicy::try_new("privacy")
        .expect("policy")
        .with_denied_key_name("email");
    let report = engine
        .remediate_table_with_privacy_policy(
            &manifest.tables[0],
            &policy,
            false,
            super::PrivacyTableRemediationMode::DeleteRows,
        )
        .expect("delete all duplicates");
    assert_eq!(report.scanned_rows, 2049);
    assert_eq!(report.affected_rows, 2049);
    assert_eq!(
        engine
            .query_unscoped_sql_json("SELECT count(*) AS count FROM users")
            .expect("query"),
        [serde_json::json!({"count": 0})]
    );
}

#[test]
fn given_corrupt_document_after_native_chunk_when_privacy_scrubs_then_error_preserves_all_destination_rows()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("in-memory engine");
    let manifest = analytics_fixtures::users_manifest();
    engine.ensure_manifest(&manifest).expect("manifest");
    engine
        .connection()
        .execute_batch(
            "ALTER TABLE users ALTER COLUMN item TYPE VARCHAR;
         INSERT INTO users (table_name, tenant_id, __id, email, item)
         SELECT 'source', 'tenant', CAST(i AS VARCHAR), 'private',
                CASE WHEN i = 4096 THEN 'corrupt' ELSE '{\"email\":\"private\"}' END
         FROM range(4097) rows(i)",
        )
        .expect("seed adversarial stored documents");
    let policy = PrivacyPolicy::try_new("privacy")
        .expect("policy")
        .with_denied_key_name("email");
    let error = engine
        .scrub_table_with_privacy_policy(&manifest.tables[0], &policy, false)
        .expect_err("corrupt document rejects remediation");
    assert!(matches!(error, AnalyticsEngineError::Json(_)));
    assert_eq!(
        engine
            .query_unscoped_sql_json("SELECT count(email) AS count FROM users")
            .expect("query"),
        [serde_json::json!({"count": 4097})]
    );
    assert_eq!(
        engine
            .query_unscoped_sql_json("SELECT item FROM users WHERE __id = '0'")
            .expect("query"),
        [serde_json::json!({"item": "{\"email\":\"private\"}"})]
    );
}

#[test]
fn given_selected_attached_catalog_with_shadow_table_when_privacy_scrubs_then_only_selected_rows_change()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("memory engine");
    let manifest = analytics_fixtures::users_manifest();
    engine.ensure_manifest(&manifest).expect("main manifest");
    engine
        .connection()
        .execute_batch(
            r#"INSERT INTO users (table_name, tenant_id, __id, email, item)
         VALUES ('source', 'tenant', 'shadow', 'main-private', '{"email":"main-private"}');
         ATTACH ':memory:' AS selected;
         USE selected;
         SET search_path = 'selected.main,memory.main';"#,
        )
        .expect("attached catalog");
    engine
        .ensure_manifest(&manifest)
        .expect("selected manifest");
    engine
        .connection()
        .execute_batch(
            r#"INSERT INTO users (table_name, tenant_id, __id, email, item)
         SELECT 'source', 'tenant', CAST(i AS VARCHAR), 'selected-private',
                '{"email":"selected-private","public":"keep"}' FROM range(3) rows(i)"#,
        )
        .expect("selected rows");
    let policy = PrivacyPolicy::try_new("privacy")
        .expect("policy")
        .with_denied_key_name("email");
    for dry_run in [true, false] {
        let report = engine
            .scrub_table_with_privacy_policy(&manifest.tables[0], &policy, dry_run)
            .expect("selected catalog remediation");
        assert_eq!(report.scanned_rows, 3);
        assert_eq!(report.affected_rows, 3);
        assert_eq!(report.projected_values_scrubbed, 3);
        assert_eq!(report.document_values_scrubbed, 3);
        assert_eq!(report.destination_mutated, !dry_run);
    }
    assert_eq!(
        engine
            .query_unscoped_sql_json(
                "SELECT __id, email, item FROM selected.main.users ORDER BY __id"
            )
            .expect("selected output"),
        [
            serde_json::json!({"__id": "0", "email": null, "item": {"public": "keep"}}),
            serde_json::json!({"__id": "1", "email": null, "item": {"public": "keep"}}),
            serde_json::json!({"__id": "2", "email": null, "item": {"public": "keep"}}),
        ]
    );
    assert_eq!(
        engine
            .query_unscoped_sql_json("SELECT email, item FROM memory.main.users")
            .expect("shadow output"),
        [serde_json::json!({"email": "main-private", "item": {"email": "main-private"}})]
    );
}

#[test]
fn given_uncommitted_concurrent_insert_when_privacy_scrubs_then_only_committed_snapshot_rows_are_modified()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("memory engine");
    let manifest = analytics_fixtures::users_manifest();
    engine.ensure_manifest(&manifest).expect("manifest");
    engine.connection().execute_batch(
        r#"INSERT INTO users (table_name, tenant_id, __id, email, item)
         VALUES ('source', 'tenant', 'original', 'private', '{"email":"private","public":"keep"}')"#
    ).expect("original row");
    let other = engine
        .connection()
        .try_clone()
        .expect("independent native transaction");
    other
        .execute_batch(
            r#"BEGIN TRANSACTION;
         INSERT INTO users (table_name, tenant_id, __id, email, item)
         VALUES ('source', 'tenant', 'concurrent', 'pending', '{"email":"pending"}')"#,
        )
        .expect("pending insert");
    let policy = PrivacyPolicy::try_new("privacy")
        .expect("policy")
        .with_denied_key_name("email");
    let report = engine
        .scrub_table_with_privacy_policy(&manifest.tables[0], &policy, false)
        .expect("committed selection snapshot");
    assert_eq!(report.scanned_rows, 1);
    assert_eq!(report.affected_rows, 1);
    assert!(report.destination_mutated);
    other
        .execute_batch("COMMIT")
        .expect("independent insert commits afterwards");
    assert_eq!(
        engine
            .query_unscoped_sql_json("SELECT __id, email, item FROM users ORDER BY __id")
            .expect("committed output"),
        [
            serde_json::json!({"__id": "concurrent", "email": "pending", "item": {"email": "pending"}}),
            serde_json::json!({"__id": "original", "email": null, "item": {"public": "keep"}}),
        ]
    );
}

#[test]
fn given_sqlite_ducklake_rows_when_privacy_scrubs_then_attached_destination_is_read_and_changed() {
    use config::{CatalogType, StorageBackend};
    let directory = tempfile::tempdir().expect("native SQLite catalog fixture");
    let backend = StorageBackend::DuckLake {
        catalog: CatalogType::Sqlite,
        catalog_path: directory
            .path()
            .join("catalog.sqlite")
            .to_string_lossy()
            .into_owned(),
        data_path: directory.path().join("data").to_string_lossy().into_owned(),
        object_storage: None,
        catalog_settings: Default::default(),
    };
    let engine = AnalyticsEngine::connect(&backend).expect("DuckLake engine");
    let manifest = analytics_fixtures::users_manifest();
    engine.ensure_manifest(&manifest).expect("manifest");
    engine
        .connection()
        .execute_batch(
            r#"INSERT INTO users (table_name, tenant_id, __id, email, item)
           SELECT 'source', 'tenant', CAST(i AS VARCHAR), 'private',
                  '{"email":"private","public":"keep"}' FROM range(2049) rows(i)"#,
        )
        .expect("multi-chunk native rows");
    let policy = PrivacyPolicy::try_new("privacy")
        .expect("policy")
        .with_denied_key_name("email");
    let report = engine
        .scrub_table_with_privacy_policy(&manifest.tables[0], &policy, false)
        .expect("DuckLake remediation");
    assert_eq!(report.scanned_rows, 2049);
    assert_eq!(report.affected_rows, 2049);
    assert_eq!(report.projected_values_scrubbed, 2049);
    assert_eq!(report.document_values_scrubbed, 2049);
    assert!(report.destination_mutated);
    assert_eq!(
        engine
            .query_unscoped_sql_json(
                "SELECT count(*) AS rows, count(email) AS emails,
         count(*) FILTER (WHERE item = json_object('public', 'keep')) AS preserved FROM users"
            )
            .expect("native output"),
        [serde_json::json!({"rows": 2049, "emails": 0, "preserved": 2049})]
    );
}

#[test]
#[ignore = "equal-work native maintenance timing/allocation/process-memory probe"]
fn given_equal_privacy_rows_when_native_and_materialized_scans_run_then_effects_and_cost_are_measured()
 {
    use std::time::Instant;

    use alloc_counter::AllocationGuard;

    use super::{PrivacyTableRemediationMode, PrivacyTableRemediationReport};

    let allocations = std::env::var_os("ANALYTICS_PRIVACY_PROBE_ALLOCATIONS").is_some();
    let selected = std::env::var_os("ANALYTICS_PRIVACY_PROBE_MODE").map(|value| {
        value
            .into_string()
            .expect("privacy probe mode must be UTF-8")
    });
    assert!(
        selected
            .as_deref()
            .is_none_or(|mode| matches!(mode, "materialized" | "native")),
        "unsupported ANALYTICS_PRIVACY_PROBE_MODE"
    );
    for rows in [64_i64, 4096] {
        for document_bytes in [32, 4096] {
            for dry_run in [true, false] {
                let payload = "p".repeat(document_bytes);
                let expected_document = if dry_run {
                    serde_json::json!({"email": "private", "public": payload})
                } else {
                    serde_json::json!({"public": payload})
                };
                let expected_document = serde_json::to_string(&expected_document).unwrap();
                let expected_report = PrivacyTableRemediationReport {
                    table_name: "users".into(),
                    dry_run,
                    mode: PrivacyTableRemediationMode::Scrub,
                    scanned_rows: rows as u64,
                    affected_rows: rows as u64,
                    projected_values_scrubbed: rows as u64,
                    document_values_scrubbed: rows as u64,
                    destination_mutated: !dry_run,
                    policy_version: "privacy-probe".into(),
                };
                for pair in 0..3 {
                    let modes = if pair % 2 == 0 {
                        ["materialized", "native"]
                    } else {
                        ["native", "materialized"]
                    };
                    for mode in modes {
                        if selected.as_deref().is_some_and(|selected| selected != mode) {
                            continue;
                        }
                        let engine = AnalyticsEngine::connect_duckdb(":memory:").unwrap();
                        let manifest = analytics_fixtures::users_manifest();
                        engine.ensure_manifest(&manifest).unwrap();
                        engine
                            .connection()
                            .execute(
                                "INSERT INTO users (table_name, tenant_id, __id, email, item)
                             SELECT 'source', 'tenant', CAST(i AS VARCHAR), 'private',
                                    json_object('email', 'private', 'public', ?) FROM range(?) \
                                 rows(i)",
                                duckdb::params![payload, rows],
                            )
                            .unwrap();
                        let policy = PrivacyPolicy::try_new("privacy-probe")
                            .unwrap()
                            .with_denied_key_name("email");
                        engine
                            .connection()
                            .execute_batch("BEGIN; SELECT 1; ROLLBACK")
                            .unwrap();
                        let guard = allocations.then(|| {
                            AllocationGuard::start(
                                "analytics-engine",
                                "privacy-maintenance",
                                file!(),
                                line!(),
                                None,
                            )
                        });
                        let started = Instant::now();
                        let report = match mode {
                            "native" => engine
                                .scrub_table_with_privacy_policy(
                                    &manifest.tables[0],
                                    &policy,
                                    dry_run,
                                )
                                .unwrap(),
                            _ => remediate_materialized_privacy_rows(&engine, &policy, dry_run),
                        };
                        let elapsed = started.elapsed();
                        let allocation_report = guard.map(AllocationGuard::finish);
                        assert_eq!(report, expected_report);
                        let expected_email = dry_run.then_some("private");
                        let actual: (i64, i64, i64, i64, i64, i64) = engine
                            .connection()
                            .query_row(
                                "SELECT count(*), count(DISTINCT __id), min(CAST(__id AS BIGINT)),
                             max(CAST(__id AS BIGINT)),
                             count(*) FILTER (WHERE email IS DISTINCT FROM ?),
                             count(*) FILTER (WHERE item != CAST(? AS JSON)) FROM users",
                                duckdb::params![expected_email, expected_document],
                                |row| {
                                    Ok((
                                        row.get(0)?,
                                        row.get(1)?,
                                        row.get(2)?,
                                        row.get(3)?,
                                        row.get(4)?,
                                        row.get(5)?,
                                    ))
                                },
                            )
                            .unwrap();
                        assert_eq!(actual, (rows, rows, 0, rows - 1, 0, 0));
                        println!(
                            "privacy-maintenance | mode={mode} | pair={pair} | rows={rows} | \
                             document_bytes={document_bytes} | dry_run={dry_run} | elapsed_ns={} \
                             | allocations={:?} | bytes={:?} | golden_rows={actual:?}",
                            elapsed.as_nanos(),
                            allocation_report
                                .as_ref()
                                .map(|report| report.allocation_count),
                            allocation_report
                                .as_ref()
                                .map(|report| report.allocated_bytes)
                        );
                    }
                }
            }
        }
    }
}

// Test-only full JSON materialization reference, without the superseded user
// response budget. It preserves the same policy, native writes and commit
// scope.
fn remediate_materialized_privacy_rows(
    engine: &AnalyticsEngine,
    policy: &PrivacyPolicy,
    dry_run: bool,
) -> super::PrivacyTableRemediationReport {
    use super::{PrivacyRowIdentity, PrivacyTableRemediationMode, PrivacyTableRemediationReport};
    let matcher = policy.validated().unwrap();
    let transaction = super::transaction::EngineTransaction::begin(engine).unwrap();
    let mut statement = engine
        .connection()
        .prepare(
            "SELECT table_name, tenant_id, __id, email IS NOT NULL, CAST(item AS VARCHAR) FROM \
             users",
        )
        .unwrap();
    let mut native_rows = statement.query([]).unwrap();
    let mut rows = Vec::new();
    while let Some(row) = native_rows.next().unwrap() {
        rows.push(serde_json::json!({
            "table_name": row.get::<_, String>(0).unwrap(),
            "tenant_id": row.get::<_, String>(1).unwrap(),
            "__id": row.get::<_, String>(2).unwrap(),
            "email_present": row.get::<_, bool>(3).unwrap(),
            "document": row.get::<_, String>(4).unwrap(),
        }));
    }
    drop(native_rows);
    drop(statement);
    let mut report = PrivacyTableRemediationReport {
        table_name: "users".into(),
        dry_run,
        mode: PrivacyTableRemediationMode::Scrub,
        scanned_rows: 0,
        affected_rows: 0,
        projected_values_scrubbed: 0,
        document_values_scrubbed: 0,
        destination_mutated: false,
        policy_version: policy.version.clone(),
    };
    for row in rows {
        report.scanned_rows += 1;
        let mut assignments = storage_types::VecMap::new();
        if row["email_present"].as_bool().unwrap() {
            assignments.insert("email", duckdb::types::Value::Null);
            report.projected_values_scrubbed += 1;
        }
        let document = serde_json::from_str(row["document"].as_str().unwrap()).unwrap();
        let mut dropped = 0;
        let filtered = super::filter_json_value(document, "", &matcher, &mut dropped);
        if dropped > 0 {
            assignments.insert("item", super::json_value_to_duckdb_value(&filtered));
            report.document_values_scrubbed += dropped;
        }
        if assignments.is_empty() {
            continue;
        }
        report.affected_rows += 1;
        if dry_run {
            continue;
        }
        engine
            .update_privacy_scrubbed_row(
                "users",
                PrivacyRowIdentity {
                    table_name: row["table_name"].as_str().unwrap(),
                    tenant_id: row["tenant_id"].as_str().unwrap(),
                    row_id: row["__id"].as_str().unwrap(),
                },
                assignments,
            )
            .unwrap();
    }
    report.destination_mutated = !dry_run && report.affected_rows > 0;
    transaction.finish(Ok(report)).unwrap()
}

#[test]
fn given_competing_update_after_native_chunk_when_privacy_scrubs_then_conflict_rolls_back_all_maintenance_effects()
 {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("memory engine");
    let manifest = analytics_fixtures::users_manifest();
    engine.ensure_manifest(&manifest).expect("manifest");
    engine
        .connection()
        .execute_batch(
            r#"INSERT INTO users (table_name, tenant_id, __id, email, item)
           SELECT 'source', 'tenant', CAST(i AS VARCHAR), 'private',
                  '{"email":"private","public":"keep"}' FROM range(4097) rows(i)"#,
        )
        .expect("original rows");
    let other = engine
        .connection()
        .try_clone()
        .expect("competing native connection");
    other.execute_batch(
        r#"BEGIN;
           UPDATE users SET email = 'concurrent', item = '{"email":"concurrent","public":"new-keep"}'
           WHERE __id = '4096'"#
    ).expect("competing uncommitted update");
    let policy = PrivacyPolicy::try_new("privacy")
        .expect("policy")
        .with_denied_key_name("email");
    let error = engine
        .scrub_table_with_privacy_policy(&manifest.tables[0], &policy, false)
        .expect_err("conflicting update must fail atomically");
    assert!(matches!(error, AnalyticsEngineError::DuckDb(_)));
    assert!(
        error.to_string().to_ascii_lowercase().contains("conflict"),
        "{error}"
    );
    assert_eq!(
        engine
            .query_unscoped_sql_json(
                "SELECT count(*) AS rows, count(email) AS emails,
         count(*) FILTER (WHERE item = json_object('email', 'private', 'public', 'keep')) AS \
                 documents FROM users"
            )
            .expect("complete rollback"),
        [serde_json::json!({"rows": 4097, "emails": 4097, "documents": 4097})]
    );
    other
        .execute_batch("COMMIT")
        .expect("independent writer can commit after rollback");
    assert_eq!(
        engine
            .query_unscoped_sql_json(
                "SELECT __id, email, item FROM users WHERE __id IN ('0', '4096') ORDER BY __id"
            )
            .expect("independent writer output"),
        [
            serde_json::json!({"__id": "0", "email": "private", "item": {"email": "private", "public": "keep"}}),
            serde_json::json!({"__id": "4096", "email": "concurrent", "item": {"email": "concurrent", "public": "new-keep"}}),
        ]
    );
}

#[test]
fn given_indexed_privacy_paths_when_ingesting_and_scrubbing_then_original_indexes_and_literal_retained_documents_agree()
 {
    use analytics_contract::{StorageItem, StorageStreamRecord};
    use serde_json::json;

    let item: StorageItem = serde_json::from_value(json!({
        "events": {"L": [
            {"M": {"name": {"S": "remove"}}},
            {"M": {"token": {"S": "remove"}, "name": {"S": "keep"}}},
            {"NULL": true}
        ]},
        "matrix": {"L": [
            {"L": [{"S": "keep"}]},
            {"L": [{"S": "remove"}, {"S": "keep"}]}
        ]},
        "tail": {"M": {"secret": {"S": "remove"}, "name": {"S": "keep"}}},
        "secretKey": {"S": "remove"},
        "values": {"L": [{"S": "secret"}, {"S": "public"}, {"NULL": true}]}
    }))
    .unwrap();
    let expected = json!({
        "events": [{"name": "keep"}, null],
        "matrix": [["keep"], ["keep"]],
        "tail": {"name": "keep"},
        "values": ["public", null]
    });
    for regex_paths in [false, true] {
        let mut policy = PrivacyPolicy::try_new("indexed-policy")
            .unwrap()
            .with_denied_key_name("secretKey")
            .with_denied_value_regex("^secret$");
        if regex_paths {
            policy.denied_path_regexes = vec![
                r"^events\[0\]$".into(),
                r"^events\[1\]\.token$".into(),
                r"^matrix\[1\]\[0\]$".into(),
                r"^tail\.secret$".into(),
            ];
        } else {
            policy.denied_exact_paths = vec![
                "events[0]".into(),
                "events[1].token".into(),
                "matrix[1][0]".into(),
                "tail.secret".into(),
            ];
        }
        policy.denied_path_regexes.push("^$".into());
        let engine = AnalyticsEngine::connect_duckdb(":memory:").unwrap();
        let manifest = analytics_fixtures::users_manifest();
        engine.ensure_manifest(&manifest).unwrap();
        let record = StorageStreamRecord {
            sequence_number: "1".into(),
            keys: StorageItem::new(),
            old_image: None,
            new_image: Some(item.clone()),
        };
        let ingested = engine
            .ingest_stream_record_with_privacy_policy(
                &manifest,
                "users",
                b"filtered",
                record.clone(),
                &policy,
            )
            .unwrap();
        assert_eq!(ingested.dropped_fields, 6);
        assert_eq!(
            engine
                .query_unscoped_sql_json("SELECT item FROM users")
                .unwrap(),
            vec![json!({"item": expected})],
        );
        // Indexed rules use the original input positions, before compaction.
        engine
            .connection()
            .execute("DELETE FROM users", [])
            .unwrap();
        engine
            .ingest_stream_record(&manifest, "users", b"raw", record)
            .unwrap();
        let before = engine
            .query_unscoped_sql_json("SELECT item FROM users ORDER BY __id")
            .unwrap();
        let dry_run = engine
            .scrub_table_with_privacy_policy(&manifest.tables[0], &policy, true)
            .unwrap();
        assert_eq!(dry_run.scanned_rows, 1);
        assert_eq!(dry_run.affected_rows, 1);
        assert_eq!(dry_run.document_values_scrubbed, 6);
        assert_eq!(dry_run.projected_values_scrubbed, 0);
        assert!(!dry_run.destination_mutated);
        assert_eq!(
            engine
                .query_unscoped_sql_json("SELECT item FROM users ORDER BY __id")
                .unwrap(),
            before
        );
        let applied = engine
            .scrub_table_with_privacy_policy(&manifest.tables[0], &policy, false)
            .unwrap();
        assert_eq!(applied.scanned_rows, 1);
        assert_eq!(applied.affected_rows, 1);
        assert_eq!(applied.document_values_scrubbed, 6);
        assert_eq!(applied.projected_values_scrubbed, 0);
        assert!(applied.destination_mutated);
        assert_eq!(applied.policy_version, "indexed-policy");
        assert_eq!(
            engine
                .query_unscoped_sql_json("SELECT item FROM users ORDER BY __id")
                .unwrap(),
            vec![json!({"item": expected})]
        );
    }
}
