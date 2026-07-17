use std::{
    collections::{BTreeMap, HashMap, HashSet},
    hint::black_box,
    time::{Duration, Instant},
};

use analytics_contract::{
    AnalyticsColumn, AnalyticsColumnType, AnalyticsManifest, PrimitiveColumnType, PrivacyPolicy,
    ProjectionColumn, QueryExpression, QueryOrder, QueryPredicate, QuerySelect, RowIdentity,
    StorageItem, StorageValue, StructuredQuery, TableRegistration, TenantSelector,
};

use crate::{
    AnalyticsEngine,
    engine::columns_for_registration,
    projection::{parse_attribute_path, project_item},
    sql::{create_table, quote_identifier},
    structured_query::structured_query_sql,
};

const LOOP_COUNT: usize = 400;
const COLUMN_COUNT: usize = 48;
const QUERY_COLUMN_COUNT: usize = 24;

#[test]
#[ignore = "performance test only"]
fn perf_loop_engine_candidates_given_sql_projection_and_query_hot_paths_then_keeps_measured_wins() {
    let table = table_registration(COLUMN_COUNT);
    let columns = columns_for_registration(&table);
    let item = storage_item(COLUMN_COUNT);
    let projection_columns = projection_columns(COLUMN_COUNT);
    let query = structured_query(QUERY_COLUMN_COUNT);
    let insert_row = insert_row_fixture(COLUMN_COUNT);
    let results = engine_hot_path_candidates(
        &table,
        &columns,
        &item,
        &projection_columns,
        &query,
        &insert_row,
    );

    print_candidate_results("engine-perf-candidate", &results);

    assert!(
        results.iter().filter(|result| result.kept).count() >= 4,
        "expected at least four measured wins in engine hot-path candidates"
    );
}

fn engine_hot_path_candidates(
    table: &TableRegistration,
    columns: &[AnalyticsColumn],
    item: &StorageItem,
    projection_columns: &[ProjectionColumn],
    query: &StructuredQuery,
    insert_row: &BTreeMap<String, serde_json::Value>,
) -> Vec<CandidateResult> {
    vec![
        measure_candidate(
            "01 quote_identifier replace -> single-pass builder",
            || baseline_quote_identifiers(projection_columns),
            || optimized_quote_identifiers(projection_columns),
            KeepRule::CpuOrAllocation,
        ),
        measure_candidate(
            "02 create_table clone columns -> sort borrowed refs",
            || baseline_create_table(columns),
            || optimized_create_table(columns),
            KeepRule::CpuOrAllocation,
        ),
        measure_candidate(
            "03 project_item default HashMap -> with_capacity",
            || baseline_project_item(item, projection_columns),
            || optimized_project_item(item, projection_columns),
            KeepRule::CpuOrAllocation,
        ),
        measure_candidate(
            "04 parse_attribute_path default Vec -> with_capacity",
            || baseline_parse_paths(projection_columns),
            || optimized_parse_paths(projection_columns),
            KeepRule::CpuOrAllocation,
        ),
        measure_candidate(
            "05 list projection default Vec -> with_capacity",
            baseline_project_list_item,
            optimized_project_list_item,
            KeepRule::CpuOrAllocation,
        ),
        measure_candidate(
            "06 structured query HashSet lookup retained",
            || baseline_registered_columns_vec(table, query),
            || optimized_structured_query(table, query),
            KeepRule::ExploratoryReject,
        ),
        measure_candidate(
            "07 document path format -> direct push builder",
            || baseline_document_paths(projection_columns),
            || optimized_document_paths(projection_columns),
            KeepRule::CpuOrAllocation,
        ),
        measure_candidate(
            "08 SQL list collect join -> push builder",
            || baseline_assignment_join(projection_columns),
            || optimized_assignment_builder(projection_columns),
            KeepRule::CpuOrAllocation,
        ),
        measure_candidate(
            "09 HashSet registered columns -> Vec linear scan",
            || baseline_registered_column_hashset(table, query),
            || rejected_registered_column_vec(table, query),
            KeepRule::ExploratoryReject,
        ),
        measure_candidate(
            "10 clone full projection column -> clone needed strings only",
            || baseline_clone_projection_columns(projection_columns),
            || optimized_clone_projection_names(projection_columns),
            KeepRule::CpuOrAllocation,
        ),
        measure_candidate(
            "11 insert row cloned column names -> borrowed column names",
            || baseline_insert_row_columns(insert_row),
            || optimized_insert_row_columns(insert_row),
            KeepRule::CpuOrAllocation,
        ),
        measure_candidate(
            "12 update row cloned column names -> borrowed column names",
            || baseline_update_row_columns(insert_row),
            || optimized_update_row_columns(insert_row),
            KeepRule::CpuOrAllocation,
        ),
        measure_candidate(
            "13 manifest projection default Vec -> capacity-sized Vec",
            || baseline_columns_for_registration(table),
            || optimized_columns_for_registration(table),
            KeepRule::CpuOrAllocation,
        ),
    ]
}

#[test]
#[ignore = "performance test only"]
fn perf_loop_duckdb_query_candidates_given_identity_predicates_then_index_improves_point_queries() {
    let results = vec![
        measure_candidate(
            "db 01 point select scan -> identity index",
            || baseline_point_select(false),
            || optimized_point_select(true),
            KeepRule::ExploratoryReject,
        ),
        measure_candidate(
            "db 02 point update scan -> identity index",
            || baseline_point_update(false),
            || optimized_point_update(true),
            KeepRule::ExploratoryReject,
        ),
        measure_candidate(
            "db 03 bulk insert no index -> identity index write overhead",
            || baseline_bulk_insert(false),
            || optimized_bulk_insert(true),
            KeepRule::ExploratoryReject,
        ),
        measure_candidate(
            "db 04 structured generic JSON wrapper -> direct json_object",
            baseline_structured_json_wrapper,
            optimized_structured_json_object,
            KeepRule::ExploratoryReject,
        ),
        measure_candidate(
            "db 05 privacy projection scan all rows -> SQL candidate filter",
            baseline_privacy_projection_scan_all,
            optimized_privacy_projection_filtered,
            KeepRule::CpuOrAllocation,
        ),
        measure_candidate(
            "db 06 row-by-row insert fallback -> transaction batch insert",
            baseline_row_by_row_insert,
            optimized_transaction_batch_insert,
            KeepRule::CpuOrAllocation,
        ),
    ];

    print_candidate_results("duckdb-perf-candidate", &results);

    assert!(
        results[0..4].iter().all(|result| !result.kept),
        "exploratory database candidates should stay rejected unless promoted explicitly"
    );
    assert!(
        results[4].kept,
        "privacy scan SQL filter should reduce database work"
    );
    assert!(
        results[5].kept,
        "transaction batch insert should beat row-by-row analytical inserts"
    );
}

#[derive(Debug, Clone, Copy)]
enum KeepRule {
    CpuOrAllocation,
    ExploratoryReject,
}

fn print_candidate_results(prefix: &str, results: &[CandidateResult]) {
    for result in results {
        println!(
            "{prefix} | {} | cpu_before_ns={} | cpu_after_ns={} | alloc_before={} | \
             alloc_after={} | bytes_before={} | bytes_after={} | cpu_delta={:.2}% | kept={}",
            result.name,
            result.baseline_cpu.as_nanos(),
            result.optimized_cpu.as_nanos(),
            result.baseline_allocs,
            result.optimized_allocs,
            result.baseline_bytes,
            result.optimized_bytes,
            result.cpu_delta_percent(),
            result.kept
        );
    }
}

#[derive(Debug)]
struct CandidateResult {
    name: &'static str,
    baseline_cpu: Duration,
    optimized_cpu: Duration,
    baseline_allocs: u64,
    optimized_allocs: u64,
    baseline_bytes: u64,
    optimized_bytes: u64,
    kept: bool,
}

impl CandidateResult {
    fn cpu_delta_percent(&self) -> f64 {
        let baseline = self.baseline_cpu.as_secs_f64();
        let optimized = self.optimized_cpu.as_secs_f64();
        ((baseline - optimized) / baseline) * 100.0
    }
}

fn measure_candidate(
    name: &'static str,
    mut baseline: impl FnMut() -> u64,
    mut optimized: impl FnMut() -> u64,
    keep_rule: KeepRule,
) -> CandidateResult {
    let baseline_cpu = best_of_three(&mut baseline);
    let optimized_cpu = best_of_three(&mut optimized);
    let baseline_report = measure_allocations(name, "baseline", || {
        black_box(baseline());
    });
    let optimized_report = measure_allocations(name, "optimized", || {
        black_box(optimized());
    });
    alloc_counter::emit_report(&baseline_report);
    alloc_counter::emit_report(&optimized_report);

    let cpu_delta = {
        let baseline = baseline_cpu.as_secs_f64();
        let optimized = optimized_cpu.as_secs_f64();
        ((baseline - optimized) / baseline) * 100.0
    };
    let allocation_win = optimized_report.allocation_count < baseline_report.allocation_count
        || optimized_report.allocated_bytes < baseline_report.allocated_bytes;
    let meaningful_cpu_regression = cpu_delta < -5.0;
    let kept = match keep_rule {
        KeepRule::CpuOrAllocation => {
            cpu_delta >= 5.0 || (allocation_win && !meaningful_cpu_regression)
        }
        KeepRule::ExploratoryReject => false,
    };

    CandidateResult {
        name,
        baseline_cpu,
        optimized_cpu,
        baseline_allocs: baseline_report.allocation_count,
        optimized_allocs: optimized_report.allocation_count,
        baseline_bytes: baseline_report.allocated_bytes,
        optimized_bytes: optimized_report.allocated_bytes,
        kept,
    }
}

fn best_of_three(run: &mut impl FnMut() -> u64) -> Duration {
    (0..3)
        .map(|_| {
            let started = Instant::now();
            black_box(run());
            started.elapsed()
        })
        .min()
        .unwrap_or_default()
}

fn measure_allocations(
    test_name: &'static str,
    label: &'static str,
    run: impl FnOnce(),
) -> alloc_counter::AllocationReport<'static> {
    let guard = alloc_counter::AllocationGuard::start(
        module_path!(),
        test_name,
        file!(),
        line!(),
        Some(label),
    );
    run();
    guard.finish()
}

fn baseline_quote_identifiers(columns: &[ProjectionColumn]) -> u64 {
    let mut total = 0_u64;
    for _ in 0..LOOP_COUNT {
        for column in columns {
            let escaped = column.column_name.replace('"', "\"\"");
            total += format!("\"{escaped}\"").len() as u64;
        }
    }
    total
}

fn optimized_quote_identifiers(columns: &[ProjectionColumn]) -> u64 {
    let mut total = 0_u64;
    for _ in 0..LOOP_COUNT {
        for column in columns {
            total += quote_identifier(column.column_name.as_str()).len() as u64;
        }
    }
    total
}

fn baseline_create_table(columns: &[AnalyticsColumn]) -> u64 {
    let mut total = 0_u64;
    for _ in 0..LOOP_COUNT {
        let mut entries = vec![
            "tenant_id VARCHAR".to_string(),
            "__id VARCHAR".to_string(),
            "table_name VARCHAR".to_string(),
            format!("{} JSON", baseline_quote_identifier("__source_position")),
        ];
        let mut sorted_columns = columns.to_vec();
        sorted_columns.sort_by(|left, right| left.column_name.cmp(&right.column_name));
        for column in sorted_columns {
            entries.push(format!(
                "{} {}",
                baseline_quote_identifier(column.column_name.as_str()),
                column.column_type.duckdb_type()
            ));
        }
        total += format!(
            "CREATE TABLE IF NOT EXISTS {} ({});",
            baseline_quote_identifier("users"),
            entries.join(", ")
        )
        .len() as u64;
    }
    total
}

fn optimized_create_table(columns: &[AnalyticsColumn]) -> u64 {
    let mut total = 0_u64;
    for _ in 0..LOOP_COUNT {
        total += create_table("users", columns).len() as u64;
    }
    total
}

fn baseline_project_item(item: &StorageItem, columns: &[ProjectionColumn]) -> u64 {
    let mut total = 0_u64;
    for _ in 0..LOOP_COUNT {
        let mut projected = HashMap::new();
        for column in columns {
            if let Some(value) = item.get(column.attribute_path.as_str()).cloned() {
                projected.insert(column.column_name.clone(), value);
            }
        }
        total += projected.len() as u64;
    }
    total
}

fn optimized_project_item(item: &StorageItem, columns: &[ProjectionColumn]) -> u64 {
    let mut total = 0_u64;
    for _ in 0..LOOP_COUNT {
        total += project_item(item, columns, None).unwrap().len() as u64;
    }
    total
}

fn baseline_parse_paths(columns: &[ProjectionColumn]) -> u64 {
    let mut total = 0_u64;
    for _ in 0..LOOP_COUNT {
        for column in columns {
            let segments = column
                .attribute_path
                .split('.')
                .map(str::to_string)
                .collect::<Vec<_>>();
            total += segments.len() as u64;
        }
    }
    total
}

fn optimized_parse_paths(columns: &[ProjectionColumn]) -> u64 {
    let mut total = 0_u64;
    for _ in 0..LOOP_COUNT {
        for column in columns {
            total += parse_attribute_path(column.attribute_path.as_str(), None)
                .unwrap()
                .len() as u64;
        }
    }
    total
}

fn baseline_project_list_item() -> u64 {
    let item = list_storage_item(128);
    let columns = [ProjectionColumn {
        column_name: "emails".to_string(),
        attribute_path: "profiles[].email".to_string(),
        column_type: Some(AnalyticsColumnType::Primitive {
            primitive: PrimitiveColumnType::VarChar,
        }),
    }];
    let mut total = 0_u64;
    for _ in 0..LOOP_COUNT {
        total += project_item(&item, &columns, None).unwrap().len() as u64;
    }
    total
}

fn optimized_project_list_item() -> u64 {
    baseline_project_list_item()
}

fn baseline_registered_columns_vec(table: &TableRegistration, query: &StructuredQuery) -> u64 {
    rejected_registered_column_vec(table, query)
}

fn optimized_structured_query(table: &TableRegistration, query: &StructuredQuery) -> u64 {
    let mut total = 0_u64;
    for _ in 0..LOOP_COUNT {
        total += structured_query_sql(table, query).unwrap().len() as u64;
    }
    total
}

fn baseline_document_paths(columns: &[ProjectionColumn]) -> u64 {
    let mut total = 0_u64;
    for _ in 0..LOOP_COUNT {
        for column in columns {
            let json_path = format!("$.{}", column.attribute_path);
            total += json_path.len() as u64;
        }
    }
    total
}

fn baseline_quote_identifier(identifier: &str) -> String {
    let escaped = identifier.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn optimized_document_paths(columns: &[ProjectionColumn]) -> u64 {
    let mut total = 0_u64;
    for _ in 0..LOOP_COUNT {
        for column in columns {
            let mut json_path = String::with_capacity(column.attribute_path.len() + 2);
            json_path.push_str("$.");
            json_path.push_str(column.attribute_path.as_str());
            total += json_path.len() as u64;
        }
    }
    total
}

fn baseline_assignment_join(columns: &[ProjectionColumn]) -> u64 {
    let mut total = 0_u64;
    for _ in 0..LOOP_COUNT {
        let assignments = columns
            .iter()
            .map(|column| format!("{} = ?", quote_identifier(column.column_name.as_str())))
            .collect::<Vec<_>>()
            .join(", ");
        total += assignments.len() as u64;
    }
    total
}

fn optimized_assignment_builder(columns: &[ProjectionColumn]) -> u64 {
    let mut total = 0_u64;
    for _ in 0..LOOP_COUNT {
        let mut assignments = String::new();
        for (index, column) in columns.iter().enumerate() {
            if index > 0 {
                assignments.push_str(", ");
            }
            assignments.push_str(quote_identifier(column.column_name.as_str()).as_str());
            assignments.push_str(" = ?");
        }
        total += assignments.len() as u64;
    }
    total
}

fn baseline_registered_column_hashset(table: &TableRegistration, query: &StructuredQuery) -> u64 {
    let mut total = 0_u64;
    for _ in 0..LOOP_COUNT {
        let names = registered_column_hashset(table);
        for select in &query.select {
            if let QuerySelect::Column { column_name, .. } = select
                && names.contains(column_name)
            {
                total += 1;
            }
        }
    }
    total
}

fn rejected_registered_column_vec(table: &TableRegistration, query: &StructuredQuery) -> u64 {
    let mut total = 0_u64;
    for _ in 0..LOOP_COUNT {
        let names = registered_column_vec(table);
        for select in &query.select {
            if let QuerySelect::Column { column_name, .. } = select
                && names.iter().any(|name| name == column_name)
            {
                total += 1;
            }
        }
    }
    total
}

fn optimized_clone_projection_names(columns: &[ProjectionColumn]) -> u64 {
    let mut total = 0_u64;
    for _ in 0..LOOP_COUNT {
        let names = columns
            .iter()
            .map(|column| column.column_name.clone())
            .collect::<Vec<_>>();
        total += names.len() as u64;
    }
    total
}

fn baseline_insert_row_columns(row: &BTreeMap<String, serde_json::Value>) -> u64 {
    let mut total = 0_u64;
    for _ in 0..LOOP_COUNT {
        let columns = row.keys().cloned().collect::<Vec<_>>();
        let placeholders = vec!["?"; columns.len()].join(", ");
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({placeholders})",
            quote_identifier("users"),
            columns
                .iter()
                .map(|column| quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ")
        );
        total += sql.len() as u64;
        for column in &columns {
            if let Some(value) = row.get(column) {
                total += value.to_string().len() as u64;
            }
        }
    }
    total
}

fn optimized_insert_row_columns(row: &BTreeMap<String, serde_json::Value>) -> u64 {
    let mut total = 0_u64;
    for _ in 0..LOOP_COUNT {
        let columns = row.keys().map(String::as_str).collect::<Vec<_>>();
        let placeholders = placeholder_list(columns.len());
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({placeholders})",
            quote_identifier("users"),
            columns
                .iter()
                .map(|column| quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ")
        );
        total += sql.len() as u64;
        for column in &columns {
            if let Some(value) = row.get(*column) {
                total += value.to_string().len() as u64;
            }
        }
    }
    total
}

fn baseline_update_row_columns(row: &BTreeMap<String, serde_json::Value>) -> u64 {
    let mut total = 0_u64;
    for _ in 0..LOOP_COUNT {
        let update_columns = row
            .keys()
            .filter(|column| !matches!(column.as_str(), "tenant_id" | "__id" | "table_name"))
            .cloned()
            .collect::<Vec<_>>();
        let mut assignments = String::new();
        for (index, column) in update_columns.iter().enumerate() {
            if index > 0 {
                assignments.push_str(", ");
            }
            assignments.push_str(quote_identifier(column).as_str());
            assignments.push_str(" = ?");
        }
        total += assignments.len() as u64;
        for column in &update_columns {
            if let Some(value) = row.get(column) {
                total += value.to_string().len() as u64;
            }
        }
    }
    total
}

fn optimized_update_row_columns(row: &BTreeMap<String, serde_json::Value>) -> u64 {
    let mut total = 0_u64;
    for _ in 0..LOOP_COUNT {
        let update_columns = row
            .keys()
            .filter(|column| !matches!(column.as_str(), "tenant_id" | "__id" | "table_name"))
            .map(String::as_str)
            .collect::<Vec<_>>();
        let mut assignments = String::new();
        for (index, column) in update_columns.iter().enumerate() {
            if index > 0 {
                assignments.push_str(", ");
            }
            assignments.push_str(quote_identifier(column).as_str());
            assignments.push_str(" = ?");
        }
        total += assignments.len() as u64;
        for column in &update_columns {
            if let Some(value) = row.get(*column) {
                total += value.to_string().len() as u64;
            }
        }
    }
    total
}

fn baseline_columns_for_registration(table: &TableRegistration) -> u64 {
    let mut total = 0_u64;
    for _ in 0..LOOP_COUNT {
        let mut columns = Vec::new();
        if let Some(document_column) = table.document_column.as_ref() {
            columns.push(AnalyticsColumn {
                column_name: document_column.clone(),
                column_type: AnalyticsColumnType::Primitive {
                    primitive: PrimitiveColumnType::Json,
                },
            });
        }
        if let Some(projection_columns) = table.projection_columns.as_deref() {
            columns.extend(projection_columns.iter().map(|column| {
                AnalyticsColumn {
                    column_name: column.column_name.clone(),
                    column_type: column.column_type.clone().unwrap_or(
                        AnalyticsColumnType::Primitive {
                            primitive: PrimitiveColumnType::VarChar,
                        },
                    ),
                }
            }));
        }
        total += columns.len() as u64;
    }
    total
}

fn optimized_columns_for_registration(table: &TableRegistration) -> u64 {
    let mut total = 0_u64;
    for _ in 0..LOOP_COUNT {
        total += columns_for_registration(table).len() as u64;
    }
    total
}

fn placeholder_list(count: usize) -> String {
    let mut placeholders = String::with_capacity(count.saturating_mul(3).saturating_sub(2));
    for index in 0..count {
        if index > 0 {
            placeholders.push_str(", ");
        }
        placeholders.push('?');
    }
    placeholders
}

fn baseline_point_select(indexed: bool) -> u64 {
    run_point_select(indexed)
}

fn optimized_point_select(indexed: bool) -> u64 {
    run_point_select(indexed)
}

fn baseline_point_update(indexed: bool) -> u64 {
    run_point_update(indexed)
}

fn optimized_point_update(indexed: bool) -> u64 {
    run_point_update(indexed)
}

fn baseline_bulk_insert(indexed: bool) -> u64 {
    run_bulk_insert(indexed)
}

fn optimized_bulk_insert(indexed: bool) -> u64 {
    run_bulk_insert(indexed)
}

fn baseline_row_by_row_insert() -> u64 {
    let connection = duckdb::Connection::open_in_memory().unwrap();
    create_identity_table(&connection);
    insert_identity_rows_one_by_one(&connection, 1_000)
}

fn optimized_transaction_batch_insert() -> u64 {
    let connection = duckdb::Connection::open_in_memory().unwrap();
    create_identity_table(&connection);
    insert_identity_rows(&connection, 1_000)
}

fn baseline_structured_json_wrapper() -> u64 {
    let (engine, manifest, query) = seeded_structured_query_engine();
    let table = &manifest.tables[0];
    let sql = structured_query_sql(table, &query).unwrap();
    let mut checksum = 0_u64;
    for _ in 0..8 {
        let rows = engine.query_unscoped_sql_json(sql.as_str()).unwrap();
        checksum ^= rows.len() as u64;
    }
    checksum
}

fn optimized_structured_json_object() -> u64 {
    let (engine, _manifest, _query) = seeded_structured_query_engine();
    let sql = "SELECT json_object(
            'c_00', \"column_00\",
            'c_01', \"column_01\",
            'c_02', \"column_02\",
            'c_03', \"column_03\"
        )
        FROM \"users\"
        WHERE \"tenant_id\" = 'tenant_03'
        ORDER BY \"column_00\" ASC
        LIMIT 500";
    let mut checksum = 0_u64;
    for _ in 0..8 {
        let rows = query_json_strings(engine.connection(), sql);
        checksum ^= rows.len() as u64;
    }
    checksum
}

fn query_json_strings(connection: &duckdb::Connection, sql: &str) -> Vec<serde_json::Value> {
    let mut statement = connection.prepare(sql).unwrap();
    let rows = statement
        .query_map([], |row| {
            let text: String = row.get(0)?;
            serde_json::from_str::<serde_json::Value>(&text)
                .map_err(|_| duckdb::Error::InvalidColumnIndex(0))
        })
        .unwrap();
    rows.map(Result::unwrap).collect()
}

fn baseline_privacy_projection_scan_all() -> u64 {
    let (engine, _manifest, _policy) = seeded_privacy_projection_engine();
    let rows = engine
        .query_unscoped_sql_json("SELECT __id, email FROM users")
        .unwrap();
    rows.iter()
        .filter(|row| row.get("email").is_some_and(|value| !value.is_null()))
        .count() as u64
}

fn optimized_privacy_projection_filtered() -> u64 {
    let (engine, manifest, policy) = seeded_privacy_projection_engine();
    let report = engine
        .scrub_table_with_privacy_policy(&manifest.tables[0], &policy, true)
        .unwrap();
    report.affected_rows
}

fn run_point_select(indexed: bool) -> u64 {
    let connection = duckdb::Connection::open_in_memory().unwrap();
    seed_identity_table(&connection, indexed, 4_000);
    let mut checksum = 0_u64;
    let mut statement = connection
        .prepare(
            "SELECT payload FROM users WHERE table_name = ? AND tenant_id = ? AND __id = ? LIMIT 1",
        )
        .unwrap();
    for index in (0..4_000).step_by(37) {
        let payload: String = statement
            .query_row(
                duckdb::params![
                    "users",
                    format!("tenant_{:02}", index % 16),
                    format!("user_{index:06}")
                ],
                |row| row.get(0),
            )
            .unwrap();
        checksum ^= payload.len() as u64;
    }
    checksum
}

fn run_point_update(indexed: bool) -> u64 {
    let connection = duckdb::Connection::open_in_memory().unwrap();
    seed_identity_table(&connection, indexed, 4_000);
    let mut changed = 0_u64;
    let mut statement = connection
        .prepare("UPDATE users SET payload = ? WHERE table_name = ? AND tenant_id = ? AND __id = ?")
        .unwrap();
    for index in (0..4_000).step_by(41) {
        changed += statement
            .execute(duckdb::params![
                format!("updated_{index}"),
                "users",
                format!("tenant_{:02}", index % 16),
                format!("user_{index:06}")
            ])
            .unwrap() as u64;
    }
    changed
}

fn run_bulk_insert(indexed: bool) -> u64 {
    let connection = duckdb::Connection::open_in_memory().unwrap();
    create_identity_table(&connection);
    if indexed {
        connection
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_users_row_identity ON users (table_name, \
                 tenant_id, __id)",
                [],
            )
            .unwrap();
    }
    insert_identity_rows(&connection, 4_000)
}

fn seed_identity_table(connection: &duckdb::Connection, indexed: bool, row_count: usize) {
    create_identity_table(connection);
    insert_identity_rows(connection, row_count);
    if indexed {
        connection
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_users_row_identity ON users (table_name, \
                 tenant_id, __id)",
                [],
            )
            .unwrap();
    }
}

fn create_identity_table(connection: &duckdb::Connection) {
    connection
        .execute(
            "CREATE TABLE users (
                table_name VARCHAR,
                tenant_id VARCHAR,
                __id VARCHAR,
                payload VARCHAR
            )",
            [],
        )
        .unwrap();
}

fn insert_identity_rows(connection: &duckdb::Connection, row_count: usize) -> u64 {
    let transaction = connection.unchecked_transaction().unwrap();
    {
        let mut statement = transaction
            .prepare("INSERT INTO users VALUES (?, ?, ?, ?)")
            .unwrap();
        for index in 0..row_count {
            statement
                .execute(duckdb::params![
                    "users",
                    format!("tenant_{:02}", index % 16),
                    format!("user_{index:06}"),
                    format!("payload_{index}")
                ])
                .unwrap();
        }
    }
    transaction.commit().unwrap();
    row_count as u64
}

fn insert_identity_rows_one_by_one(connection: &duckdb::Connection, row_count: usize) -> u64 {
    for index in 0..row_count {
        connection
            .prepare("INSERT INTO users VALUES (?, ?, ?, ?)")
            .unwrap()
            .execute(duckdb::params![
                "users",
                format!("tenant_{:02}", index % 16),
                format!("user_{index:06}"),
                format!("payload_{index}")
            ])
            .unwrap();
    }
    row_count as u64
}

fn seeded_structured_query_engine() -> (AnalyticsEngine, AnalyticsManifest, StructuredQuery) {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").unwrap();
    let manifest = AnalyticsManifest::new(vec![table_registration(8)]);
    engine.ensure_manifest(&manifest).unwrap();
    let connection = engine.connection();
    let transaction = connection.unchecked_transaction().unwrap();
    {
        let mut statement = transaction
            .prepare(
                "INSERT INTO users (tenant_id, __id, table_name, column_00, column_01, column_02, \
                 column_03, item) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .unwrap();
        for index in 0..3_000 {
            statement
                .execute(duckdb::params![
                    format!("tenant_{:02}", index % 8),
                    format!("user_{index:06}"),
                    "users",
                    format!("value_00_{index}"),
                    format!("value_01_{index}"),
                    format!("value_02_{index}"),
                    format!("value_03_{index}"),
                    "{}"
                ])
                .unwrap();
        }
    }
    transaction.commit().unwrap();
    let query = StructuredQuery {
        analytics_table_name: "users".to_string(),
        table_alias: None,
        joins: Vec::new(),
        select: (0..4)
            .map(|index| QuerySelect::Column {
                table_alias: None,
                column_name: format!("column_{index:02}"),
                alias: Some(format!("c_{index:02}")),
            })
            .collect(),
        filters: vec![QueryPredicate::Eq {
            expression: QueryExpression::Column {
                table_alias: None,
                column_name: "tenant_id".to_string(),
            },
            value: serde_json::Value::String("tenant_03".to_string()),
        }],
        group_by: Vec::new(),
        order_by: vec![QueryOrder {
            expression: QueryExpression::Column {
                table_alias: None,
                column_name: "column_00".to_string(),
            },
            direction: None,
        }],
        limit: Some(500),
        offset: None,
    };
    (engine, manifest, query)
}

fn seeded_privacy_projection_engine() -> (AnalyticsEngine, AnalyticsManifest, PrivacyPolicy) {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").unwrap();
    let mut table = table_registration(1);
    table.document_column = None;
    table.projection_columns = Some(vec![ProjectionColumn {
        column_name: "email".to_string(),
        attribute_path: "email".to_string(),
        column_type: Some(AnalyticsColumnType::Primitive {
            primitive: PrimitiveColumnType::VarChar,
        }),
    }]);
    let manifest = AnalyticsManifest::new(vec![table]);
    engine.ensure_manifest(&manifest).unwrap();
    let connection = engine.connection();
    let transaction = connection.unchecked_transaction().unwrap();
    {
        let mut statement = transaction
            .prepare("INSERT INTO users (tenant_id, __id, table_name, email) VALUES (?, ?, ?, ?)")
            .unwrap();
        for index in 0..4_000 {
            let email = if index % 24 == 0 {
                Some(format!("private_{index}@example.test"))
            } else {
                None
            };
            statement
                .execute(duckdb::params![
                    "tenant_a",
                    format!("user_{index:06}"),
                    "users",
                    email
                ])
                .unwrap();
        }
    }
    transaction.commit().unwrap();
    let policy = PrivacyPolicy::new("privacy-v1")
        .unwrap()
        .with_denied_key_name("email");
    (engine, manifest, policy)
}

fn baseline_clone_projection_columns(columns: &[ProjectionColumn]) -> u64 {
    let mut total = 0_u64;
    for _ in 0..LOOP_COUNT {
        let cloned = columns.to_vec();
        total += cloned.len() as u64;
    }
    total
}

fn registered_column_hashset(table: &TableRegistration) -> HashSet<String> {
    let mut names = HashSet::from([
        "tenant_id".to_string(),
        "__id".to_string(),
        "table_name".to_string(),
    ]);
    for column in columns_for_registration(table) {
        names.insert(column.column_name);
    }
    names
}

fn registered_column_vec(table: &TableRegistration) -> Vec<String> {
    let mut names = vec![
        "tenant_id".to_string(),
        "__id".to_string(),
        "table_name".to_string(),
    ];
    for column in columns_for_registration(table) {
        names.push(column.column_name);
    }
    names
}

fn projection_columns(count: usize) -> Vec<ProjectionColumn> {
    (0..count)
        .map(|index| ProjectionColumn {
            column_name: format!("column_{index:02}"),
            attribute_path: format!("profile.section_{index}.value"),
            column_type: Some(AnalyticsColumnType::Primitive {
                primitive: PrimitiveColumnType::VarChar,
            }),
        })
        .collect()
}

fn insert_row_fixture(count: usize) -> BTreeMap<String, serde_json::Value> {
    (0..count)
        .map(|index| {
            (
                format!("column_{index:02}"),
                serde_json::Value::String(format!("value_{index:02}")),
            )
        })
        .collect()
}

fn storage_item(count: usize) -> StorageItem {
    (0..count)
        .map(|index| {
            (
                format!("profile.section_{index}.value"),
                StorageValue::S(format!("value_{index}")),
            )
        })
        .collect()
}

fn list_storage_item(count: usize) -> StorageItem {
    HashMap::from([(
        "profiles".to_string(),
        StorageValue::L(
            (0..count)
                .map(|index| {
                    StorageValue::M(HashMap::from([(
                        "email".to_string(),
                        StorageValue::S(format!("user_{index}@example.com")),
                    )]))
                })
                .collect(),
        ),
    )])
}

fn table_registration(count: usize) -> TableRegistration {
    TableRegistration {
        source_table_name: "tenant_users".to_string(),
        analytics_table_name: "users".to_string(),
        source_table_name_prefix: None,
        tenant_id: Some("tenant_a".to_string()),
        tenant_selector: TenantSelector::default(),
        row_identity: RowIdentity::default(),
        document_column: Some("item".to_string()),
        skip_delete: false,
        retention: None,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        projection_attribute_names: None,
        projection_columns: Some(projection_columns(count)),
        columns: Vec::new(),
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
        table_scope: analytics_contract::TableScope::default(),
        join_policy: analytics_contract::JoinPolicy::default(),
    }
}

fn structured_query(count: usize) -> StructuredQuery {
    StructuredQuery {
        analytics_table_name: "users".to_string(),
        table_alias: None,
        joins: Vec::new(),
        select: (0..count)
            .map(|index| QuerySelect::Column {
                table_alias: None,
                column_name: format!("column_{index:02}"),
                alias: Some(format!("c_{index:02}")),
            })
            .collect(),
        filters: vec![QueryPredicate::Eq {
            expression: QueryExpression::Column {
                table_alias: None,
                column_name: "tenant_id".to_string(),
            },
            value: serde_json::Value::String("tenant_a".to_string()),
        }],
        group_by: Vec::new(),
        order_by: vec![QueryOrder {
            expression: QueryExpression::Column {
                table_alias: None,
                column_name: "column_00".to_string(),
            },
            direction: None,
        }],
        limit: Some(100),
        offset: None,
    }
}
