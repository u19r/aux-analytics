use std::{hint::black_box, time::Instant};

use serde_json::{Value, json};

use crate::{
    QueryExpression, QueryPredicate, QuerySelect, StructuredQuery,
    constants::{
        CANONICAL_PROBE_QUERIES, CANONICAL_PROBE_SAMPLES, CANONICAL_PROBE_WARMUPS,
        EXPECTED_CANONICAL_JSON, EXPECTED_QUERY_HASH, QUERY_CANONICAL_PROBE_ITERATIONS,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CanonicalProbeSample {
    checksum: u64,
    bytes: usize,
    elapsed_ns: u128,
    allocations: u64,
    allocated_bytes: u64,
}

#[test]
fn given_nested_literals_when_canonicalized_then_bytes_and_hash_are_stable() {
    let query = representative_query(representative_literal());
    let reordered = representative_query(reordered_literal());

    assert_eq!(
        query.canonical_json().expect("query canonicalizes"),
        EXPECTED_CANONICAL_JSON
    );
    assert_eq!(
        query.query_hash().expect("query hashes"),
        EXPECTED_QUERY_HASH
    );
    assert_eq!(
        query.canonical_json().expect("query canonicalizes"),
        reordered
            .canonical_json()
            .expect("reordered query canonicalizes")
    );
    assert_eq!(
        query.query_hash().expect("query hashes"),
        reordered.query_hash().expect("reordered query hashes")
    );
    assert_eq!(
        legacy_canonical_json(&query).expect("legacy query canonicalizes"),
        EXPECTED_CANONICAL_JSON
    );
}

#[test]
#[ignore = "release-only query canonical allocation and CPU probe"]
fn given_representative_queries_when_canonicalized_then_rebuild_cost_is_reduced() {
    let queries = canonical_probe_queries();
    for _ in 0..CANONICAL_PROBE_WARMUPS {
        let baseline = measure_path(&queries, "baseline", legacy_canonical_json);
        let candidate = measure_path(&queries, "candidate", StructuredQuery::canonical_json);
        assert_eq!(baseline.checksum, candidate.checksum);
        assert_eq!(baseline.bytes, candidate.bytes);
    }

    let mut baseline_samples = Vec::with_capacity(CANONICAL_PROBE_SAMPLES);
    let mut candidate_samples = Vec::with_capacity(CANONICAL_PROBE_SAMPLES);
    for _ in 0..CANONICAL_PROBE_SAMPLES {
        let baseline = measure_path(&queries, "baseline", legacy_canonical_json);
        let candidate = measure_path(&queries, "candidate", StructuredQuery::canonical_json);
        assert_eq!(baseline.checksum, candidate.checksum);
        assert_eq!(baseline.bytes, candidate.bytes);
        baseline_samples.push(baseline);
        candidate_samples.push(candidate);
    }

    println!(
        "query-canonical | queries={CANONICAL_PROBE_QUERIES} | \
         iterations={QUERY_CANONICAL_PROBE_ITERATIONS} | baseline={baseline_samples:?} | \
         candidate={candidate_samples:?}"
    );
    assert!(
        baseline_samples
            .iter()
            .zip(&candidate_samples)
            .all(
                |(baseline, candidate)| candidate.allocations < baseline.allocations
                    && candidate.allocated_bytes < baseline.allocated_bytes
            )
    );
}

fn representative_query(value: Value) -> StructuredQuery {
    StructuredQuery {
        analytics_table_name: "events".to_string(),
        table_alias: None,
        joins: Vec::new(),
        select: vec![QuerySelect::Column {
            table_alias: None,
            column_name: "payload".to_string(),
            alias: None,
        }],
        filters: vec![QueryPredicate::Eq {
            expression: QueryExpression::Column {
                table_alias: None,
                column_name: "payload".to_string(),
            },
            value,
        }],
        group_by: Vec::new(),
        order_by: Vec::new(),
        limit: Some(25),
        offset: Some(3),
    }
}

fn canonical_probe_queries() -> Vec<StructuredQuery> {
    (0..CANONICAL_PROBE_QUERIES)
        .map(|index| {
            let mut query = representative_query(representative_literal());
            query.analytics_table_name = format!("events_{index}");
            query.limit = Some(100_u32 + index as u32);
            query
        })
        .collect()
}

fn representative_literal() -> Value {
    json!({
        "zeta": [
            {"beta": 2, "alpha": [true, null, {"z": "last", "a": "first"}]},
            {"middle": {"right": 4, "left": 3}}
        ],
        "alpha": {
            "nested": {"z": 1, "a": 0},
            "array": [{"z": 7, "a": 6}]
        }
    })
}

fn reordered_literal() -> Value {
    json!({
        "alpha": {
            "array": [{"a": 6, "z": 7}],
            "nested": {"a": 0, "z": 1}
        },
        "zeta": [
            {"alpha": [true, null, {"a": "first", "z": "last"}], "beta": 2},
            {"middle": {"left": 3, "right": 4}}
        ]
    })
}

fn legacy_canonical_json(query: &StructuredQuery) -> Result<String, serde_json::Error> {
    let value = serde_json::to_value(query)?;
    serde_json::to_string(&legacy_sort_json(value))
}

fn legacy_sort_json(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(legacy_sort_json).collect()),
        Value::Object(fields) => {
            let mut entries = fields.into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            let mut sorted = serde_json::Map::new();
            for (key, value) in entries {
                sorted.insert(key, legacy_sort_json(value));
            }
            Value::Object(sorted)
        }
        value => value,
    }
}

fn measure_path(
    queries: &[StructuredQuery],
    label: &'static str,
    canonicalize: fn(&StructuredQuery) -> Result<String, serde_json::Error>,
) -> CanonicalProbeSample {
    let started = Instant::now();
    let guard = alloc_counter::AllocationGuard::start(
        module_path!(),
        "query-canonical",
        file!(),
        line!(),
        Some(label),
    );
    let mut checksum = 1_469_598_103_934_665_603_u64;
    let mut bytes = 0;
    for _ in 0..QUERY_CANONICAL_PROBE_ITERATIONS {
        for query in queries {
            let encoded = canonicalize(query).expect("query canonicalizes");
            bytes += encoded.len();
            for byte in encoded.as_bytes() {
                checksum ^= u64::from(*byte);
                checksum = checksum.wrapping_mul(1_099_511_628_211);
            }
            black_box(encoded);
        }
    }
    let elapsed_ns = started.elapsed().as_nanos();
    let report = guard.finish();
    alloc_counter::emit_report(&report).expect("query canonical allocation report");
    CanonicalProbeSample {
        checksum,
        bytes,
        elapsed_ns,
        allocations: report.allocation_count,
        allocated_bytes: report.allocated_bytes,
    }
}
