use std::{hint::black_box, time::Instant};

use analytics_contract::{AnalyticsManifest, StructuredQuery};
use analytics_fixtures::users_manifest;
use axum::http::Method;
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::sync::RwLock;

use crate::{
    request_validation::{RequestValidators, resolve_schema_refs},
    types::{IngestStreamRecordRequest, TenantQueryBatchRequest, TenantQueryRequest},
};

#[test]
#[ignore = "performance test only"]
fn perf_request_validation_body_sizes_and_routes() {
    let validators = RequestValidators::compile();
    for size in [1_024, 65_536, 2_097_000] {
        let iterations = if size < 65_536 {
            20
        } else if size < 1_000_000 {
            5
        } else {
            1
        };
        measure_route::<IngestStreamRecordRequest>(
            &validators,
            "ingest-valid",
            "/ingest/users",
            sized_payload(ingest_payload(true), size),
            iterations,
        );
        measure_route::<IngestStreamRecordRequest>(
            &validators,
            "ingest-invalid",
            "/ingest/users",
            sized_payload(ingest_payload(false), size),
            iterations,
        );
        measure_route::<TenantQueryRequest>(
            &validators,
            "tenant-query-valid",
            "/tenant-query",
            sized_payload(tenant_query_payload(true), size),
            iterations,
        );
        measure_route::<TenantQueryRequest>(
            &validators,
            "tenant-query-invalid",
            "/tenant-query",
            sized_payload(tenant_query_payload(false), size),
            iterations,
        );
        measure_route::<TenantQueryBatchRequest>(
            &validators,
            "query-batch-valid",
            "/tenant-query-batch",
            sized_payload(query_batch_payload(true), size),
            iterations,
        );
        measure_route::<TenantQueryBatchRequest>(
            &validators,
            "query-batch-invalid",
            "/tenant-query-batch",
            sized_payload(query_batch_payload(false), size),
            iterations,
        );
    }

    let started = Instant::now();
    for _ in 0..10_000 {
        black_box(
            validators
                .schema(&Method::POST, "/operations/op-1/cancel")
                .is_none(),
        );
    }
    println!(
        "request-validation | operation-no-body | iterations=10000 | compilations=0 | parses=0 | \
         elapsed_ns={}",
        started.elapsed().as_nanos()
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "performance test only"]
async fn perf_manifest_snapshot_reads_by_table_count() {
    for table_count in [10, 100, 1_000] {
        let iterations = match table_count {
            10 => 1_000,
            100 => 200,
            _ => 20,
        };
        let manifest = manifest_with_tables(table_count);
        let baseline = RwLock::new(manifest.clone());
        let candidate = RwLock::new(std::sync::Arc::new(manifest));

        let baseline_started = Instant::now();
        let baseline_guard = alloc_counter::AllocationGuard::start(
            module_path!(),
            "manifest-snapshot",
            file!(),
            line!(),
            Some("baseline"),
        );
        for _ in 0..iterations {
            black_box(baseline.read().await.clone());
        }
        let baseline_report = baseline_guard.finish();
        let baseline_elapsed = baseline_started.elapsed();

        let candidate_started = Instant::now();
        let candidate_guard = alloc_counter::AllocationGuard::start(
            module_path!(),
            "manifest-snapshot",
            file!(),
            line!(),
            Some("candidate"),
        );
        for _ in 0..iterations {
            black_box(candidate.read().await.clone());
        }
        let candidate_report = candidate_guard.finish();
        let candidate_elapsed = candidate_started.elapsed();
        alloc_counter::emit_report(&baseline_report);
        alloc_counter::emit_report(&candidate_report);
        println!(
            "manifest-snapshot | tables={table_count} | iterations={iterations} | baseline_ns={} \
             | candidate_ns={} | baseline_allocs={} | candidate_allocs={} | baseline_bytes={} | \
             candidate_bytes={}",
            baseline_elapsed.as_nanos(),
            candidate_elapsed.as_nanos(),
            baseline_report.allocation_count,
            candidate_report.allocation_count,
            baseline_report.allocated_bytes,
            candidate_report.allocated_bytes,
        );
    }
}

fn manifest_with_tables(table_count: usize) -> AnalyticsManifest {
    let template = users_manifest().tables.remove(0);
    AnalyticsManifest::new(
        (0..table_count)
            .map(|index| {
                let mut table = template.clone();
                table.analytics_table_name = format!("users_{index}");
                table
            })
            .collect(),
    )
}

fn measure_route<T>(
    validators: &RequestValidators,
    name: &'static str,
    path: &'static str,
    body: Vec<u8>,
    iterations: usize,
) where
    T: DeserializeOwned + JsonSchema,
{
    let baseline_started = Instant::now();
    let baseline_allocations = allocation_report(name, "baseline", || {
        for _ in 0..iterations {
            black_box(baseline_validate::<T>(path, &body));
        }
    });
    let baseline_elapsed = baseline_started.elapsed();
    let candidate_started = Instant::now();
    let candidate_allocations = allocation_report(name, "candidate", || {
        for _ in 0..iterations {
            black_box(candidate_validate::<T>(validators, path, &body));
        }
    });
    let candidate_elapsed = candidate_started.elapsed();
    alloc_counter::emit_report(&baseline_allocations);
    alloc_counter::emit_report(&candidate_allocations);
    println!(
        "request-validation | {name} | bytes={} | iterations={iterations} | \
         baseline_compilations={} | candidate_compilations=0 | baseline_parses={} | \
         candidate_parses={} | baseline_ns={} | candidate_ns={} | baseline_allocs={} | \
         candidate_allocs={} | baseline_bytes={} | candidate_bytes={}",
        body.len(),
        iterations * 2,
        iterations * 2,
        iterations,
        baseline_elapsed.as_nanos(),
        candidate_elapsed.as_nanos(),
        baseline_allocations.allocation_count,
        candidate_allocations.allocation_count,
        baseline_allocations.allocated_bytes,
        candidate_allocations.allocated_bytes,
    );
}

fn baseline_validate<T>(path: &str, body: &[u8]) -> bool
where T: DeserializeOwned + JsonSchema {
    let spec = crate::openapi::build_json();
    let schema = request_schema(&spec, &Method::POST, path).expect("request schema");
    let validator = jsonschema::validator_for(&schema).expect("OpenAPI validator");
    let first_payload = serde_json::from_slice::<Value>(body).expect("JSON payload");
    let first_valid = validator.is_valid(&first_payload);

    let second_payload = serde_json::from_slice::<Value>(body).expect("rebuilt JSON payload");
    let typed_schema = serde_json::to_value(schemars::schema_for!(T)).expect("typed schema");
    let typed_validator = jsonschema::validator_for(&typed_schema).expect("typed validator");
    first_valid
        && typed_validator.is_valid(&second_payload)
        && serde_json::from_value::<T>(second_payload).is_ok()
}

fn candidate_validate<T>(validators: &RequestValidators, path: &str, body: &[u8]) -> bool
where T: DeserializeOwned {
    let payload = serde_json::from_slice::<Value>(body).expect("JSON payload");
    validators
        .schema(&Method::POST, path)
        .expect("compiled validator")
        .is_valid(&payload)
        && serde_json::from_value::<T>(payload).is_ok()
}

fn request_schema(spec: &Value, method: &Method, path: &str) -> Option<Value> {
    let paths = spec.get("paths")?.as_object()?;
    let method = method.as_str().to_ascii_lowercase();
    paths.iter().find_map(|(template, item)| {
        path_matches(template, path)
            .then(|| {
                item.get(&method)?
                    .pointer("/requestBody/content/application~1json/schema")
                    .cloned()
                    .map(|schema| resolve_schema_refs(schema, spec))
            })
            .flatten()
    })
}

fn path_matches(template: &str, path: &str) -> bool {
    let mut template = template.trim_matches('/').split('/');
    let mut path = path.trim_matches('/').split('/');
    loop {
        match (template.next(), path.next()) {
            (None, None) => return true,
            (Some(template), Some(path))
                if (template.starts_with('{') && template.ends_with('}')) || template == path => {}
            _ => return false,
        }
    }
}

fn sized_payload(mut payload: Value, size: usize) -> Vec<u8> {
    payload["padding"] = Value::String(String::new());
    let overhead = serde_json::to_vec(&payload).expect("payload").len();
    assert!(overhead <= size, "fixture exceeds requested body size");
    payload["padding"] = Value::String("x".repeat(size - overhead));
    let bytes = serde_json::to_vec(&payload).expect("payload");
    assert_eq!(bytes.len(), size);
    bytes
}

fn ingest_payload(valid: bool) -> Value {
    if !valid {
        return json!({"record_key": "user-1"});
    }
    json!({
        "record_key": "user-1",
        "record": {"Keys": {}, "SequenceNumber": "1", "NewImage": {}}
    })
}

fn tenant_query_payload(valid: bool) -> Value {
    let mut payload = json!({"query": minimal_query()});
    if valid {
        payload["target_tenant_id"] = json!("tenant_01");
    }
    payload
}

fn query_batch_payload(valid: bool) -> Value {
    let mut payload = json!({
        "queries": [{"name": "count", "query": minimal_query()}]
    });
    if valid {
        payload["target_tenant_id"] = json!("tenant_01");
    }
    payload
}

fn minimal_query() -> StructuredQuery {
    serde_json::from_value(json!({
        "analytics_table_name": "users",
        "select": [{"kind": "count", "alias": "count"}]
    }))
    .expect("minimal query")
}

fn allocation_report(
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
