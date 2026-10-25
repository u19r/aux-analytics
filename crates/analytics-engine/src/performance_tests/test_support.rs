use super::*;

#[derive(Clone)]
pub(super) struct RealisticWorkload {
    pub(super) manifest: AnalyticsManifest,
    pub(super) records: Vec<StreamRecordBatchItem>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ProductionProbe {
    pub(super) output_digest: u64,
    pub(super) serialized_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct AllocationSample {
    pub(super) count: u64,
    pub(super) bytes: u64,
}

#[derive(Debug)]
pub(super) struct ProductionCandidateResult {
    pub(super) name: &'static str,
    pub(super) owner: &'static str,
    pub(super) baseline_samples: [Duration; PRODUCTION_PAIRED_SAMPLES],
    pub(super) candidate_samples: [Duration; PRODUCTION_PAIRED_SAMPLES],
    pub(super) pair_deltas: [f64; PRODUCTION_PAIRED_SAMPLES],
    pub(super) baseline_allocations: [AllocationSample; PRODUCTION_PAIRED_SAMPLES],
    pub(super) candidate_allocations: [AllocationSample; PRODUCTION_PAIRED_SAMPLES],
    pub(super) output_digest: u64,
    pub(super) serialized_bytes: u64,
    pub(super) kept: bool,
}

pub(super) fn measure_production_candidate(
    name: &'static str,
    owner: &'static str,
    mut baseline: impl FnMut() -> ProductionProbe,
    mut candidate: impl FnMut() -> ProductionProbe,
) -> ProductionCandidateResult {
    let mut baseline_samples = [Duration::ZERO; PRODUCTION_PAIRED_SAMPLES];
    let mut candidate_samples = [Duration::ZERO; PRODUCTION_PAIRED_SAMPLES];
    let mut output_digest = None;
    let mut serialized_bytes = None;
    for index in 0..PRODUCTION_WARMUP_RUNS {
        let (baseline_probe, candidate_probe) = if index.is_multiple_of(2) {
            let (_, baseline_probe) = timed_production_probe(&mut baseline);
            let (_, candidate_probe) = timed_production_probe(&mut candidate);
            (baseline_probe, candidate_probe)
        } else {
            let (_, candidate_probe) = timed_production_probe(&mut candidate);
            let (_, baseline_probe) = timed_production_probe(&mut baseline);
            (baseline_probe, candidate_probe)
        };
        assert_eq!(
            baseline_probe, candidate_probe,
            "semantic output changed for {name} during warmup"
        );
    }
    for index in 0..PRODUCTION_PAIRED_SAMPLES {
        let (baseline_time, baseline_probe, candidate_time, candidate_probe) =
            if index.is_multiple_of(2) {
                let (baseline_time, baseline_probe) = timed_production_probe(&mut baseline);
                let (candidate_time, candidate_probe) = timed_production_probe(&mut candidate);
                (
                    baseline_time,
                    baseline_probe,
                    candidate_time,
                    candidate_probe,
                )
            } else {
                let (candidate_time, candidate_probe) = timed_production_probe(&mut candidate);
                let (baseline_time, baseline_probe) = timed_production_probe(&mut baseline);
                (
                    baseline_time,
                    baseline_probe,
                    candidate_time,
                    candidate_probe,
                )
            };
        baseline_samples[index] = baseline_time;
        candidate_samples[index] = candidate_time;
        assert_eq!(
            baseline_probe, candidate_probe,
            "semantic output changed for {name}"
        );
        output_digest = Some(baseline_probe.output_digest);
        serialized_bytes = Some(baseline_probe.serialized_bytes);
    }
    let mut baseline_allocations =
        [AllocationSample { count: 0, bytes: 0 }; PRODUCTION_PAIRED_SAMPLES];
    let mut candidate_allocations =
        [AllocationSample { count: 0, bytes: 0 }; PRODUCTION_PAIRED_SAMPLES];
    for index in 0..PRODUCTION_PAIRED_SAMPLES {
        let (baseline_sample, baseline_probe, candidate_sample, candidate_probe) =
            if index.is_multiple_of(2) {
                let (baseline_sample, baseline_probe) =
                    allocation_probe(name, "baseline", &mut baseline);
                let (candidate_sample, candidate_probe) =
                    allocation_probe(name, "candidate", &mut candidate);
                (
                    baseline_sample,
                    baseline_probe,
                    candidate_sample,
                    candidate_probe,
                )
            } else {
                let (candidate_sample, candidate_probe) =
                    allocation_probe(name, "candidate", &mut candidate);
                let (baseline_sample, baseline_probe) =
                    allocation_probe(name, "baseline", &mut baseline);
                (
                    baseline_sample,
                    baseline_probe,
                    candidate_sample,
                    candidate_probe,
                )
            };
        assert_eq!(
            baseline_probe, candidate_probe,
            "semantic output changed for {name} during allocation sample"
        );
        baseline_allocations[index] = baseline_sample;
        candidate_allocations[index] = candidate_sample;
    }
    let pair_deltas = std::array::from_fn(|index| {
        cpu_delta_percent(baseline_samples[index], candidate_samples[index])
    });
    let cpu_win = pair_deltas.iter().all(|delta| *delta >= 5.0);
    let allocation_win = allocation_reduction(&baseline_allocations, &candidate_allocations);
    // Allocation-only wins require strict count-and-byte reductions and a
    // non-negative median CPU delta and no timing dip beyond five percent.
    let allocation_cpu_stable = allocation_cpu_stable(&pair_deltas);
    ProductionCandidateResult {
        name,
        owner,
        baseline_samples,
        candidate_samples,
        pair_deltas,
        baseline_allocations,
        candidate_allocations,
        output_digest: output_digest.expect("paired production probe ran"),
        serialized_bytes: serialized_bytes.expect("paired production probe ran"),
        kept: cpu_win || (allocation_win && allocation_cpu_stable),
    }
}

pub(super) fn timed_production_probe(
    run: &mut impl FnMut() -> ProductionProbe,
) -> (Duration, ProductionProbe) {
    let mut observed = None;
    let mut samples = [Duration::ZERO; PRODUCTION_TIMING_TRIALS];
    for sample in &mut samples {
        let started = Instant::now();
        for _ in 0..CANDIDATE_REPETITIONS {
            observed = Some(black_box(run()));
        }
        *sample = started.elapsed();
    }
    samples.sort_unstable();
    (
        samples[PRODUCTION_TIMING_TRIALS / 2],
        observed.expect("repeated production probe ran"),
    )
}

pub(super) fn allocation_probe(
    name: &'static str,
    label: &'static str,
    run: &mut impl FnMut() -> ProductionProbe,
) -> (AllocationSample, ProductionProbe) {
    let mut observed = None;
    let report = measure_allocations(name, label, || {
        for _ in 0..CANDIDATE_REPETITIONS {
            observed = Some(black_box(run()));
        }
    });
    (
        AllocationSample {
            count: report.allocation_count,
            bytes: report.allocated_bytes,
        },
        observed.expect("allocation probe ran"),
    )
}

pub(super) fn cpu_delta_percent(baseline: Duration, candidate: Duration) -> f64 {
    ((baseline.as_secs_f64() - candidate.as_secs_f64()) / baseline.as_secs_f64()) * 100.0
}

pub(super) fn allocation_reduction(
    baseline: &[AllocationSample; PRODUCTION_PAIRED_SAMPLES],
    candidate: &[AllocationSample; PRODUCTION_PAIRED_SAMPLES],
) -> bool {
    let count_non_increasing = candidate
        .iter()
        .zip(baseline)
        .all(|(candidate, baseline)| candidate.count <= baseline.count);
    let bytes_non_increasing = candidate
        .iter()
        .zip(baseline)
        .all(|(candidate, baseline)| candidate.bytes <= baseline.bytes);
    let strict_reduction = candidate.iter().zip(baseline).any(|(candidate, baseline)| {
        candidate.count < baseline.count || candidate.bytes < baseline.bytes
    });
    count_non_increasing && bytes_non_increasing && strict_reduction
}

pub(super) fn allocation_cpu_stable(deltas: &[f64]) -> bool {
    if deltas.is_empty() {
        return false;
    }
    let minimum = deltas.iter().copied().fold(f64::INFINITY, f64::min);
    let maximum = deltas.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let median = deltas.iter().sum::<f64>() - minimum - maximum;
    // Permit one bounded timing dip, but reject a material regression and
    // require a non-negative median improvement.
    minimum >= -5.0 && median >= MIN_ALLOCATION_CPU_DELTA_PERCENT
}

pub(super) fn print_production_candidate_results(
    prefix: &str,
    results: &[ProductionCandidateResult],
) {
    for result in results {
        println!(
            "{prefix} | name={} | owner={} | baseline_ns={:?} | candidate_ns={:?} | \
             pair_delta_percent={:?} | baseline_allocations={:?} | candidate_allocations={:?} | \
             output_digest={} | returned_serialized_bytes={} | kept={}",
            result.name,
            result.owner,
            result
                .baseline_samples
                .iter()
                .map(Duration::as_nanos)
                .collect::<Vec<_>>(),
            result
                .candidate_samples
                .iter()
                .map(Duration::as_nanos)
                .collect::<Vec<_>>(),
            result.pair_deltas,
            result.baseline_allocations,
            result.candidate_allocations,
            result.output_digest,
            result.serialized_bytes,
            result.kept,
        );
    }
}

pub(super) fn print_production_summary(prefix: &str, results: &[ProductionCandidateResult]) {
    let retained = results.iter().filter(|result| result.kept).count();
    println!(
        "{prefix}-summary | candidates={} | retained={} | production_candidates={} | \
         semantic_equality=all | paired_samples={}",
        results.len(),
        retained,
        results.len(),
        PRODUCTION_PAIRED_SAMPLES,
    );
}
pub(super) fn digest_bytes(bytes: &[u8]) -> u64 {
    digest_bytes_from(1469598103934665603_u64, bytes)
}

pub(super) fn digest_bytes_from(mut digest: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        digest = digest
            .wrapping_mul(1099511628211)
            .wrapping_add(u64::from(*byte));
    }
    digest
}

pub(super) fn digest_duckdb_values(values: &[duckdb::types::Value], mut digest: u64) -> u64 {
    for value in values {
        digest = match value {
            duckdb::types::Value::Null => digest_bytes_from(digest, &[0]),
            duckdb::types::Value::Boolean(value) => {
                let digest = digest_bytes_from(digest, &[1]);
                digest_bytes_from(digest, &[u8::from(*value)])
            }
            duckdb::types::Value::BigInt(value) => {
                let digest = digest_bytes_from(digest, &[2]);
                digest_bytes_from(digest, &value.to_le_bytes())
            }
            duckdb::types::Value::Text(value) => {
                let digest = digest_bytes_from(digest, &[3]);
                let digest = digest_bytes_from(digest, &(value.len() as u64).to_le_bytes());
                digest_bytes_from(digest, value.as_bytes())
            }
            other => panic!("unhandled fixture batch value variant: {other:?}"),
        };
    }
    digest
}

pub(super) fn realistic_batch_rows(
    workload: &RealisticWorkload,
    count: usize,
) -> Vec<BTreeMap<String, serde_json::Value>> {
    let engine = realistic_engine(workload);
    let table = &workload.manifest.tables[0];
    workload.records[..count]
        .iter()
        .map(|record| {
            let item = record.record.new_image.as_ref().expect("fixture image");
            engine
                .row_from_item(
                    table,
                    RowSource {
                        stream_key_digest: None,
                        source_table_name: &record.source_table_name,
                        record_key: &record.record_key,
                        record_keys: &record.record.keys,
                    },
                    item,
                    1_900_000_000,
                    None,
                )
                .expect("fixture row materializes")
        })
        .collect()
}

pub(super) fn realistic_row_table(workload: &RealisticWorkload) -> TableRegistration {
    let mut table = workload.manifest.tables[0].clone();
    table.projection_columns = None;
    table.columns = columns_for_registration(&workload.manifest.tables[0]);
    table
}

pub(super) fn realistic_document_row_table(workload: &RealisticWorkload) -> TableRegistration {
    let mut table = realistic_row_table(workload);
    table.document_column = Some("item".to_string());
    table.columns = columns_for_registration(&table);
    table
}

pub(super) fn row_reference(
    item: &StorageItem,
    record: &StreamRecordBatchItem,
) -> BTreeMap<String, serde_json::Value> {
    let mut row = item
        .iter()
        .map(|(name, value)| {
            (
                name.clone(),
                value.to_json_value().expect("fixture value converts"),
            )
        })
        .collect::<BTreeMap<_, _>>();
    row.insert(
        "table_name".to_string(),
        serde_json::Value::String(record.source_table_name.clone()),
    );
    row.insert(
        "tenant_id".to_string(),
        serde_json::Value::String(match item.get("tenant_id").expect("fixture tenant") {
            StorageValue::S(value) => value.clone(),
            _ => panic!("fixture tenant is a string"),
        }),
    );
    row.insert(
        "__id".to_string(),
        serde_json::Value::String(hex_key(&record.record_key)),
    );
    row
}

pub(super) fn hex_key(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut value, byte| {
            value.push(char::from(
                crate::constants::HEX_DIGITS[usize::from(byte >> 4)],
            ));
            value.push(char::from(
                crate::constants::HEX_DIGITS[usize::from(byte & 0x0f)],
            ));
            value
        })
}
pub(super) fn combine_probes(probes: impl IntoIterator<Item = ProductionProbe>) -> ProductionProbe {
    let mut output_digest = 1469598103934665603_u64;
    let mut serialized_bytes = 0_u64;
    for probe in probes {
        output_digest = digest_bytes_from(output_digest, &probe.output_digest.to_le_bytes());
        serialized_bytes = serialized_bytes.saturating_add(probe.serialized_bytes);
    }
    ProductionProbe {
        output_digest,
        serialized_bytes,
    }
}
pub(super) fn measure_allocations(
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

pub(super) fn realistic_storage_item_to_json(item: &StorageItem) -> serde_json::Value {
    let mut object = serde_json::Map::new();
    let ordered = item.iter().collect::<BTreeMap<_, _>>();
    for (name, value) in ordered {
        object.insert(name.clone(), value.to_json_value().unwrap());
    }
    serde_json::Value::Object(object)
}

pub(super) fn realistic_workload(record_count: usize) -> RealisticWorkload {
    RealisticWorkload {
        manifest: AnalyticsManifest::new(vec![realistic_table_registration()]),
        records: (0..record_count).map(realistic_record).collect(),
    }
}

pub(super) fn realistic_engine(workload: &RealisticWorkload) -> AnalyticsEngine {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").unwrap();
    engine.ensure_manifest(&workload.manifest).unwrap();
    engine
}

pub(super) fn realistic_table_registration() -> TableRegistration {
    let projected = [
        ("gsi1_pk", PrimitiveColumnType::VarChar),
        ("gsi1_sk", PrimitiveColumnType::VarChar),
        ("gsi2_pk", PrimitiveColumnType::VarChar),
        ("gsi2_sk", PrimitiveColumnType::VarChar),
        ("payload_a", PrimitiveColumnType::VarChar),
        ("payload_b", PrimitiveColumnType::VarChar),
        ("payload_c", PrimitiveColumnType::VarChar),
        ("status", PrimitiveColumnType::VarChar),
        ("ttl", PrimitiveColumnType::BigInt),
    ];
    TableRegistration {
        source_table_name: "source_items".to_string(),
        analytics_table_name: "realistic_items".to_string(),
        source_table_name_prefix: None,
        row_expansion: None,
        tenant_id: None,
        tenant_selector: TenantSelector::Attribute {
            attribute_name: "tenant_id".to_string(),
        },
        row_identity: RowIdentity::RecordKey,
        document_column: None,
        skip_delete: false,
        retention: None,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        projection_attribute_names: None,
        projection_columns: Some(
            projected
                .into_iter()
                .map(|(name, primitive)| ProjectionColumn {
                    column_name: name.to_string(),
                    attribute_path: name.to_string(),
                    column_type: Some(AnalyticsColumnType::Primitive { primitive }),
                })
                .collect(),
        ),
        columns: Vec::new(),
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
        table_scope: analytics_contract::TableScope::default(),
        join_policy: analytics_contract::JoinPolicy::default(),
    }
}

pub(super) fn realistic_record(index: usize) -> StreamRecordBatchItem {
    let tenant = if index.is_multiple_of(2) {
        "tenant-a"
    } else {
        "tenant-b"
    };
    let tenant_key = format!("{tenant}#item-{index:08}");
    let mut item = VecMap::with_capacity(REALISTIC_ATTRIBUTE_COUNT);
    item.insert("tenant_id".to_string(), StorageValue::S(tenant.to_string()));
    for family in 1..=2 {
        item.insert(
            format!("gsi{family}_pk"),
            StorageValue::S(format!("gsi{family}#pk#{tenant_key}")),
        );
        item.insert(
            format!("gsi{family}_sk"),
            StorageValue::S(format!("gsi{family}#sk#{index:012}")),
        );
    }
    for (name, length, salt) in [
        ("payload_a", 360, 17),
        ("payload_b", 400, 31),
        ("payload_c", 440, 47),
    ] {
        item.insert(
            name.to_string(),
            StorageValue::S(realistic_payload(index, salt, length)),
        );
    }
    item.insert(
        "status".to_string(),
        StorageValue::S(format!("state-{}", index % 3)),
    );
    item.insert(
        "ttl".to_string(),
        StorageValue::N((1_900_000_000_i64 + index as i64).to_string()),
    );
    let key = fixed_key(&tenant_key);
    StreamRecordBatchItem {
        analytics_table_name: "realistic_items".to_string(),
        source_table_name: "source_items".to_string(),
        shard_id: None,
        system_position: None,
        record_key: key.clone(),
        record: StorageStreamRecord {
            keys: VecMap::from_iter([
                (
                    "pk".to_string(),
                    StorageValue::S(fixed_key_string(&tenant_key)),
                ),
                (
                    "sk".to_string(),
                    StorageValue::S(format!("item#{index:012}")),
                ),
            ]),
            sequence_number: index.to_string(),
            old_image: None,
            new_image: Some(item),
        },
    }
}

pub(super) fn fixed_key(prefix: &str) -> Vec<u8> {
    let mut key = prefix.as_bytes().to_vec();
    key.resize(100, b'k');
    key
}

pub(super) fn fixed_key_string(prefix: &str) -> String {
    String::from_utf8(fixed_key(prefix)).unwrap()
}

pub(super) fn realistic_payload(index: usize, salt: usize, length: usize) -> String {
    let mut payload = String::with_capacity(length);
    for offset in 0..length {
        payload.push((b'a' + ((index + salt + offset) % 26) as u8) as char);
    }
    payload
}

pub(super) fn realistic_query(limit: usize) -> StructuredQuery {
    StructuredQuery {
        analytics_table_name: "realistic_items".to_string(),
        table_alias: None,
        joins: Vec::new(),
        select: [
            "tenant_id",
            "gsi1_pk",
            "gsi1_sk",
            "gsi2_pk",
            "gsi2_sk",
            "payload_a",
            "payload_b",
            "payload_c",
            "status",
            "ttl",
        ]
        .into_iter()
        .map(|column_name| QuerySelect::Column {
            table_alias: None,
            column_name: column_name.to_string(),
            alias: None,
        })
        .collect(),
        filters: Vec::new(),
        group_by: Vec::new(),
        order_by: vec![QueryOrder {
            expression: QueryExpression::Column {
                table_alias: None,
                column_name: "gsi1_pk".to_string(),
            },
            direction: None,
        }],
        limit: Some(limit.try_into().unwrap()),
        offset: None,
    }
}

// literal, so the owning SQL builder no longer allocates to quote it.
