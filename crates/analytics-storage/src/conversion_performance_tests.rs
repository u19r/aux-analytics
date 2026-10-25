//! Paired probes for storage response conversion in a matching build profile.
//!
//! The workload keeps transport and routing out of the measurement. It models
//! the response-owned records that the system-stream poller receives and the
//! projected fields that a current-row page consumes.

use std::{
    cell::RefCell,
    collections::{BTreeMap, VecDeque},
    hint::black_box,
    time::{Duration, Instant},
};

use alloc_counter::AllocationGuard;
use analytics_contract::StorageStreamRecord;
use analytics_operations::{
    CheckError, CheckRowProjection, CheckSourcePosition, CurrentRowPageRequest,
    ProductionCurrentRowPageReader,
};
use storage_types::{
    AttributeMap, AttributeValue, ExclusiveStartKey, KeyAttributes, ScanResponse, StreamRecord,
    SystemStreamPosition, TableName, WireItem, WireItemKeyAttributes,
};

use crate::{
    AuxStorageCurrentRowClient, AuxStorageCurrentRowPageReader, AuxStorageCurrentRowScanRequest,
    constants::{CURRENT_ROW_PAGE_FIELDS, PAIRED_SAMPLES, RECORD_COUNT, REPETITIONS, WARMUP_RUNS},
    stream_conversion::convert_stream_record,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Probe {
    digest: u64,
    bytes: u64,
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    elapsed: Duration,
    allocations: u64,
    allocated_bytes: u64,
}

#[test]
#[ignore = "manual paired storage conversion performance probe"]
fn given_stream_and_current_rows_when_converted_then_values_match_and_allocations_drop() {
    let stream_records = stream_records();
    let (stream_baseline, stream_candidate) = measure_stream_paths(&stream_records);
    assert_eq!(stream_baseline.0, stream_candidate.0);
    println!(
        "system-stream-conversion | records={RECORD_COUNT} | records_only_bytes={} | \
         baseline={stream_baseline:?} | owned_records={stream_candidate:?}",
        stream_candidate.0.bytes
    );

    let wire_items = wire_items();
    let fields = ["id", "tenant", "source_position", "private", "payload"];
    let row = measure_row_path(&wire_items, &fields);
    let request = current_row_page_request();
    let responses = current_row_page_responses(&wire_items);
    let (row_page_baseline, row_page_candidate) =
        measure_current_row_page_paths(&request, &responses);
    println!(
        "current-row-item-map-conversion | records={RECORD_COUNT} | fields={} | \
         owned_attribute_map={row:?}",
        fields.len()
    );
    println!(
        "current-row-page-conversion | records={RECORD_COUNT} | fields={} | \
         legacy_borrowed_btreemap={row_page_baseline:?} | production_reader={row_page_candidate:?}",
        CURRENT_ROW_PAGE_FIELDS.len()
    );
    assert!(
        stream_baseline
            .1
            .iter()
            .chain(stream_candidate.1.iter())
            .all(|sample| sample.elapsed > Duration::ZERO)
    );
    assert!(row.1.iter().all(|sample| sample.elapsed > Duration::ZERO));
    assert!(
        row_page_baseline
            .1
            .iter()
            .chain(row_page_candidate.1.iter())
            .all(|sample| sample.elapsed > Duration::ZERO)
    );
    assert_eq!(row_page_baseline.0, row_page_candidate.0);
    assert!(stream_candidate.1.iter().all(|sample| {
        sample.allocations < stream_baseline.1[0].allocations
            && sample.allocated_bytes < stream_baseline.1[0].allocated_bytes
    }));
    assert!(row_page_candidate.1.iter().all(|sample| {
        sample.allocations < row_page_baseline.1[0].allocations
            && sample.allocated_bytes < row_page_baseline.1[0].allocated_bytes
    }));
}

fn measure_stream_paths(
    records: &[StreamRecord],
) -> (
    (Probe, [Sample; PAIRED_SAMPLES]),
    (Probe, [Sample; PAIRED_SAMPLES]),
) {
    let mut warmup_serialization_buffer = Vec::new();
    for _ in 0..WARMUP_RUNS {
        let baseline = stream_baseline(records);
        let candidate = stream_owned(
            &mut owned_batches(records),
            &mut warmup_serialization_buffer,
        );
        assert_eq!(baseline, candidate);
    }

    let mut baseline_samples = [Sample::default(); PAIRED_SAMPLES];
    let mut candidate_samples = [Sample::default(); PAIRED_SAMPLES];
    let mut baseline_probe = Probe {
        digest: 0,
        bytes: 0,
    };
    let mut candidate_probe = baseline_probe;
    for index in 0..PAIRED_SAMPLES {
        let before = measure_stream_baseline(records);
        let after = measure_stream_owned(records);
        baseline_probe = before.0;
        candidate_probe = after.0;
        baseline_samples[index] = before.1;
        candidate_samples[index] = after.1;
    }
    (
        (baseline_probe, baseline_samples),
        (candidate_probe, candidate_samples),
    )
}

fn measure_stream_baseline(records: &[StreamRecord]) -> (Probe, Sample) {
    let started = Instant::now();
    let guard = AllocationGuard::start(
        module_path!(),
        "system-stream-conversion",
        file!(),
        line!(),
        Some("baseline"),
    );
    let mut observed = Probe {
        digest: 0,
        bytes: 0,
    };
    for _ in 0..REPETITIONS {
        observed = black_box(stream_baseline(records));
    }
    let elapsed = started.elapsed();
    let report = guard.finish();
    alloc_counter::emit_report(&report).expect("stream baseline allocation report");
    (
        observed,
        Sample {
            elapsed,
            allocations: report.allocation_count,
            allocated_bytes: report.allocated_bytes,
        },
    )
}

fn measure_stream_owned(records: &[StreamRecord]) -> (Probe, Sample) {
    let mut serialization_buffer = Vec::new();
    serde_json::to_writer(&mut serialization_buffer, records)
        .expect("prewarm source records serialization");
    let mut batches = owned_batches(records);
    let started = Instant::now();
    let guard = AllocationGuard::start(
        module_path!(),
        "system-stream-conversion",
        file!(),
        line!(),
        Some("owned-response"),
    );
    let mut observed = Probe {
        digest: 0,
        bytes: 0,
    };
    for _ in 0..REPETITIONS {
        observed = black_box(stream_owned(&mut batches, &mut serialization_buffer));
    }
    let elapsed = started.elapsed();
    let report = guard.finish();
    assert!(
        batches.is_empty(),
        "candidate must consume every paired batch"
    );
    alloc_counter::emit_report(&report).expect("stream candidate allocation report");
    (
        observed,
        Sample {
            elapsed,
            allocations: report.allocation_count,
            allocated_bytes: report.allocated_bytes,
        },
    )
}

fn stream_baseline(records: &[StreamRecord]) -> Probe {
    let encoded_bytes = serde_json::to_vec(records)
        .expect("serialize source records")
        .len();
    let mut digest = 1469598103934665603_u64;
    for source_record in records.iter().cloned() {
        digest_record(&mut digest, convert_stream_record(source_record));
    }
    Probe {
        digest,
        bytes: encoded_bytes as u64,
    }
}

fn stream_owned(
    batches: &mut VecDeque<Vec<StreamRecord>>,
    serialization_buffer: &mut Vec<u8>,
) -> Probe {
    let records = batches.pop_front().expect("owned response batch");
    serialization_buffer.clear();
    serde_json::to_writer(&mut *serialization_buffer, records.as_slice())
        .expect("serialize source records with a reusable buffer");
    let encoded_bytes = serialization_buffer.len();
    let mut digest = 1469598103934665603_u64;
    for source_record in records {
        digest_record(&mut digest, convert_stream_record(source_record));
    }
    Probe {
        digest,
        bytes: encoded_bytes as u64,
    }
}

fn measure_row_path(items: &[WireItem], fields: &[&str]) -> (Probe, [Sample; PAIRED_SAMPLES]) {
    for _ in 0..WARMUP_RUNS {
        black_box(row_canonical(&mut owned_item_batches(items), fields));
    }

    let mut samples = [Sample::default(); PAIRED_SAMPLES];
    let mut probe = Probe {
        digest: 0,
        bytes: 0,
    };
    for sample in &mut samples {
        let observed = measure_row_canonical(items, fields);
        probe = observed.0;
        *sample = observed.1;
    }
    (probe, samples)
}

fn measure_row_canonical(items: &[WireItem], fields: &[&str]) -> (Probe, Sample) {
    let mut batches = owned_item_batches(items);
    let started = Instant::now();
    let guard = AllocationGuard::start(
        module_path!(),
        "current-row-item-map-conversion",
        file!(),
        line!(),
        Some("owned-attribute-map"),
    );
    let mut observed = Probe {
        digest: 0,
        bytes: 0,
    };
    for _ in 0..REPETITIONS {
        observed = black_box(row_canonical(&mut batches, fields));
    }
    let elapsed = started.elapsed();
    let report = guard.finish();
    alloc_counter::emit_report(&report).expect("row canonical allocation report");
    (
        observed,
        Sample {
            elapsed,
            allocations: report.allocation_count,
            allocated_bytes: report.allocated_bytes,
        },
    )
}

fn row_canonical(batches: &mut VecDeque<Vec<WireItem>>, fields: &[&str]) -> Probe {
    let items = batches.pop_front().expect("wire item batch");
    let mut digest = 1469598103934665603_u64;
    let mut bytes = 0_u64;
    for item in items {
        let attributes = item.into_attribute_map().expect("attribute map");
        digest_attributes(&mut digest, &mut bytes, &attributes, fields);
    }
    Probe { digest, bytes }
}

fn measure_current_row_page_paths(
    request: &CurrentRowPageRequest,
    responses: &[ScanResponse],
) -> (
    (Probe, [Sample; PAIRED_SAMPLES]),
    (Probe, [Sample; PAIRED_SAMPLES]),
) {
    assert_current_row_page_parity(request, responses);
    for _ in 0..WARMUP_RUNS {
        let mut baseline_batches = owned_response_batches(responses);
        let baseline = legacy_current_row_page_probe(request, &mut baseline_batches);
        let candidate = serialized_probe(&production_current_row_page(request, responses));
        assert_eq!(baseline, candidate);
    }

    let mut baseline_samples = [Sample::default(); PAIRED_SAMPLES];
    let mut candidate_samples = [Sample::default(); PAIRED_SAMPLES];
    let mut baseline_probe = Probe {
        digest: 0,
        bytes: 0,
    };
    let mut candidate_probe = baseline_probe;
    for index in 0..PAIRED_SAMPLES {
        let baseline = measure_legacy_current_row_page(request, responses);
        let candidate = measure_production_current_row_page(request, responses);
        baseline_probe = baseline.0;
        candidate_probe = candidate.0;
        baseline_samples[index] = baseline.1;
        candidate_samples[index] = candidate.1;
    }
    (
        (baseline_probe, baseline_samples),
        (candidate_probe, candidate_samples),
    )
}

fn measure_legacy_current_row_page(
    request: &CurrentRowPageRequest,
    responses: &[ScanResponse],
) -> (Probe, Sample) {
    let mut batches = owned_response_batches(responses);
    let started = Instant::now();
    let guard = AllocationGuard::start(
        module_path!(),
        "current-row-page-conversion",
        file!(),
        line!(),
        Some("legacy-borrowed-btreemap"),
    );
    let mut observed = Probe {
        digest: 0,
        bytes: 0,
    };
    for _ in 0..REPETITIONS {
        observed = black_box(legacy_current_row_page_probe(request, &mut batches));
    }
    let elapsed = started.elapsed();
    let report = guard.finish();
    assert!(
        batches.is_empty(),
        "legacy current-row page must consume every batch"
    );
    alloc_counter::emit_report(&report).expect("legacy current-row page allocation report");
    (
        observed,
        Sample {
            elapsed,
            allocations: report.allocation_count,
            allocated_bytes: report.allocated_bytes,
        },
    )
}

fn measure_production_current_row_page(
    request: &CurrentRowPageRequest,
    responses: &[ScanResponse],
) -> (Probe, Sample) {
    let client = OwnedCurrentRowClient::new(owned_response_batches(responses));
    let reader = AuxStorageCurrentRowPageReader::new(&client);
    let started = Instant::now();
    let guard = AllocationGuard::start(
        module_path!(),
        "current-row-page-conversion",
        file!(),
        line!(),
        Some("production-reader"),
    );
    let mut observed = Probe {
        digest: 0,
        bytes: 0,
    };
    for _ in 0..REPETITIONS {
        observed = black_box(production_current_row_page_probe_from_reader(
            &reader, request,
        ));
    }
    let elapsed = started.elapsed();
    let report = guard.finish();
    assert!(
        client.is_empty(),
        "production current-row page must consume every batch"
    );
    alloc_counter::emit_report(&report).expect("production current-row page allocation report");
    (
        observed,
        Sample {
            elapsed,
            allocations: report.allocation_count,
            allocated_bytes: report.allocated_bytes,
        },
    )
}

fn legacy_current_row_page_probe(
    request: &CurrentRowPageRequest,
    responses: &mut VecDeque<ScanResponse>,
) -> Probe {
    let response = responses
        .pop_front()
        .expect("legacy current-row response batch");
    black_box(legacy_aux_storage_scan_request(request));
    let page = legacy_projected_page(request, response);
    serialized_probe(&page)
}

fn production_current_row_page_probe_from_reader<Client>(
    reader: &AuxStorageCurrentRowPageReader<Client>,
    request: &CurrentRowPageRequest,
) -> Probe
where
    Client: AuxStorageCurrentRowClient,
{
    let page = reader
        .current_row_page(request)
        .expect("production current-row page");
    serialized_probe(&page)
}

#[derive(Debug)]
struct OwnedCurrentRowClient {
    responses: RefCell<VecDeque<ScanResponse>>,
}

impl OwnedCurrentRowClient {
    fn new(responses: VecDeque<ScanResponse>) -> Self {
        Self {
            responses: RefCell::new(responses),
        }
    }

    fn is_empty(&self) -> bool {
        self.responses.borrow().is_empty()
    }
}

impl AuxStorageCurrentRowClient for OwnedCurrentRowClient {
    fn scan_current_rows(
        &self,
        request: &AuxStorageCurrentRowScanRequest,
    ) -> Result<ScanResponse, CheckError> {
        black_box(request);
        self.responses.borrow_mut().pop_front().ok_or_else(|| {
            CheckError::InvalidBackendConfiguration(
                "current-row conversion probe exhausted response batches".to_string(),
            )
        })
    }
}

impl AuxStorageCurrentRowClient for &OwnedCurrentRowClient {
    fn scan_current_rows(
        &self,
        request: &AuxStorageCurrentRowScanRequest,
    ) -> Result<ScanResponse, CheckError> {
        (*self).scan_current_rows(request)
    }
}

#[derive(Debug, serde::Serialize)]
struct LegacyProjectedCurrentRow {
    key: String,
    // This map intentionally models the pre-migration current-row boundary.
    values: BTreeMap<String, Option<String>>,
    source_position: CheckSourcePosition,
    contains_private_data: bool,
}

#[derive(Debug, serde::Serialize)]
struct LegacyProjectedCurrentRowPage {
    rows: Vec<LegacyProjectedCurrentRow>,
    next_cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    partition: Option<String>,
}

fn current_row_page_request() -> CurrentRowPageRequest {
    CurrentRowPageRequest {
        projection: CheckRowProjection::try_new_with_optional_columns(
            "users",
            "id",
            vec!["tenant".to_string(), "payload".to_string()],
            Some("source_position".to_string()),
            Some("private".to_string()),
        )
        .expect("current-row probe projection"),
        cursor: Some(r#"{"z_cursor":{"S":"start-z"},"id":{"S":"start-page"}}"#.to_string()),
        page_size: RECORD_COUNT,
    }
}

fn current_row_page_responses(items: &[WireItem]) -> Vec<ScanResponse> {
    let count = u32::try_from(items.len()).expect("current-row probe record count");
    let response = ScanResponse {
        items: Some(items.to_vec()),
        count,
        scanned_count: count,
        last_evaluated_key: Some(KeyAttributes::from([
            (
                "z_cursor".to_string(),
                AttributeValue::S("next-z".to_string()),
            ),
            ("id".to_string(), AttributeValue::S("next-page".to_string())),
        ])),
        consumed_capacity: None,
    };
    (0..REPETITIONS).map(|_| response.clone()).collect()
}

fn assert_current_row_page_parity(request: &CurrentRowPageRequest, responses: &[ScanResponse]) {
    let baseline_response = responses
        .first()
        .cloned()
        .expect("current-row probe response");
    let baseline = legacy_projected_page(request, baseline_response);
    let candidate = production_current_row_page(request, &responses[..1]);
    assert_eq!(baseline.next_cursor, candidate.next_cursor);
    assert_eq!(baseline.partition, candidate.partition);
    assert_eq!(baseline.rows.len(), candidate.rows.len());
    for (baseline_row, candidate_row) in baseline.rows.iter().zip(&candidate.rows) {
        assert_eq!(baseline_row.key, candidate_row.key);
        assert_eq!(baseline_row.source_position, candidate_row.source_position);
        assert_eq!(
            baseline_row.contains_private_data,
            candidate_row.contains_private_data
        );
        assert_eq!(baseline_row.values.len(), candidate_row.values.len());
        for (column, value) in &baseline_row.values {
            assert_eq!(candidate_row.values.get(column), Some(value));
        }
    }
    let baseline_bytes = serde_json::to_vec(&baseline).expect("legacy current-row page bytes");
    let candidate_bytes =
        serde_json::to_vec(&candidate).expect("production current-row page bytes");
    assert_eq!(baseline_bytes, candidate_bytes);
    assert_eq!(serialized_probe(&baseline), serialized_probe(&candidate));
}

fn production_current_row_page(
    request: &CurrentRowPageRequest,
    responses: &[ScanResponse],
) -> analytics_operations::ProjectedCurrentRowPage {
    let client = OwnedCurrentRowClient::new(owned_response_batches(responses));
    let reader = AuxStorageCurrentRowPageReader::new(&client);
    reader
        .current_row_page(request)
        .expect("production current-row page")
}

fn legacy_projected_page(
    request: &CurrentRowPageRequest,
    response: ScanResponse,
) -> LegacyProjectedCurrentRowPage {
    let ScanResponse {
        items,
        last_evaluated_key,
        ..
    } = response;
    let rows = items
        .unwrap_or_default()
        .iter()
        .map(|item| legacy_projected_row(request, item))
        .collect();
    let next_cursor = last_evaluated_key.as_ref().map(|key| {
        key.canonical_dynamo_json()
            .expect("legacy current-row cursor")
    });
    LegacyProjectedCurrentRowPage {
        rows,
        next_cursor,
        partition: None,
    }
}

fn legacy_projected_row(
    request: &CurrentRowPageRequest,
    item: &WireItem,
) -> LegacyProjectedCurrentRow {
    let attributes = item
        .to_attribute_map()
        .expect("legacy borrowed current-row item");
    let key = legacy_required_attribute(&attributes, &request.projection.key_column);
    let values = request
        .projection
        .hash_columns
        .iter()
        .map(|column| {
            (
                column.clone(),
                Some(legacy_required_attribute(&attributes, column)),
            )
        })
        .collect();
    let source_position = request
        .projection
        .source_position_column
        .as_deref()
        .and_then(|column| attributes.get(column))
        .map(legacy_attribute_to_string)
        .map(|value| {
            CheckSourcePosition::parse_source_position(&value).expect("fixture source position")
        })
        .unwrap_or_default();
    let contains_private_data = request
        .projection
        .private_data_column
        .as_deref()
        .and_then(|column| attributes.get(column))
        .map(legacy_bool_attribute)
        .unwrap_or(false);
    LegacyProjectedCurrentRow {
        key,
        values,
        source_position,
        contains_private_data,
    }
}

fn legacy_required_attribute(item: &AttributeMap, column: &str) -> String {
    item.get(column)
        .map(legacy_attribute_to_string)
        .unwrap_or_else(|| panic!("legacy current-row item missing {column}"))
}

fn legacy_attribute_to_string(value: &AttributeValue) -> String {
    match value {
        AttributeValue::S(value) | AttributeValue::N(value) | AttributeValue::B(value) => {
            value.clone()
        }
        AttributeValue::BOOL(value) => value.to_string(),
        AttributeValue::NULL(true) => String::new(),
        other => panic!("legacy current-row scalar variant {other:?}"),
    }
}

fn legacy_bool_attribute(value: &AttributeValue) -> bool {
    match value {
        AttributeValue::BOOL(value) => *value,
        AttributeValue::S(value) if value == "true" => true,
        AttributeValue::S(value) if value == "false" => false,
        other => panic!("legacy current-row private-data variant {other:?}"),
    }
}

fn legacy_aux_storage_scan_request(
    request: &CurrentRowPageRequest,
) -> AuxStorageCurrentRowScanRequest {
    let mut columns = vec![request.projection.key_column.clone()];
    columns.extend(request.projection.hash_columns.iter().cloned());
    if let Some(column) = request.projection.source_position_column.as_ref() {
        columns.push(column.clone());
    }
    if let Some(column) = request.projection.private_data_column.as_ref() {
        columns.push(column.clone());
    }
    columns.sort_unstable();
    columns.dedup();
    let expression_attribute_names: storage_types::VecMap<_, _> = columns
        .into_iter()
        .enumerate()
        .map(|(index, column)| (format!("#c{index}"), column))
        .collect();
    AuxStorageCurrentRowScanRequest {
        table_name: TableName::new(&request.projection.table_name).to_string(),
        projection_expression: expression_attribute_names
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join(", "),
        expression_attribute_names,
        limit: u32::try_from(request.page_size).expect("legacy current-row page size"),
        exclusive_start_key: request.cursor.as_deref().map(|cursor| {
            serde_json::from_str::<KeyAttributes>(cursor)
                .map(ExclusiveStartKey::Key)
                .unwrap_or_else(|_| ExclusiveStartKey::Token(cursor.to_string()))
        }),
    }
}

fn owned_response_batches(responses: &[ScanResponse]) -> VecDeque<ScanResponse> {
    responses.iter().cloned().collect()
}

fn serialized_probe<T: serde::Serialize>(value: &T) -> Probe {
    let encoded = serde_json::to_vec(value).expect("serialized current-row page");
    let mut digest = 1469598103934665603_u64;
    digest_bytes(&mut digest, &encoded);
    Probe {
        digest,
        bytes: encoded.len() as u64,
    }
}

fn digest_record(digest: &mut u64, record: StorageStreamRecord) {
    digest_bytes(digest, record.sequence_number.as_bytes());
    for field in ["id", "tenant", "source_position", "private", "payload"] {
        if let Some(value) = record.new_image.as_ref().and_then(|item| item.get(field)) {
            digest_serialized(digest, value);
        }
    }
}

fn digest_attributes(digest: &mut u64, bytes: &mut u64, item: &AttributeMap, fields: &[&str]) {
    for field in fields {
        if let Some(value) = item.get(field) {
            digest_value(digest, bytes, value);
        }
    }
}

fn digest_value(digest: &mut u64, bytes: &mut u64, value: &AttributeValue) {
    let encoded = serde_json::to_vec(value).expect("attribute value");
    digest_bytes(digest, &encoded);
    *bytes = bytes.saturating_add(encoded.len() as u64);
}

fn digest_serialized<T: serde::Serialize>(digest: &mut u64, value: &T) {
    let encoded = serde_json::to_vec(value).expect("serialized value");
    digest_bytes(digest, &encoded);
}

fn digest_bytes(digest: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *digest = digest
            .wrapping_mul(1099511628211)
            .wrapping_add(u64::from(*byte));
    }
}

fn owned_batches(records: &[StreamRecord]) -> VecDeque<Vec<StreamRecord>> {
    (0..REPETITIONS)
        .map(|_| records.to_vec())
        .collect::<VecDeque<_>>()
}

fn owned_item_batches(items: &[WireItem]) -> VecDeque<Vec<WireItem>> {
    (0..REPETITIONS)
        .map(|_| items.to_vec())
        .collect::<VecDeque<_>>()
}

fn stream_records() -> Vec<StreamRecord> {
    (0..RECORD_COUNT)
        .map(|index| StreamRecord {
            cursor: None,
            source_table_name: Some(TableName::new("source_users")),
            keys: KeyAttributes::from([(
                "id".to_string(),
                AttributeValue::S(format!("user-{index}")),
            )]),
            sequence_number: format!("sequence-{index}"),
            old_image: None,
            new_image: Some(AttributeMap::from_iter([
                ("id".to_string(), AttributeValue::S(format!("user-{index}"))),
                (
                    "tenant".to_string(),
                    AttributeValue::S(format!("tenant-{}", index % 8)),
                ),
                (
                    "source_position".to_string(),
                    AttributeValue::S(
                        serde_json::to_string(
                            &analytics_contract::SourcePosition::RawBackupObjectOffset {
                                object_id: "check-fixture".to_string(),
                                offset: index as u64,
                            },
                        )
                        .unwrap(),
                    ),
                ),
                ("private".to_string(), AttributeValue::BOOL(index % 2 == 0)),
                ("payload".to_string(), AttributeValue::S("x".repeat(1_200))),
            ])),
            system_position: Some(
                SystemStreamPosition::try_new(
                    format!("shard-{}", index % 4),
                    format!("lineage-{}", index % 3),
                    format!("offset-{index}"),
                )
                .expect("system position"),
            ),
        })
        .collect()
}

fn wire_items() -> Vec<WireItem> {
    stream_records()
        .into_iter()
        .enumerate()
        .map(|(index, record)| {
            let mut image = record.new_image.expect("new image");
            for index in 0..16 {
                image.insert(
                    format!("unselected_{index}"),
                    AttributeValue::S("y".repeat(300)),
                );
            }
            if index % 2 == 0 {
                WireItem::try_from(image).expect("wire item")
            } else {
                let key = image.remove("id").expect("wire item key");
                let blob = serde_json::to_vec(&image).expect("local split attributes");
                WireItem::local_split(
                    WireItemKeyAttributes::new("id".to_string(), key, None, None),
                    None,
                    Some(blob),
                )
            }
        })
        .collect()
}

impl Default for Sample {
    fn default() -> Self {
        Self {
            elapsed: Duration::ZERO,
            allocations: 0,
            allocated_bytes: 0,
        }
    }
}
