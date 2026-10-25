//! Equal decoded SDK responses through conversion, byte accounting and fan-out.
//! Transport, SDK JSON decoding and ingestion are outside this measurement.

use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    hint::black_box,
    time::Instant,
};

use alloc_counter::AllocationGuard;
use analytics_contract::{StorageItem, StorageStreamRecord, StorageValue};
use aws_sdk_dynamodbstreams::{
    operation::get_records::GetRecordsOutput,
    primitives::Blob,
    types::{AttributeValue as AwsStreamAttributeValue, Record, StreamRecord},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use config::{AnalyticsSourceConfig, RemoteCredentialsConfig, RemoteStaticCredentialsConfig};

use super::state::{
    AwsDiscoverySchedule, AwsStreamResponseBatchInput, AwsStreamTablePoller,
    aws_stream_response_batch,
};
use crate::{
    aws_stream::AwsStreams,
    constants::{PAIRED_SAMPLES, REPETITIONS, WARMUP_RUNS},
    error::{AnalyticsStorageError, AnalyticsStorageErrorKind, AnalyticsStorageResult},
    source_batch::{PollBatch, SourceCheckpoint},
};

struct AwsResponseWorkload {
    record_count: usize,
    value_bytes: usize,
    nesting_depth: usize,
    destinations: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
enum ConversionPath {
    Borrowed,
    Consumed,
}

impl ConversionPath {
    fn convert_response_to_batch(
        self,
        poller: &mut AwsStreamTablePoller,
        response: GetRecordsOutput,
    ) -> PollBatch {
        match self {
            Self::Consumed => poller.convert_response("shard-a", response).unwrap(),
            Self::Borrowed => {
                // Former converter borrows individual values; cloning an entire
                // SDK Record would invent extra map and metadata copies that
                // never existed.
                let records = response
                    .records()
                    .iter()
                    .map(borrowed_record_to_contract)
                    .collect::<AnalyticsStorageResult<Vec<_>>>()
                    .unwrap();
                aws_stream_response_batch(
                    AwsStreamResponseBatchInput {
                        source_table_name: &poller.source_table_name,
                        analytics_table_names: &poller.analytics_table_names,
                        shard_id: "shard-a",
                        source_predecessor: poller.durable_checkpoints.first().cloned(),
                        contract_records: records,
                        next_iterator: response.next_shard_iterator,
                    },
                    &mut poller.serialization_buffer,
                )
                .unwrap()
            }
        }
    }
}

impl AwsResponseWorkload {
    async fn create_poller(&self) -> AwsStreamTablePoller {
        let source = AnalyticsSourceConfig {
            endpoint_url: Some("http://127.0.0.1:1".to_string()),
            region: Some("us-east-1".to_string()),
            credentials: Some(RemoteCredentialsConfig {
                r#static: Some(RemoteStaticCredentialsConfig {
                    access_key: "test-access".to_string(),
                    secret_key: "test-secret".to_string(),
                    session_token: None,
                }),
                instance_keys: None,
            }),
            ..AnalyticsSourceConfig::default()
        };
        AwsStreamTablePoller {
            client: AwsStreams::from_source(&source).await,
            stream_arn: "test-stream".to_string(),
            source_table_name: "source_events".to_string(),
            analytics_table_names: self.destinations.clone(),
            max_shards: 1,
            max_records_per_response: 1000,
            durable_checkpoints: vec![Self::durable_predecessor()],
            shard_iterators: Vec::new(),
            pending_batches: VecDeque::new(),
            pending_shard_ids: VecDeque::new(),
            pending_expired_shard_ids: VecDeque::new(),
            next_exclusive_start_shard_id: None,
            startup_exhausted: true,
            pending_terminal_shards: BTreeSet::new(),
            next_shard_id: None,
            discovery_schedule: AwsDiscoverySchedule::Quiet,
            serialization_buffer: Vec::new(),
        }
    }

    fn durable_predecessor() -> SourceCheckpoint {
        SourceCheckpoint {
            source_table_name: "source_events".to_string(),
            shard_id: "shard-a".to_string(),
            position: "before".to_string(),
        }
    }

    fn sdk_response(&self, iterator: Option<&str>) -> GetRecordsOutput {
        let records = (0..self.record_count)
            .map(|index| {
                Record::builder()
                    .event_id("unused-event-metadata")
                    .dynamodb(
                        StreamRecord::builder()
                            .sequence_number(index.to_string())
                            .size_bytes(17)
                            .set_keys(Some(HashMap::from([(
                                "id".to_string(),
                                AwsStreamAttributeValue::S(index.to_string()),
                            )])))
                            .set_old_image(Some(HashMap::new()))
                            .set_new_image(Some(self.sdk_image()))
                            .build(),
                    )
                    .build()
            })
            .collect();
        GetRecordsOutput::builder()
            .set_records(Some(records))
            .set_next_shard_iterator(iterator.map(str::to_owned))
            .build()
    }

    fn sdk_image(&self) -> HashMap<String, AwsStreamAttributeValue> {
        let mut nested = AwsStreamAttributeValue::L(vec![
            AwsStreamAttributeValue::S("x".repeat(self.value_bytes)),
            AwsStreamAttributeValue::N("123.456".to_string()),
            AwsStreamAttributeValue::Bool(true),
            AwsStreamAttributeValue::Null(true),
        ]);
        for _ in 0..self.nesting_depth {
            nested = AwsStreamAttributeValue::M(HashMap::from([("child".to_string(), nested)]));
        }
        HashMap::from([
            ("nested".to_string(), nested),
            (
                "strings".to_string(),
                AwsStreamAttributeValue::Ss(vec!["a".repeat(self.value_bytes), "b".to_string()]),
            ),
            (
                "numbers".to_string(),
                AwsStreamAttributeValue::Ns(vec!["1".to_string(), "2".to_string()]),
            ),
            (
                "binary".to_string(),
                AwsStreamAttributeValue::B(Blob::new("abc")),
            ),
            (
                "binaries".to_string(),
                AwsStreamAttributeValue::Bs(vec![Blob::new("a"), Blob::new("b")]),
            ),
        ])
    }

    fn expected_records(&self) -> Vec<StorageStreamRecord> {
        let mut nested = StorageValue::L(vec![
            StorageValue::S("x".repeat(self.value_bytes)),
            StorageValue::N("123.456".to_string()),
            StorageValue::BOOL(true),
            StorageValue::NULL(true),
        ]);
        for _ in 0..self.nesting_depth {
            nested = StorageValue::M(StorageItem::from_iter([("child".to_string(), nested)]));
        }
        let image = StorageItem::from_iter([
            ("nested".to_string(), nested),
            (
                "strings".to_string(),
                StorageValue::SS(vec!["a".repeat(self.value_bytes), "b".to_string()]),
            ),
            (
                "numbers".to_string(),
                StorageValue::NS(vec!["1".to_string(), "2".to_string()]),
            ),
            ("binary".to_string(), StorageValue::B("YWJj".to_string())),
            (
                "binaries".to_string(),
                StorageValue::BS(vec!["YQ==".to_string(), "Yg==".to_string()]),
            ),
        ]);
        (0..self.record_count)
            .map(|index| StorageStreamRecord {
                sequence_number: index.to_string(),
                keys: StorageItem::from_iter([(
                    "id".to_string(),
                    StorageValue::S(index.to_string()),
                )]),
                old_image: None,
                new_image: Some(image.clone()),
            })
            .collect()
    }

    fn assert_complete_batch(&self, batch: &PollBatch, iterator: Option<&str>) {
        let expected = self.expected_records();
        assert_eq!(batch.source_response_count, 1);
        assert_eq!(
            batch.source_nonempty_response_count,
            usize::from(self.record_count > 0)
        );
        assert_eq!(batch.source_record_count, self.record_count);
        assert_eq!(
            batch.source_encoded_bytes,
            serde_json::to_vec(&expected).unwrap().len()
        );
        assert_eq!(batch.source_predecessor, Some(Self::durable_predecessor()));
        assert!(batch.system_stream.is_none());
        assert_eq!(batch.records.len(), self.record_count);
        for (record, source) in batch.records.iter().zip(&expected) {
            assert_eq!(record.source_table_name, "source_events");
            assert_eq!(record.shard_id, "shard-a");
            assert_eq!(record.analytics_table_names, self.destinations);
            assert_eq!(record.record_key, source.sequence_number);
            assert_eq!(&record.record, source);
            assert!(record.system_position.is_none());
        }
        let mut checkpoints = Vec::new();
        if self.record_count > 0 {
            checkpoints.push(SourceCheckpoint {
                position: (self.record_count - 1).to_string(),
                ..Self::durable_predecessor()
            });
        }
        if let Some(iterator) = iterator {
            checkpoints.push(SourceCheckpoint {
                shard_id: "__iterator:shard-a".to_string(),
                position: iterator.to_string(),
                ..Self::durable_predecessor()
            });
        }
        assert_eq!(batch.checkpoints, checkpoints);
    }

    fn measure_conversion(
        &self,
        path: ConversionPath,
        poller: &mut AwsStreamTablePoller,
    ) -> (u64, u64) {
        // Both inputs and result-vector capacity are prepared outside
        // measurement. Both paths drop their input SDK response before
        // returning a batch.
        let inputs = (0..REPETITIONS)
            .map(|_| self.sdk_response(Some("next")))
            .collect::<Vec<_>>();
        let mut batches = Vec::with_capacity(REPETITIONS);
        let guard = AllocationGuard::start(
            module_path!(),
            "aws-response-to-batch",
            file!(),
            line!(),
            None,
        );
        let started = Instant::now();
        for response in inputs {
            batches.push(black_box(path.convert_response_to_batch(poller, response)));
        }
        let elapsed = started.elapsed();
        let report = guard.finish();
        for batch in &batches {
            self.assert_complete_batch(batch, Some("next"));
        }
        println!(
            "aws-response-to-batch | path={path:?} | records={} | width={} | depth={} | \
             destinations={} | repetitions={REPETITIONS} | elapsed_ns={} | allocations={} | \
             allocated_bytes={}",
            self.record_count,
            self.value_bytes,
            self.nesting_depth,
            self.destinations.len(),
            elapsed.as_nanos(),
            report.allocation_count,
            report.allocated_bytes
        );
        (report.allocation_count, report.allocated_bytes)
    }
}

#[tokio::test]
async fn given_nested_sdk_responses_when_consumed_then_records_and_resume_metadata_are_preserved() {
    for record_count in [0, 2] {
        let workload = AwsResponseWorkload {
            record_count,
            value_bytes: 7,
            nesting_depth: 2,
            destinations: vec!["events".to_string(), "archive".to_string()],
        };
        let mut poller = workload.create_poller().await;
        for iterator in [None, Some("next")] {
            let batch = poller
                .convert_response("shard-a", workload.sdk_response(iterator))
                .unwrap();
            workload.assert_complete_batch(&batch, iterator);
            assert_eq!(
                poller.durable_checkpoints,
                [AwsResponseWorkload::durable_predecessor()]
            );
        }
    }
}

#[tokio::test]
async fn given_valid_then_malformed_sdk_records_when_converted_then_no_batch_or_checkpoint_is_published()
 {
    let workload = AwsResponseWorkload {
        record_count: 1,
        value_bytes: 7,
        nesting_depth: 2,
        destinations: vec!["events".to_string()],
    };
    let mut poller = workload.create_poller().await;
    let mut response = workload.sdk_response(Some("next"));
    response.records.as_mut().unwrap().push(
        Record::builder()
            .dynamodb(StreamRecord::builder().sequence_number("invalid").build())
            .build(),
    );
    assert_eq!(
        poller
            .convert_response("shard-a", response)
            .unwrap_err()
            .to_string(),
        "aws stream record did not include keys"
    );
    assert_eq!(
        poller.durable_checkpoints,
        [AwsResponseWorkload::durable_predecessor()]
    );
    assert!(poller.pending_batches.is_empty());
    assert!(poller.pending_terminal_shards.is_empty());
}

#[tokio::test]
#[ignore = "manual equal-work AWS response-to-batch allocation and timing probe"]
async fn given_equal_sdk_responses_when_moved_instead_of_copied_then_complete_batches_preserve_values_with_fewer_allocations()
 {
    for record_count in [0, 1, 32] {
        for value_bytes in [16, 1024] {
            for nesting_depth in [0, 3] {
                for destination_count in [1, 4, 16] {
                    let workload = AwsResponseWorkload {
                        record_count,
                        value_bytes,
                        nesting_depth,
                        destinations: (0..destination_count)
                            .map(|index| format!("events_{index}"))
                            .collect(),
                    };
                    let mut baseline = workload.create_poller().await;
                    let mut candidate = workload.create_poller().await;
                    for _ in 0..WARMUP_RUNS {
                        for (path, poller) in [
                            (ConversionPath::Borrowed, &mut baseline),
                            (ConversionPath::Consumed, &mut candidate),
                        ] {
                            workload.assert_complete_batch(
                                &path.convert_response_to_batch(
                                    poller,
                                    workload.sdk_response(Some("next")),
                                ),
                                Some("next"),
                            );
                        }
                    }
                    for pair in 0..PAIRED_SAMPLES {
                        let (before, after) = if pair % 2 == 0 {
                            (
                                workload
                                    .measure_conversion(ConversionPath::Borrowed, &mut baseline),
                                workload
                                    .measure_conversion(ConversionPath::Consumed, &mut candidate),
                            )
                        } else {
                            let after = workload
                                .measure_conversion(ConversionPath::Consumed, &mut candidate);
                            (
                                workload
                                    .measure_conversion(ConversionPath::Borrowed, &mut baseline),
                                after,
                            )
                        };
                        if record_count == 0 {
                            assert_eq!(before, after);
                        } else {
                            assert!(after.0 < before.0);
                            assert!(after.1 < before.1);
                        }
                    }
                }
            }
        }
    }
}

#[tokio::test]
#[ignore = "manual separate-process AWS conversion memory probe; requires explicit path"]
async fn given_wide_sdk_responses_when_converted_in_one_process_then_full_batches_remain_owned() {
    let path = match std::env::var("AUX_ANALYTICS_AWS_CONVERSION_PROBE").as_deref() {
        Ok("borrowed") => ConversionPath::Borrowed,
        Ok("consumed") => ConversionPath::Consumed,
        other => panic!("explicit borrowed or consumed AWS conversion probe required: {other:?}"),
    };
    let workload = AwsResponseWorkload {
        record_count: 256,
        value_bytes: 8192,
        nesting_depth: 3,
        destinations: vec!["events".to_string()],
    };
    let mut poller = workload.create_poller().await;
    for _ in 0..WARMUP_RUNS {
        workload.assert_complete_batch(
            &path.convert_response_to_batch(&mut poller, workload.sdk_response(Some("next"))),
            Some("next"),
        );
    }
    workload.measure_conversion(path, &mut poller);
}

// Exact former borrowed converter, isolated from the one production conversion
// path.
fn borrowed_record_to_contract(
    record: &aws_sdk_dynamodbstreams::types::Record,
) -> AnalyticsStorageResult<StorageStreamRecord> {
    let Some(storage_record) = record.dynamodb() else {
        return Err(AnalyticsStorageError::new(
            AnalyticsStorageErrorKind::MissingSequenceNumber,
        ));
    };
    let sequence_number = storage_record
        .sequence_number()
        .ok_or_else(|| {
            AnalyticsStorageError::new(AnalyticsStorageErrorKind::MissingSequenceNumber)
        })?
        .to_string();
    Ok(StorageStreamRecord {
        keys: borrowed_item_to_contract(
            storage_record.keys().ok_or_else(|| {
                AnalyticsStorageError::new(AnalyticsStorageErrorKind::MissingKeys)
            })?,
        )?,
        sequence_number,
        old_image: optional_borrowed_item_to_contract(storage_record.old_image())?,
        new_image: optional_borrowed_item_to_contract(storage_record.new_image())?,
    })
}

fn optional_borrowed_item_to_contract(
    item: Option<&HashMap<String, AwsStreamAttributeValue>>,
) -> AnalyticsStorageResult<Option<StorageItem>> {
    let Some(item) = item else {
        return Ok(None);
    };
    let item = borrowed_item_to_contract(item)?;
    Ok((!item.is_empty()).then_some(item))
}

fn borrowed_item_to_contract(
    item: &HashMap<String, AwsStreamAttributeValue>,
) -> AnalyticsStorageResult<StorageItem> {
    item.iter()
        .map(|(name, value)| Ok((name.clone(), borrowed_value_to_contract(value)?)))
        .collect()
}

fn borrowed_value_to_contract(
    value: &AwsStreamAttributeValue,
) -> AnalyticsStorageResult<StorageValue> {
    Ok(match value {
        AwsStreamAttributeValue::S(value) => StorageValue::S(value.clone()),
        AwsStreamAttributeValue::N(value) => StorageValue::N(value.clone()),
        AwsStreamAttributeValue::B(value) => StorageValue::B(BASE64.encode(value.as_ref())),
        AwsStreamAttributeValue::Ss(value) => StorageValue::SS(value.clone()),
        AwsStreamAttributeValue::Ns(value) => StorageValue::NS(value.clone()),
        AwsStreamAttributeValue::Bs(value) => StorageValue::BS(
            value
                .iter()
                .map(|blob| BASE64.encode(blob.as_ref()))
                .collect(),
        ),
        AwsStreamAttributeValue::Bool(value) => StorageValue::BOOL(*value),
        AwsStreamAttributeValue::Null(value) => StorageValue::NULL(*value),
        AwsStreamAttributeValue::L(values) => StorageValue::L(
            values
                .iter()
                .map(borrowed_value_to_contract)
                .collect::<AnalyticsStorageResult<Vec<_>>>()?,
        ),
        AwsStreamAttributeValue::M(values) => StorageValue::M(
            values
                .iter()
                .map(|(name, value)| Ok((name.clone(), borrowed_value_to_contract(value)?)))
                .collect::<AnalyticsStorageResult<StorageItem>>()?,
        ),
        _ => {
            return Err(AnalyticsStorageError::new(
                AnalyticsStorageErrorKind::UnsupportedAwsAttributeValue,
            ));
        }
    })
}
