use analytics_contract::{StorageItem, StorageStreamRecord};
use analytics_storage::{PollBatch, PolledRecord, SourceCheckpoint};
use serde_json::json;
use storage_types::SystemStreamPosition;

use super::{SourceJobKind, SourceJobPayload, observation::SourceJobShared};

#[test]
fn given_logical_records_when_native_destinations_are_iterated_then_complete_images_order_and_positions_are_preserved()
 {
    let record: StorageStreamRecord = serde_json::from_value(json!({
        "SequenceNumber": "17",
        "Keys": {"pk": {"S": "event"}},
        "OldImage": {"payload": {"S": "old"}},
        "NewImage": {
            "nested": {"M": {"values": {"L": [{"B": "AAE="}, {"NULL": true}]}}},
            "strings": {"SS": ["a", "b"]}, "numbers": {"NS": ["12.3400", "-9"]},
            "binary": {"BS": ["AAE=", "AgM="]}
        }
    }))
    .unwrap();
    for count in [1, 4, 16] {
        for position in [
            None,
            Some(SystemStreamPosition::try_new("shard", "lineage", "17").unwrap()),
        ] {
            let destinations = (0..count)
                .map(|index| format!("destination_{index}"))
                .collect::<Vec<_>>();
            let source = PolledRecord {
                source_table_name: "source".into(),
                shard_id: "shard".into(),
                analytics_table_names: destinations.clone(),
                record_key: "event".into(),
                record: record.clone(),
                system_position: position.clone(),
            };
            let mut output = SourceJobShared::into_destination_records(source);
            for destination in destinations {
                let item = output.next().unwrap();
                assert_eq!(item.analytics_table_name, destination);
                assert_eq!(item.source_table_name, "source");
                assert_eq!(item.shard_id.as_deref(), Some("shard"));
                assert_eq!(item.record_key, b"event");
                assert_eq!(item.record, record);
                assert_eq!(item.system_position, position);
            }
            assert!(output.next().is_none());
            assert!(output.next().is_none());
        }
    }
}

#[test]
fn given_logical_fanout_when_native_intake_ingests_then_all_destination_documents_and_late_failure_rollback_are_exact()
 {
    for count in [1, 4, 16] {
        let engine = analytics_engine::AnalyticsEngine::connect_duckdb(":memory:").unwrap();
        let mut manifest = analytics_fixtures::users_manifest();
        manifest.tables[0].analytics_table_name = "destination_0".into();
        for index in 1..count {
            let mut table = manifest.tables[0].clone();
            table.analytics_table_name = format!("destination_{index}");
            manifest.tables.push(table);
        }
        let destinations = manifest
            .tables
            .iter()
            .map(|table| table.analytics_table_name.clone())
            .collect::<Vec<_>>();
        engine.ensure_manifest(&manifest).unwrap();
        let record = StorageStreamRecord {
            sequence_number: "17".into(),
            keys: StorageItem::new(),
            old_image: None,
            new_image: Some(
                serde_json::from_value(json!({
                    "profile": {"M": {"email": {"S": "event@example.test"}}},
                    "nested": {"M": {"values": {"L": [{"B": "AAE="}, {"NULL": true}]}}}
                }))
                .unwrap(),
            ),
        };
        let source = PolledRecord {
            source_table_name: "tenant_01".into(),
            shard_id: "shard".into(),
            analytics_table_names: destinations.clone(),
            record_key: "event".into(),
            record,
            system_position: None,
        };
        let expanded = destinations
            .iter()
            .map(|destination| PolledRecord {
                analytics_table_names: vec![destination.clone()],
                ..source.clone()
            })
            .collect();
        let digest = analytics_engine::manifest_digest(&manifest);
        let baseline = encode_records(expanded, digest);
        let encoded = encode_records(vec![source.clone()], digest);
        assert_eq!(
            encoded, baseline,
            "logical routing changed persisted V1 bytes"
        );
        let batch = SourceJobPayload::decode(&encoded).unwrap().into_batch();
        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.records[0].analytics_table_names, destinations);
        assert_eq!(batch.records[0].record, source.record);
        let outcomes = engine
            .ingest_stream_record_batch(
                &manifest,
                batch
                    .records
                    .into_iter()
                    .flat_map(SourceJobShared::into_destination_records)
                    .collect(),
            )
            .unwrap();
        assert_eq!(
            outcomes,
            vec![analytics_engine::IngestOutcome::Inserted; count]
        );
        let expected = json!({"email": "event@example.test", "item": {
            "profile": {"email": "event@example.test"}, "nested": {"values": ["AAE=", null]}
        }});
        for table in &destinations {
            assert_eq!(
                engine
                    .query_unscoped_sql_json(&format!("SELECT email, item FROM {table}"))
                    .unwrap(),
                std::slice::from_ref(&expected)
            );
        }
        let mut invalid = source;
        invalid.record_key = "late-event".into();
        invalid.analytics_table_names = vec![destinations[0].clone(), "absent".into()];
        assert!(matches!(
            engine.ingest_stream_record_batch(
                &manifest,
                SourceJobShared::into_destination_records(invalid).collect()
            ),
            Err(analytics_engine::AnalyticsEngineError::TableNotRegistered(table)) if table == "absent"
        ));
        for table in &destinations {
            assert_eq!(
                engine
                    .query_unscoped_sql_json(&format!("SELECT email, item FROM {table}"))
                    .unwrap(),
                std::slice::from_ref(&expected)
            );
        }
    }
}

fn encode_records(
    records: Vec<PolledRecord>,
    digest: [u8; analytics_engine::MANIFEST_DIGEST_BYTES],
) -> Vec<u8> {
    SourceJobPayload::from_batch(
        SourceJobKind::StorageStream,
        PollBatch {
            source_response_count: 1,
            source_nonempty_response_count: 1,
            source_record_count: 1,
            source_encoded_bytes: 0,
            records,
            checkpoints: vec![SourceCheckpoint {
                source_table_name: "tenant_01".into(),
                shard_id: "shard".into(),
                position: "17".into(),
            }],
            source_predecessor: None,
            system_stream: None,
        },
        1_000,
        digest,
    )
    .unwrap()
    .encode()
    .unwrap()
}
