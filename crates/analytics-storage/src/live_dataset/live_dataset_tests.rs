use std::sync::Arc;

use analytics_contract::{DatasetQuery, DatasetQueryMode, DatasetRegistration};
use storage::{DatabaseManager, DeleteItemInput, PutItemInput};
use storage_types::{
    AttributeDefinition, AttributeMap, AttributeValue, BillingMode, CreateTableRequest,
    KeyAttributeType, KeySchemaElement, KeyType, TableName,
};

use super::{LiveDatasetError, LiveDatasetManager};

#[derive(Clone)]
struct Fixture {
    db: Arc<DatabaseManager>,
    manager: LiveDatasetManager,
    catalog: TableName,
    source: TableName,
    tenant: String,
    application: String,
}
impl Fixture {
    async fn new() -> Self {
        let db = Arc::new(DatabaseManager::new_for_test().await.unwrap());
        let catalog = TableName::new("metadata");
        let source = LiveDatasetManager::source_table("tenant_one", "projects").unwrap();
        for table in [&catalog, &source] {
            create_table(&db, table.clone()).await;
        }
        initialize_source(&db, "tenant_one", "projects").await;
        let manager = LiveDatasetManager::new(db.clone());
        manager
            .register_dataset(
                "tenant_one",
                catalog.clone(),
                DatasetRegistration {
                    dataset_id: "projects".into(),
                    application_id: "projects".into(),
                    allowed_fields: vec!["id".into(), "status".into(), "event_time".into()],
                    expected_version: 0,
                },
                vec![],
            )
            .await
            .unwrap();
        Self {
            db,
            manager,
            catalog,
            source,
            tenant: "tenant_one".into(),
            application: "projects".into(),
        }
    }
    fn record(id: usize, status: &str) -> super::ApplicationRecord {
        super::ApplicationRecord {
            id: format!("{id:06}"),
            status: status.into(),
            event_time: 100,
            fields: [("secret".into(), AttributeValue::S("hidden".into()))]
                .into_iter()
                .collect(),
        }
    }
    async fn source_state(&self) -> super::ApplicationSource {
        let item = self
            .db
            .get_item_with_consistent_read(
                self.source.clone(),
                LiveDatasetManager::source_marker_key(),
                true,
            )
            .await
            .unwrap()
            .unwrap();
        super::ApplicationSource::from_marker(
            &self.tenant,
            &self.application,
            &item.to_attribute_map().unwrap(),
        )
        .unwrap()
    }
    async fn write(&self, mutations: Vec<super::ApplicationMutation>) {
        let request = self
            .source_state()
            .await
            .transaction(uuid::Uuid::new_v4(), &mutations)
            .unwrap();
        self.db
            .transact_write_items(serde_json::from_value(request).unwrap())
            .await
            .unwrap();
    }
    async fn put(&self, id: usize, status: &str) {
        let key: storage_types::KeyAttributes = [
            ("pk".into(), AttributeValue::S(format!("RECORD#{id:06}"))),
            ("sk".into(), AttributeValue::S("UNIQUE".into())),
        ]
        .into_iter()
        .collect();
        let previous = self
            .db
            .get_item_with_consistent_read(self.source.clone(), key, true)
            .await
            .unwrap()
            .map(|item| {
                let map = item.to_attribute_map().unwrap();
                let AttributeValue::S(status) = map.get("status").unwrap() else {
                    panic!()
                };
                Self::record(id, status)
            });
        self.write(vec![super::ApplicationMutation {
            previous,
            next: Some(Self::record(id, status)),
        }])
        .await;
    }
    async fn put_range(&self, start: usize, end: usize, status: &str) {
        for from in (start..end).step_by(25) {
            let mutations = (from..end.min(from + 25))
                .map(|id| super::ApplicationMutation {
                    previous: None,
                    next: Some(Self::record(id, status)),
                })
                .collect();
            self.write(mutations).await;
        }
    }

    async fn query(
        &self,
        request: DatasetQuery,
    ) -> Result<analytics_contract::DatasetQueryResponse, LiveDatasetError> {
        self.manager
            .query_dataset("tenant_one", self.catalog.clone(), "projects", request)
            .await
    }
    async fn close(&self, id: usize) {
        self.put(id, "closed").await;
    }
}

async fn initialize_source(db: &DatabaseManager, tenant: &str, application: &str) {
    let request: storage_types::PutItemRequest = serde_json::from_value(
        super::ApplicationSource::initialize_request(tenant, application).unwrap(),
    )
    .unwrap();
    db.put_item(
        PutItemInput::builder()
            .table_name(request.table_name)
            .item(request.item)
            .condition_expression("attribute_not_exists(pk)")
            .build(),
    )
    .await
    .unwrap();
}

async fn create_table(db: &DatabaseManager, table: TableName) {
    let request = CreateTableRequest::new(
        table,
        ["pk", "sk"]
            .map(|name| AttributeDefinition {
                attribute_name: name.into(),
                attribute_type: KeyAttributeType::S,
            })
            .to_vec(),
        vec![
            KeySchemaElement {
                attribute_name: "pk".into(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".into(),
                key_type: KeyType::Range,
            },
        ],
        BillingMode::PayPerRequest,
    );
    db.create_table(&request).await.unwrap();
}
fn count_query() -> DatasetQuery {
    DatasetQuery {
        expected_dataset_version: 1,
        status: "open".into(),
        from_ms: 0,
        through_ms: 1000,
        mode: DatasetQueryMode::Count,
        page_size: 100,
        cursor: None,
    }
}

#[tokio::test]
async fn given_real_projects_when_count_repeats_after_close_and_reopen_then_source_watermark_and_count_change()
 {
    let f = Fixture::new().await;
    f.put(1, "open").await;
    f.put(2, "open").await;
    f.put(3, "closed").await;
    let first = f.query(count_query()).await.unwrap();
    assert_eq!(first.count, Some(2));
    let repeated = f.query(count_query()).await.unwrap();
    assert_eq!(first.source_watermark, repeated.source_watermark);
    f.close(1).await;
    let changed = f.query(count_query()).await.unwrap();
    assert_eq!(changed.count, Some(1));
    assert_ne!(first.source_watermark, changed.source_watermark);
    f.put(1, "open").await;
    let reopened = f.query(count_query()).await.unwrap();
    assert_eq!(reopened.count, Some(2));
    assert_ne!(changed.source_watermark, reopened.source_watermark);
    let catalog_before = f
        .manager
        .get_dataset("tenant_one", f.catalog.clone(), "projects")
        .await
        .unwrap()
        .cursor_secret;
    f.query(count_query()).await.unwrap();
    let catalog_after = f
        .manager
        .get_dataset("tenant_one", f.catalog.clone(), "projects")
        .await
        .unwrap()
        .cursor_secret;
    assert_eq!(catalog_before, catalog_after, "query cannot write catalog");
}

#[tokio::test]
async fn given_dataset_when_invalid_fields_version_tenant_and_cursor_are_requested_then_refusals_preserve_source()
 {
    let f = Fixture::new().await;
    f.put(1, "open").await;
    f.put(2, "open").await;
    let original = f.query(count_query()).await.unwrap();
    assert!(matches!(
        f.manager
            .query_dataset("tenant_two", f.catalog.clone(), "projects", count_query())
            .await,
        Err(LiveDatasetError::NotFound)
    ));
    let mut invalid = count_query();
    invalid.mode = DatasetQueryMode::Rows {
        fields: vec!["secret".into()],
    };
    assert!(matches!(
        f.query(invalid).await,
        Err(LiveDatasetError::InvalidQuery)
    ));
    let mut invalid = count_query();
    invalid.expected_dataset_version = 2;
    assert!(matches!(
        f.query(invalid).await,
        Err(LiveDatasetError::VersionConflict)
    ));
    let mut invalid = count_query();
    invalid.through_ms = u64::MAX;
    assert!(matches!(
        f.query(invalid).await,
        Err(LiveDatasetError::InvalidQuery)
    ));
    let mut rows = count_query();
    rows.mode = DatasetQueryMode::Rows {
        fields: vec!["id".into()],
    };
    rows.page_size = 1;
    let first = f.query(rows.clone()).await.unwrap();
    assert_eq!(first.rows.len(), 1);
    assert!(first.rows[0].get("secret").is_none());
    let mut next = rows.clone();
    next.cursor = first.next_cursor.clone();
    assert_eq!(f.query(next.clone()).await.unwrap().rows.len(), 1);
    next.status = "closed".into();
    assert!(matches!(
        f.query(next).await,
        Err(LiveDatasetError::InvalidQuery)
    ));
    let mut forged = rows.clone();
    forged.cursor = Some(format!("{}x", first.next_cursor.as_ref().unwrap()));
    assert!(matches!(
        f.query(forged).await,
        Err(LiveDatasetError::InvalidQuery)
    ));
    f.close(1).await;
    let mut stale = rows;
    stale.cursor = first.next_cursor;
    assert!(matches!(
        f.query(stale).await,
        Err(LiveDatasetError::SourceChanged)
    ));
    let after = f.query(count_query()).await.unwrap();
    assert_eq!(after.count, Some(1));
    assert_ne!(original.source_watermark, after.source_watermark);
}

#[tokio::test]
async fn given_indexed_sources_when_same_bounded_query_runs_then_work_grows_only_by_matching_pages()
{
    let f = Fixture::new().await;
    assert_eq!(f.query(count_query()).await.unwrap().count, Some(0));
    let other_source = LiveDatasetManager::source_table("tenant_two", "other_application").unwrap();
    create_table(&f.db, other_source.clone()).await;
    initialize_source(&f.db, "tenant_two", "other_application").await;
    let unrelated = Fixture {
        source: other_source,
        tenant: "tenant_two".into(),
        application: "other_application".into(),
        ..f.clone()
    };
    for target in [1, 100, 10_000] {
        let start = if target == 1 {
            0
        } else if target == 100 {
            1
        } else {
            100
        };
        f.put_range(start, target, "open").await;
        f.put_range(20_000 + start, 20_000 + target, "closed").await;
        unrelated.put_range(start, target, "open").await;
        // Unrelated tenant/application tables and status partitions are never
        // examined.
        let started = std::time::Instant::now();
        let result = f.query(count_query()).await.unwrap();
        println!(
            "source_rows={target} examined={} pages={} elapsed_us={}",
            result.examined_rows,
            result.source_pages,
            started.elapsed().as_micros()
        );
        assert_eq!(result.count, Some(target as u64));
        assert_eq!(result.examined_rows, target as u32);
        assert_eq!(result.source_pages, (target as u32).div_ceil(100));
    }
    f.put(10_001, "open").await;
    assert!(matches!(
        f.query(count_query()).await,
        Err(LiveDatasetError::QueryTooBroad)
    ));
}

#[tokio::test]
async fn given_source_mutation_between_index_pages_when_count_completes_then_mixed_result_is_refused()
 {
    let f = Fixture::new().await;
    for id in 0..3 {
        f.put(id, "open").await;
    }
    let pause = Arc::new(super::DatasetPagePause {
        reached: tokio::sync::Notify::new(),
        resume: tokio::sync::Notify::new(),
        observed: std::sync::atomic::AtomicBool::new(false),
    });
    let mut manager = f.manager.clone();
    manager.page_pause = Some(pause.clone());
    let catalog = f.catalog.clone();
    let mut query = count_query();
    query.page_size = 1;
    let pending = tokio::spawn(async move {
        manager
            .query_dataset("tenant_one", catalog, "projects", query)
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), pause.reached.notified())
        .await
        .unwrap();
    f.close(2).await;
    pause.resume.notify_one();
    assert!(matches!(
        pending.await.unwrap(),
        Err(LiveDatasetError::SourceChanged)
    ));
    assert_eq!(f.query(count_query()).await.unwrap().count, Some(2));
}

#[tokio::test]
async fn given_registration_when_version_retries_race_then_only_one_conditional_write_succeeds() {
    let f = Fixture::new().await;
    let registration = DatasetRegistration {
        dataset_id: "projects".into(),
        application_id: "projects".into(),
        allowed_fields: vec!["id".into()],
        expected_version: 1,
    };
    let (first, second) = tokio::join!(
        f.manager.register_dataset(
            "tenant_one",
            f.catalog.clone(),
            registration.clone(),
            vec![]
        ),
        f.manager
            .register_dataset("tenant_one", f.catalog.clone(), registration, vec![])
    );
    assert_ne!(first.is_ok(), second.is_ok());
    assert!(matches!(
        first.as_ref().err().or(second.as_ref().err()),
        Some(LiveDatasetError::VersionConflict)
    ));
    let mut query = count_query();
    query.expected_dataset_version = 2;
    assert_eq!(f.query(query).await.unwrap().count, Some(0));
}

#[tokio::test]
async fn given_companion_effect_failure_when_registration_updates_then_catalog_and_observation_remain_unchanged()
 {
    let f = Fixture::new().await;
    let item: AttributeMap = [
        ("pk".into(), AttributeValue::S("AUDIT#existing".into())),
        ("sk".into(), AttributeValue::S("AUDIT#existing".into())),
    ]
    .into_iter()
    .collect();
    f.db.put_item(
        PutItemInput::builder()
            .table_name(f.catalog.clone())
            .item(item.clone())
            .build(),
    )
    .await
    .unwrap();
    let companion = storage_types::TransactEncodeItem::builder()
        .put(
            storage_types::TransactEncodePutRequest::builder()
                .table_name(f.catalog.clone())
                .item(storage_types::single_table_entity::WireEntity::unindexed(
                    storage_types::WireItem::try_from(item).unwrap(),
                ))
                .condition_expression("attribute_not_exists(pk)")
                .build(),
        )
        .build();
    let registration = DatasetRegistration {
        dataset_id: "projects".into(),
        application_id: "projects".into(),
        allowed_fields: vec!["id".into()],
        expected_version: 1,
    };
    assert!(matches!(
        f.manager
            .register_dataset(
                "tenant_one",
                f.catalog.clone(),
                registration,
                vec![companion]
            )
            .await,
        Err(LiveDatasetError::Storage)
    ));
    assert_eq!(f.query(count_query()).await.unwrap().dataset_version, 1);
}

#[tokio::test]
async fn given_catalog_edit_or_removal_during_query_when_pages_complete_then_stale_dataset_result_is_refused()
 {
    for action in ["edit", "remove", "recreate"] {
        let f = Fixture::new().await;
        f.put(1, "open").await;
        f.put(2, "open").await;
        let pause = Arc::new(super::DatasetPagePause {
            reached: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
            observed: std::sync::atomic::AtomicBool::new(false),
        });
        let mut manager = f.manager.clone();
        manager.page_pause = Some(pause.clone());
        let catalog = f.catalog.clone();
        let pending = tokio::spawn(async move {
            manager
                .query_dataset("tenant_one", catalog, "projects", count_query())
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), pause.reached.notified())
            .await
            .unwrap();
        if action != "edit" {
            f.manager
                .remove_dataset(
                    "tenant_one",
                    f.catalog.clone(),
                    analytics_contract::DatasetRevision {
                        dataset_id: "projects".into(),
                        version: 1,
                    },
                    vec![],
                )
                .await
                .unwrap();
        } else {
            f.manager
                .register_dataset(
                    "tenant_one",
                    f.catalog.clone(),
                    DatasetRegistration {
                        dataset_id: "projects".into(),
                        application_id: "projects".into(),
                        allowed_fields: vec!["id".into()],
                        expected_version: 1,
                    },
                    vec![],
                )
                .await
                .unwrap();
        }
        if action == "recreate" {
            f.manager
                .register_dataset(
                    "tenant_one",
                    f.catalog.clone(),
                    DatasetRegistration {
                        dataset_id: "projects".into(),
                        application_id: "projects".into(),
                        allowed_fields: vec!["id".into()],
                        expected_version: 0,
                    },
                    vec![],
                )
                .await
                .unwrap();
        }
        pause.resume.notify_one();
        assert!(matches!(
            pending.await.unwrap(),
            Err(LiveDatasetError::VersionConflict)
        ));
    }
}

#[tokio::test]
async fn given_missing_source_index_descriptor_when_registered_or_queried_then_source_is_refused_without_catalog_write()
 {
    let f = Fixture::new().await;
    let key = LiveDatasetManager::source_marker_key();
    f.db.delete_item(
        DeleteItemInput::builder()
            .table_name(f.source.clone())
            .key(key)
            .build(),
    )
    .await
    .unwrap();
    let registration = DatasetRegistration {
        dataset_id: "projects".into(),
        application_id: "projects".into(),
        allowed_fields: vec!["id".into()],
        expected_version: 1,
    };
    assert!(matches!(
        f.manager
            .register_dataset("tenant_one", f.catalog.clone(), registration, vec![])
            .await,
        Err(LiveDatasetError::SourceUnavailable)
    ));
    assert!(matches!(
        f.query(count_query()).await,
        Err(LiveDatasetError::SourceUnavailable)
    ));
    let stored = f
        .manager
        .get_dataset("tenant_one", f.catalog.clone(), "projects")
        .await
        .unwrap();
    assert_eq!(stored.dataset.version, 1);
}

#[tokio::test]
async fn given_source_recreated_after_first_page_when_cursor_reused_then_table_incarnation_change_is_refused()
 {
    let f = Fixture::new().await;
    f.put(1, "open").await;
    f.put(2, "open").await;
    let mut query = count_query();
    query.mode = DatasetQueryMode::Rows {
        fields: vec!["id".into()],
    };
    query.page_size = 1;
    let first = f.query(query.clone()).await.unwrap();
    let catalog_key: storage_types::KeyAttributes = [
        (
            "pk".into(),
            AttributeValue::S("ANALYTICS_DATASET#tenant_one#projects".into()),
        ),
        (
            "sk".into(),
            AttributeValue::S("ANALYTICS_DATASET#tenant_one#projects".into()),
        ),
    ]
    .into_iter()
    .collect();
    let registration =
        f.db.get_item(f.catalog.clone(), catalog_key)
            .await
            .unwrap()
            .unwrap();
    // Native table deletion retains its name until reclamation. Use the storage
    // test maintenance owner, then restore only the catalog receipt, never
    // rows.
    f.db.clear_all_tables().await.unwrap();
    create_table(&f.db, f.catalog.clone()).await;
    create_table(&f.db, f.source.clone()).await;
    f.db.put_item(
        PutItemInput::builder()
            .table_name(f.catalog.clone())
            .item(registration)
            .build(),
    )
    .await
    .unwrap();
    initialize_source(&f.db, "tenant_one", "projects").await;
    f.put(3, "open").await;
    query.cursor = first.next_cursor;
    assert!(matches!(
        f.query(query).await,
        Err(LiveDatasetError::SourceChanged)
    ));
}

#[tokio::test]
async fn given_registration_when_removed_then_source_records_and_watermark_are_preserved() {
    let f = Fixture::new().await;
    f.put(1, "open").await;
    let before = f.query(count_query()).await.unwrap();
    f.manager
        .remove_dataset(
            "tenant_one",
            f.catalog.clone(),
            analytics_contract::DatasetRevision {
                dataset_id: "projects".into(),
                version: 1,
            },
            vec![],
        )
        .await
        .unwrap();
    assert!(matches!(
        f.query(count_query()).await,
        Err(LiveDatasetError::NotFound)
    ));
    f.manager
        .register_dataset(
            "tenant_one",
            f.catalog.clone(),
            DatasetRegistration {
                dataset_id: "projects".into(),
                application_id: "projects".into(),
                allowed_fields: vec!["id".into()],
                expected_version: 0,
            },
            vec![],
        )
        .await
        .unwrap();
    let after = f.query(count_query()).await.unwrap();
    assert_eq!(after.count, Some(1));
    assert_eq!(before.source_watermark, after.source_watermark);
}

#[tokio::test]
async fn given_same_source_marker_when_writers_race_then_one_commits_and_failed_retry_rolls_back() {
    let f = Fixture::new().await;
    let source = f.source_state().await;
    let make = |id| {
        serde_json::from_value(
            source
                .transaction(
                    uuid::Uuid::new_v4(),
                    &[super::ApplicationMutation {
                        previous: None,
                        next: Some(Fixture::record(id, "open")),
                    }],
                )
                .unwrap(),
        )
        .unwrap()
    };
    let (a, b) = tokio::join!(
        f.db.transact_write_items(make(1)),
        f.db.transact_write_items(make(2))
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    assert_eq!(f.query(count_query()).await.unwrap().count, Some(1));
    let before = f.source_state().await.watermark();
    let winner = if a.is_ok() { 1 } else { 2 };
    let duplicate = f
        .source_state()
        .await
        .transaction(
            uuid::Uuid::new_v4(),
            &[super::ApplicationMutation {
                previous: None,
                next: Some(Fixture::record(winner, "open")),
            }],
        )
        .unwrap();
    assert!(
        f.db.transact_write_items(serde_json::from_value(duplicate).unwrap())
            .await
            .is_err()
    );
    assert_eq!(f.source_state().await.watermark(), before);
    let record = Fixture::record(winner, "open");
    f.write(vec![super::ApplicationMutation {
        previous: Some(record),
        next: None,
    }])
    .await;
    assert_eq!(f.query(count_query()).await.unwrap().count, Some(0));
    assert_ne!(f.source_state().await.watermark(), before);
}

#[tokio::test]
async fn given_same_record_status_time_when_fields_change_then_stale_content_edit_and_transport_retry_are_safe()
 {
    let f = Fixture::new().await;
    let original = Fixture::record(1, "open");
    let request = f
        .source_state()
        .await
        .transaction(
            uuid::Uuid::new_v4(),
            &[super::ApplicationMutation {
                previous: None,
                next: Some(original.clone()),
            }],
        )
        .unwrap();
    f.db.transact_write_items(serde_json::from_value(request.clone()).unwrap())
        .await
        .unwrap();
    let watermark = f.source_state().await.watermark();
    f.db.transact_write_items(serde_json::from_value(request).unwrap())
        .await
        .unwrap();
    assert_eq!(f.source_state().await.watermark(), watermark);
    let mut edited = original.clone();
    edited
        .fields
        .insert("secret", AttributeValue::S("first edit".into()));
    f.write(vec![super::ApplicationMutation {
        previous: Some(original.clone()),
        next: Some(edited),
    }])
    .await;
    let before = f.source_state().await.watermark();
    let mut stale = original.clone();
    stale
        .fields
        .insert("secret", AttributeValue::S("stale edit".into()));
    let request = f
        .source_state()
        .await
        .transaction(
            uuid::Uuid::new_v4(),
            &[super::ApplicationMutation {
                previous: Some(original),
                next: Some(stale),
            }],
        )
        .unwrap();
    assert!(
        f.db.transact_write_items(serde_json::from_value(request).unwrap())
            .await
            .is_err()
    );
    assert_eq!(f.source_state().await.watermark(), before);
    assert_eq!(f.query(count_query()).await.unwrap().count, Some(1));
}

#[tokio::test]
async fn given_registered_source_when_marker_is_malformed_then_query_fails_closed() {
    let f = Fixture::new().await;
    for (name, value) in [
        ("source_incarnation", AttributeValue::S("not-a-uuid".into())),
        ("mutation_sequence", AttributeValue::N("-1".into())),
    ] {
        let request =
            super::ApplicationSource::initialize_request("tenant_one", "projects").unwrap();
        let mut item: AttributeMap = serde_json::from_value(request["Item"].clone()).unwrap();
        item.insert(name, value);
        f.db.put_item(
            PutItemInput::builder()
                .table_name(f.source.clone())
                .item(item)
                .build(),
        )
        .await
        .unwrap();
        assert!(matches!(
            f.query(count_query()).await,
            Err(LiveDatasetError::SourceUnavailable)
        ));
    }
}

#[tokio::test]
async fn given_loaded_registration_when_removed_and_recreated_before_delete_then_new_registration_survives()
 {
    let f = Fixture::new().await;
    let pause = Arc::new(super::DatasetPagePause {
        reached: tokio::sync::Notify::new(),
        resume: tokio::sync::Notify::new(),
        observed: std::sync::atomic::AtomicBool::new(false),
    });
    let mut manager = f.manager.clone();
    manager.page_pause = Some(pause.clone());
    let catalog = f.catalog.clone();
    let pending = tokio::spawn(async move {
        manager
            .remove_dataset(
                "tenant_one",
                catalog,
                analytics_contract::DatasetRevision {
                    dataset_id: "projects".into(),
                    version: 1,
                },
                vec![],
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), pause.reached.notified())
        .await
        .unwrap();
    f.manager
        .remove_dataset(
            "tenant_one",
            f.catalog.clone(),
            analytics_contract::DatasetRevision {
                dataset_id: "projects".into(),
                version: 1,
            },
            vec![],
        )
        .await
        .unwrap();
    f.manager
        .register_dataset(
            "tenant_one",
            f.catalog.clone(),
            DatasetRegistration {
                dataset_id: "projects".into(),
                application_id: "projects".into(),
                allowed_fields: vec!["id".into(), "status".into(), "event_time".into()],
                expected_version: 0,
            },
            vec![],
        )
        .await
        .unwrap();
    let recreated = f
        .manager
        .get_dataset("tenant_one", f.catalog.clone(), "projects")
        .await
        .unwrap()
        .cursor_secret;
    pause.resume.notify_one();
    assert!(matches!(
        pending.await.unwrap(),
        Err(LiveDatasetError::VersionConflict)
    ));
    assert_eq!(
        f.manager
            .get_dataset("tenant_one", f.catalog.clone(), "projects")
            .await
            .unwrap()
            .cursor_secret,
        recreated
    );
}

#[tokio::test]
async fn given_shared_catalog_when_tenants_register_same_dataset_then_reads_and_removal_are_isolated()
 {
    let f = Fixture::new().await;
    f.put(1, "open").await;
    let source = LiveDatasetManager::source_table("tenant_two", "projects").unwrap();
    create_table(&f.db, source).await;
    initialize_source(&f.db, "tenant_two", "projects").await;
    let second = f
        .manager
        .register_dataset(
            "tenant_two",
            f.catalog.clone(),
            DatasetRegistration {
                dataset_id: "projects".into(),
                application_id: "projects".into(),
                allowed_fields: vec!["id".into()],
                expected_version: 0,
            },
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(second.tenant, "tenant_two");
    assert_eq!(f.query(count_query()).await.unwrap().count, Some(1));
    assert_eq!(
        f.manager
            .query_dataset("tenant_two", f.catalog.clone(), "projects", count_query())
            .await
            .unwrap()
            .count,
        Some(0)
    );
    f.manager
        .remove_dataset(
            "tenant_two",
            f.catalog.clone(),
            analytics_contract::DatasetRevision {
                dataset_id: "projects".into(),
                version: 1,
            },
            vec![],
        )
        .await
        .unwrap();
    assert!(matches!(
        f.manager
            .query_dataset("tenant_two", f.catalog.clone(), "projects", count_query())
            .await,
        Err(LiveDatasetError::NotFound)
    ));
    assert_eq!(f.query(count_query()).await.unwrap().count, Some(1));
}
