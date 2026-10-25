use std::collections::BTreeMap;

use analytics_contract::{
    AnalyticsColumnType, AnalyticsManifest, PrimitiveColumnType, ProjectionColumn, RowIdentity,
    SourcePosition, StorageItem, StorageStreamRecord, StorageValue, TableRegistration,
    TenantSelector,
};
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;
use storage_types::VecMap;

use crate::{AnalyticsEngine, AnalyticsEngineError};

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct IngestionState {
    rows: BTreeMap<String, String>,
    #[serde(rename = "auditRows")]
    audit_rows: BTreeMap<String, String>,
}

impl State<IngestionDriver> for IngestionState {
    fn from_driver(driver: &IngestionDriver) -> Result<Self> {
        Ok(Self {
            rows: driver.user_rows()?,
            audit_rows: driver.audit_rows()?,
        })
    }
}

struct IngestionDriver {
    engine: AnalyticsEngine,
    manifest: AnalyticsManifest,
}

impl Default for IngestionDriver {
    fn default() -> Self {
        let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect test duckdb");
        let manifest = manifest();
        engine
            .ensure_manifest(&manifest)
            .expect("initialize test manifest");
        Self { engine, manifest }
    }
}

impl Driver for IngestionDriver {
    type State = IngestionState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            InsertUser(user_id: String, email: String) => {
                self.insert_user(user_id.as_str(), email.as_str())?;
            },
            UpdateUser(user_id: String, email: String) => {
                self.update_user(user_id.as_str(), email.as_str())?;
            },
            DeleteUser(user_id: String) => {
                self.delete_user(user_id.as_str())?;
            },
            InsertAudit(audit_id: String, action_name: String) => {
                self.insert_audit(audit_id.as_str(), action_name.as_str())?;
            },
            DeleteAuditSkipped(audit_id: String) => {
                self.delete_audit(audit_id.as_str())?;
            },
            UnregisteredIngestRejected => {
                self.unregistered_ingest_rejected()?;
            },
        })
    }
}

impl IngestionDriver {
    fn insert_user(&self, user_id: &str, email: &str) -> Result {
        self.engine.ingest_stream_record(
            &self.manifest,
            "users",
            user_id.as_bytes(),
            StorageStreamRecord {
                keys: storage_key("USER", user_id),
                sequence_number: format!("seq-{user_id}-insert"),
                old_image: None,
                new_image: Some(user_item(user_id, email)),
            },
        )?;
        Ok(())
    }

    fn update_user(&self, user_id: &str, email: &str) -> Result {
        let old_email = self
            .user_rows()?
            .get(user_id)
            .cloned()
            .unwrap_or_else(|| email.to_string());
        self.engine.ingest_stream_record(
            &self.manifest,
            "users",
            user_id.as_bytes(),
            StorageStreamRecord {
                keys: storage_key("USER", user_id),
                sequence_number: format!("seq-{user_id}-update"),
                old_image: Some(user_item(user_id, old_email.as_str())),
                new_image: Some(user_item(user_id, email)),
            },
        )?;
        Ok(())
    }

    fn delete_user(&self, user_id: &str) -> Result {
        let Some(old_email) = self.user_rows()?.get(user_id).cloned() else {
            return Ok(());
        };
        self.engine.ingest_stream_record(
            &self.manifest,
            "users",
            user_id.as_bytes(),
            StorageStreamRecord {
                keys: storage_key("USER", user_id),
                sequence_number: format!("seq-{user_id}-delete"),
                old_image: Some(user_item(user_id, old_email.as_str())),
                new_image: None,
            },
        )?;
        Ok(())
    }

    fn insert_audit(&self, audit_id: &str, action_name: &str) -> Result {
        self.engine.ingest_stream_record(
            &self.manifest,
            "audit_events",
            audit_id.as_bytes(),
            StorageStreamRecord {
                keys: storage_key("AUDIT", audit_id),
                sequence_number: format!("seq-{audit_id}-insert"),
                old_image: None,
                new_image: Some(audit_item(audit_id, action_name)),
            },
        )?;
        Ok(())
    }

    fn delete_audit(&self, audit_id: &str) -> Result {
        let Some(action_name) = self.audit_rows()?.get(audit_id).cloned() else {
            return Ok(());
        };
        self.engine.ingest_stream_record(
            &self.manifest,
            "audit_events",
            audit_id.as_bytes(),
            StorageStreamRecord {
                keys: storage_key("AUDIT", audit_id),
                sequence_number: format!("seq-{audit_id}-delete"),
                old_image: Some(audit_item(audit_id, action_name.as_str())),
                new_image: None,
            },
        )?;
        Ok(())
    }

    fn unregistered_ingest_rejected(&self) -> Result {
        match self.engine.ingest_stream_record(
            &self.manifest,
            "orders",
            b"order-1",
            StorageStreamRecord {
                keys: storage_key("ORDER", "order-1"),
                sequence_number: "seq-order-1-insert".to_string(),
                old_image: None,
                new_image: Some(user_item("order-1", "alpha@example.com")),
            },
        ) {
            Err(AnalyticsEngineError::TableNotRegistered(_)) => Ok(()),
            Err(err) => Err(err.into()),
            Ok(_) => Err(anyhow::anyhow!(
                "unregistered ingest unexpectedly succeeded"
            )),
        }
    }

    fn user_rows(&self) -> Result<BTreeMap<String, String>> {
        let rows = self
            .engine
            .query_unscoped_sql_json("select user_id, email from users order by user_id")?;
        Ok(rows
            .into_iter()
            .filter_map(|row| {
                let user_id = row.get("user_id")?.as_str()?.to_string();
                let email = row.get("email")?.as_str()?.to_string();
                Some((user_id, email))
            })
            .collect())
    }

    fn audit_rows(&self) -> Result<BTreeMap<String, String>> {
        let rows = self.engine.query_unscoped_sql_json(
            "select audit_id, action from audit_events order by audit_id",
        )?;
        Ok(rows
            .into_iter()
            .filter_map(|row| {
                let audit_id = row.get("audit_id")?.as_str()?.to_string();
                let action = row.get("action")?.as_str()?.to_string();
                Some((audit_id, action))
            })
            .collect())
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/ingestion.qnt",
    max_samples = 50,
    max_steps = 12,
    seed = "0x1"
)]
fn given_quint_ingestion_model_when_replayed_then_engine_matches() -> impl Driver {
    IngestionDriver::default()
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum LifecycleTableKind {
    Mutable,
    AppendOnly,
    Missing,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum LifecycleRecordKind {
    Matching,
    FilteredOut,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct LifecycleState {
    rows: BTreeMap<String, bool>,
    append_rows: BTreeMap<String, bool>,
    row_counts: BTreeMap<String, i64>,
    row_sequences: BTreeMap<String, i64>,
    rejected_missing_table: bool,
    last_rows_before: BTreeMap<String, bool>,
    last_append_rows_before: BTreeMap<String, bool>,
    last_row_counts_before: BTreeMap<String, i64>,
    last_row_sequences_before: BTreeMap<String, i64>,
    last_operation: LifecycleOperation,
    last_row_id: String,
    last_table_kind: LifecycleTableKind,
    last_record_kind: LifecycleRecordKind,
    last_row_existed_before: bool,
    last_append_row_existed_before: bool,
    last_sequence_before: i64,
    last_incoming_sequence: i64,
    last_sequence_after: i64,
    last_update_stale: bool,
}

#[derive(Debug)]
struct ProductionSnapshot {
    rows: BTreeMap<String, bool>,
    append_rows: BTreeMap<String, bool>,
    row_counts: BTreeMap<String, i64>,
    row_sequences: BTreeMap<String, i64>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum LifecycleOperation {
    NoOperation,
    Ingest,
    Duplicate,
    Update,
    Delete,
}

impl State<LifecycleDriver> for LifecycleState {
    fn from_driver(driver: &LifecycleDriver) -> Result<Self> {
        let snapshot = driver.production_snapshot()?;
        Ok(Self {
            rows: snapshot.rows,
            append_rows: snapshot.append_rows,
            row_counts: snapshot.row_counts,
            row_sequences: snapshot.row_sequences,
            rejected_missing_table: driver.rejected_missing_table,
            last_rows_before: driver.last_rows_before.clone(),
            last_append_rows_before: driver.last_append_rows_before.clone(),
            last_row_counts_before: driver.last_row_counts_before.clone(),
            last_row_sequences_before: driver.last_row_sequences_before.clone(),
            last_operation: driver.last_operation,
            last_row_id: driver.last_row_id.clone(),
            last_table_kind: driver.last_table_kind,
            last_record_kind: driver.last_record_kind,
            last_row_existed_before: driver.last_row_existed_before,
            last_append_row_existed_before: driver.last_append_row_existed_before,
            last_sequence_before: driver.last_sequence_before,
            last_incoming_sequence: driver.last_incoming_sequence,
            last_sequence_after: driver.last_sequence_after,
            last_update_stale: driver.last_update_stale,
        })
    }
}

struct LifecycleDriver {
    engine: AnalyticsEngine,
    manifest: AnalyticsManifest,
    rejected_missing_table: bool,
    last_rows_before: BTreeMap<String, bool>,
    last_append_rows_before: BTreeMap<String, bool>,
    last_row_counts_before: BTreeMap<String, i64>,
    last_row_sequences_before: BTreeMap<String, i64>,
    last_operation: LifecycleOperation,
    last_row_id: String,
    last_table_kind: LifecycleTableKind,
    last_record_kind: LifecycleRecordKind,
    last_row_existed_before: bool,
    last_append_row_existed_before: bool,
    last_sequence_before: i64,
    last_incoming_sequence: i64,
    last_sequence_after: i64,
    last_update_stale: bool,
}

impl Default for LifecycleDriver {
    fn default() -> Self {
        let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect test duckdb");
        let manifest = lifecycle_manifest();
        engine
            .ensure_manifest(&manifest)
            .expect("initialize lifecycle manifest");
        Self {
            engine,
            manifest,
            rejected_missing_table: false,
            last_rows_before: BTreeMap::new(),
            last_append_rows_before: BTreeMap::new(),
            last_row_counts_before: BTreeMap::new(),
            last_row_sequences_before: BTreeMap::new(),
            last_operation: LifecycleOperation::NoOperation,
            last_row_id: String::new(),
            last_table_kind: LifecycleTableKind::Missing,
            last_record_kind: LifecycleRecordKind::FilteredOut,
            last_row_existed_before: false,
            last_append_row_existed_before: false,
            last_sequence_before: 0,
            last_incoming_sequence: 0,
            last_sequence_after: 0,
            last_update_stale: false,
        }
    }
}

impl Driver for LifecycleDriver {
    type State = LifecycleState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            IngestRecord(row_id, table_kind, record_kind) => {
                let row_id: String = row_id;
                self.ingest_record(row_id.as_str(), table_kind, record_kind)?;
            },
            DuplicateInsert(row_id) => {
                let row_id: String = row_id;
                self.duplicate_insert(row_id.as_str())?;
            },
            UpdateRecord(row_id, new_seq, new_kind) => {
                let row_id: String = row_id;
                self.update_record(row_id.as_str(), new_seq, new_kind)?;
            },
            DeleteRecord(row_id, table_kind) => {
                let row_id: String = row_id;
                self.delete_record(row_id.as_str(), table_kind)?;
            },
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/ingestion_lifecycle_bughunt.qnt",
    max_samples = 100,
    max_steps = 12,
    seed = "0x515d"
)]
fn given_quint_ingestion_lifecycle_bughunt_when_replayed_then_engine_matches() -> impl Driver {
    LifecycleDriver::default()
}

#[test]
fn given_insert_delete_stale_update_when_replayed_then_production_skips_without_revival() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect lifecycle engine");
    let manifest = lifecycle_manifest();
    engine
        .ensure_manifest(&manifest)
        .expect("ensure lifecycle manifest");

    assert_eq!(
        engine
            .ingest_stream_record_at_source_position(
                &manifest,
                "users",
                b"row-hostile",
                lifecycle_record("row-hostile", None, Some(LifecycleRecordKind::Matching),),
                &lifecycle_position(1),
            )
            .expect("insert"),
        crate::IngestOutcome::Inserted
    );
    assert_eq!(
        engine
            .ingest_stream_record_at_source_position(
                &manifest,
                "users",
                b"row-hostile",
                lifecycle_record("row-hostile", Some(LifecycleRecordKind::Matching), None,),
                &lifecycle_position(2),
            )
            .expect("delete"),
        crate::IngestOutcome::Deleted
    );
    assert_eq!(
        engine
            .ingest_stream_record_at_source_position(
                &manifest,
                "users",
                b"row-hostile",
                lifecycle_record(
                    "row-hostile",
                    Some(LifecycleRecordKind::Matching),
                    Some(LifecycleRecordKind::Matching),
                ),
                &lifecycle_position(1),
            )
            .expect("stale update"),
        crate::IngestOutcome::Skipped
    );

    let rows = engine
        .query_unscoped_sql_json("select count(*) as row_count from users")
        .expect("query visible rows");
    assert_eq!(rows[0]["row_count"], 0);
    let metadata = engine
        .query_unscoped_sql_json(
            "select row_visible, source_position_json from __analytics_row_source_positions",
        )
        .expect("query source-position authority");
    assert_eq!(metadata.len(), 1);
    assert_eq!(metadata[0]["row_visible"], false);
    assert_eq!(metadata[0]["source_position_json"]["cursor"], "2");

    assert_eq!(
        engine
            .ingest_stream_record_at_source_position(
                &manifest,
                "users",
                b"row-hostile",
                lifecycle_record(
                    "row-hostile",
                    Some(LifecycleRecordKind::FilteredOut),
                    Some(LifecycleRecordKind::Matching),
                ),
                &lifecycle_position(3),
            )
            .expect("higher-cursor recovery"),
        crate::IngestOutcome::Inserted
    );
    let rows = engine
        .query_unscoped_sql_json("select count(*) as row_count from users")
        .expect("query recovered visible rows");
    assert_eq!(rows[0]["row_count"], 1);
    let metadata = engine
        .query_unscoped_sql_json(
            "select row_visible, source_position_json from __analytics_row_source_positions",
        )
        .expect("query recovered source-position authority");
    assert_eq!(metadata[0]["row_visible"], true);
    assert_eq!(metadata[0]["source_position_json"]["cursor"], "3");
}

#[test]
fn given_filtered_record_when_boundary_is_called_then_production_skips_and_state_stays_empty() {
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect lifecycle engine");
    let manifest = lifecycle_manifest();
    engine
        .ensure_manifest(&manifest)
        .expect("ensure lifecycle manifest");

    assert_eq!(
        engine
            .ingest_stream_record_at_source_position(
                &manifest,
                "users",
                b"row-filtered",
                lifecycle_record("row-filtered", None, Some(LifecycleRecordKind::FilteredOut),),
                &lifecycle_position(1),
            )
            .expect("filtered ingest"),
        crate::IngestOutcome::Skipped
    );
    let visible = engine
        .query_unscoped_sql_json("select count(*) as row_count from users")
        .expect("query visible rows");
    let metadata = engine
        .query_unscoped_sql_json(
            "select count(*) as row_count from __analytics_row_source_positions",
        )
        .expect("query source-position metadata");
    assert_eq!(visible[0]["row_count"], 0);
    assert_eq!(metadata[0]["row_count"], 0);
}

#[test]
fn given_malformed_source_metadata_when_reconstructed_then_driver_fails() {
    let mut driver = LifecycleDriver::default();
    driver
        .ingest_record(
            "row-malformed",
            LifecycleTableKind::Mutable,
            LifecycleRecordKind::Matching,
        )
        .expect("insert source metadata");
    driver
        .engine
        .connection()
        .execute_batch(
            "update __analytics_row_source_positions
             set source_position_json = '{}'
             where analytics_table_name = 'users'",
        )
        .expect("corrupt source metadata for negative proof");

    let error = driver
        .production_snapshot()
        .expect_err("malformed durable source metadata must fail reconstruction");
    assert!(error.to_string().contains("no cursor"), "{error}");
}

impl LifecycleDriver {
    fn begin_action(
        &mut self,
        row_id: &str,
        table_kind: LifecycleTableKind,
        record_kind: LifecycleRecordKind,
    ) -> Result {
        let snapshot = self.production_snapshot()?;
        self.last_rows_before = snapshot.rows.clone();
        self.last_append_rows_before = snapshot.append_rows.clone();
        self.last_row_counts_before = snapshot.row_counts.clone();
        self.last_row_sequences_before = snapshot.row_sequences.clone();
        self.last_row_id = row_id.to_string();
        self.last_table_kind = table_kind;
        self.last_record_kind = record_kind;
        self.last_row_existed_before = snapshot.rows.get(row_id).copied().unwrap_or(false);
        self.last_append_row_existed_before =
            snapshot.append_rows.get(row_id).copied().unwrap_or(false);
        self.last_sequence_before = snapshot.row_sequences.get(row_id).copied().unwrap_or(0);
        self.last_incoming_sequence = 0;
        self.last_update_stale = false;
        Ok(())
    }

    fn ingest_record(
        &mut self,
        row_id: &str,
        table_kind: LifecycleTableKind,
        record_kind: LifecycleRecordKind,
    ) -> Result {
        self.begin_action(row_id, table_kind, record_kind)?;
        self.last_operation = LifecycleOperation::Ingest;
        match table_kind {
            LifecycleTableKind::Mutable => {
                let current_position = self.authority_sequence("users", row_id)?;
                let sequence = if self.last_row_existed_before {
                    current_position.max(1)
                } else {
                    current_position.saturating_add(1).max(1)
                };
                self.last_incoming_sequence = if record_kind == LifecycleRecordKind::Matching {
                    sequence
                } else {
                    0
                };
                let outcome = self.engine.ingest_stream_record_at_source_position(
                    &self.manifest,
                    "users",
                    row_id.as_bytes(),
                    lifecycle_record(row_id, None, Some(record_kind)),
                    &lifecycle_position(sequence),
                )?;
                if record_kind == LifecycleRecordKind::FilteredOut
                    && outcome != crate::IngestOutcome::Skipped
                {
                    return Err(anyhow::anyhow!(
                        "filtered mutable ingest returned {outcome:?}"
                    ));
                }
            }
            LifecycleTableKind::AppendOnly => {
                let outcome = self.engine.ingest_stream_record_at_source_position(
                    &self.manifest,
                    "audit_events",
                    row_id.as_bytes(),
                    lifecycle_record(row_id, None, Some(record_kind)),
                    &lifecycle_position(1),
                )?;
                if record_kind == LifecycleRecordKind::FilteredOut
                    && outcome != crate::IngestOutcome::Skipped
                {
                    return Err(anyhow::anyhow!(
                        "filtered append ingest returned {outcome:?}"
                    ));
                }
            }
            LifecycleTableKind::Missing => match self.engine.ingest_stream_record(
                &self.manifest,
                "missing",
                row_id.as_bytes(),
                lifecycle_record(row_id, None, Some(record_kind)),
            ) {
                Err(AnalyticsEngineError::TableNotRegistered(_)) => {
                    self.rejected_missing_table = true;
                }
                Err(err) => return Err(err.into()),
                Ok(_) => {
                    return Err(anyhow::anyhow!(
                        "missing-table ingest unexpectedly succeeded"
                    ));
                }
            },
        }
        self.last_sequence_after = self
            .production_snapshot()?
            .row_sequences
            .get(row_id)
            .copied()
            .unwrap_or(0);
        Ok(())
    }

    fn duplicate_insert(&mut self, row_id: &str) -> Result {
        self.begin_action(
            row_id,
            LifecycleTableKind::Mutable,
            LifecycleRecordKind::Matching,
        )?;
        self.last_append_row_existed_before = false;
        let position = lifecycle_position(self.authority_sequence("users", row_id)?.max(1));
        let record = lifecycle_record(row_id, None, Some(LifecycleRecordKind::Matching));
        let durable_position_known = self.last_row_sequences_before.contains_key(row_id);
        // Unknown ids are a no-op in the model. Exercise both real ingestion
        // calls in an isolated in-memory engine without nesting transactions or
        // mutating the model's live state.
        let isolated_engine = if durable_position_known {
            None
        } else {
            let engine = AnalyticsEngine::connect_duckdb(":memory:")?;
            engine.ensure_manifest(&self.manifest)?;
            Some(engine)
        };
        let engine = isolated_engine.as_ref().unwrap_or(&self.engine);
        let first = engine.ingest_stream_record_at_source_position(
            &self.manifest,
            "users",
            row_id.as_bytes(),
            record.clone(),
            &position,
        )?;
        let second = engine.ingest_stream_record_at_source_position(
            &self.manifest,
            "users",
            row_id.as_bytes(),
            record,
            &position,
        )?;
        let expected = if durable_position_known {
            (crate::IngestOutcome::Skipped, crate::IngestOutcome::Skipped)
        } else {
            (
                crate::IngestOutcome::Inserted,
                crate::IngestOutcome::Skipped,
            )
        };
        if (first, second) != expected {
            return Err(anyhow::anyhow!(
                "duplicate production outcomes were {first:?}, {second:?}, expected {expected:?}"
            ));
        }
        self.last_operation = if self.last_row_existed_before {
            LifecycleOperation::Duplicate
        } else {
            LifecycleOperation::NoOperation
        };
        self.last_sequence_after = self.last_sequence_before;
        Ok(())
    }

    fn update_record(
        &mut self,
        row_id: &str,
        new_seq: i64,
        new_kind: LifecycleRecordKind,
    ) -> Result {
        self.begin_action(row_id, LifecycleTableKind::Mutable, new_kind)?;
        self.last_append_row_existed_before = false;
        self.last_operation = LifecycleOperation::Update;
        self.last_incoming_sequence = new_seq;
        self.last_update_stale = new_seq <= self.last_sequence_before;
        let outcome = self.engine.ingest_stream_record_at_source_position(
            &self.manifest,
            "users",
            row_id.as_bytes(),
            lifecycle_record(
                row_id,
                Some(if self.last_row_existed_before {
                    LifecycleRecordKind::Matching
                } else {
                    LifecycleRecordKind::FilteredOut
                }),
                Some(new_kind),
            ),
            &lifecycle_position(new_seq),
        )?;
        if self.last_update_stale && outcome != crate::IngestOutcome::Skipped {
            return Err(anyhow::anyhow!(
                "stale production update returned {outcome:?}"
            ));
        }
        self.last_sequence_after = self
            .production_snapshot()?
            .row_sequences
            .get(row_id)
            .copied()
            .unwrap_or(0);
        Ok(())
    }

    fn delete_record(&mut self, row_id: &str, table_kind: LifecycleTableKind) -> Result {
        self.begin_action(row_id, table_kind, LifecycleRecordKind::Matching)?;
        self.last_operation = LifecycleOperation::Delete;
        match table_kind {
            LifecycleTableKind::Mutable => {
                let delete_position = self
                    .authority_sequence("users", row_id)?
                    .saturating_add(1)
                    .max(1);
                self.last_incoming_sequence = delete_position;
                let outcome = self.engine.ingest_stream_record_at_source_position(
                    &self.manifest,
                    "users",
                    row_id.as_bytes(),
                    lifecycle_record(row_id, Some(LifecycleRecordKind::Matching), None),
                    &lifecycle_position(delete_position),
                )?;
                if outcome != crate::IngestOutcome::Deleted {
                    return Err(anyhow::anyhow!(
                        "mutable delete returned {outcome:?}; expected Deleted"
                    ));
                }
            }
            LifecycleTableKind::AppendOnly => {
                let outcome = self.engine.ingest_stream_record(
                    &self.manifest,
                    "audit_events",
                    row_id.as_bytes(),
                    lifecycle_record(
                        row_id,
                        Some(if self.last_append_row_existed_before {
                            LifecycleRecordKind::Matching
                        } else {
                            LifecycleRecordKind::FilteredOut
                        }),
                        None,
                    ),
                )?;
                if outcome != crate::IngestOutcome::Skipped {
                    return Err(anyhow::anyhow!(
                        "append-only delete returned unexpected {outcome:?}"
                    ));
                }
            }
            LifecycleTableKind::Missing => match self.engine.ingest_stream_record(
                &self.manifest,
                "missing",
                row_id.as_bytes(),
                lifecycle_record(row_id, Some(LifecycleRecordKind::Matching), None),
            ) {
                Err(AnalyticsEngineError::TableNotRegistered(_)) => {
                    self.rejected_missing_table = true;
                }
                Err(err) => return Err(err.into()),
                Ok(_) => {
                    return Err(anyhow::anyhow!(
                        "missing-table delete unexpectedly succeeded"
                    ));
                }
            },
        }
        self.last_sequence_after = self
            .production_snapshot()?
            .row_sequences
            .get(row_id)
            .copied()
            .unwrap_or(0);
        Ok(())
    }

    fn production_snapshot(&self) -> Result<ProductionSnapshot> {
        let mut rows = BTreeMap::new();
        let mut append_rows = BTreeMap::new();
        let mut row_counts = BTreeMap::new();
        let mut row_sequences = BTreeMap::new();

        let metadata = self.engine.query_unscoped_sql_json(
            "select analytics_table_name, source_row_id, source_position_json, row_visible
             from __analytics_row_source_positions
             order by analytics_table_name, source_row_id",
        )?;
        for entry in metadata {
            let table = entry
                .get("analytics_table_name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    anyhow::anyhow!("production metadata table name was not a string: {entry}")
                })?;
            let source_row_id = entry
                .get("source_row_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    anyhow::anyhow!("production metadata source row id was not a string: {entry}")
                })?;
            if table != "users" {
                continue;
            }
            let row_id = decode_record_key(source_row_id)?;
            let visible = entry
                .get("row_visible")
                .and_then(serde_json::Value::as_bool)
                .ok_or_else(|| anyhow::anyhow!("production metadata row_visible was not bool"))?;
            rows.insert(row_id.clone(), visible);
            row_counts.insert(row_id.clone(), 0);
            row_sequences.insert(
                row_id,
                source_position_cursor(
                    entry
                        .get("source_position_json")
                        .ok_or_else(|| anyhow::anyhow!("missing production source position"))?,
                )?,
            );
        }

        for entry in self.engine.query_unscoped_sql_json(
            "select entity_id, count(*) as row_count from users group by entity_id order by \
             entity_id",
        )? {
            let Some(row_id) = entry.get("entity_id").and_then(|v| v.as_str()) else {
                continue;
            };
            let count = entry
                .get("row_count")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| anyhow::anyhow!("production user count was not integer"))?;
            rows.insert(row_id.to_string(), true);
            row_counts.insert(row_id.to_string(), count);
        }

        for entry in self.engine.query_unscoped_sql_json(
            "select entity_id, count(*) as row_count from audit_events group by entity_id order \
             by entity_id",
        )? {
            let Some(row_id) = entry.get("entity_id").and_then(|v| v.as_str()) else {
                continue;
            };
            let count = entry
                .get("row_count")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| anyhow::anyhow!("production audit count was not integer"))?;
            if count > 0 {
                append_rows.insert(row_id.to_string(), true);
            }
        }
        Ok(ProductionSnapshot {
            rows,
            append_rows,
            row_counts,
            row_sequences,
        })
    }

    fn authority_sequence(&self, table: &str, row_id: &str) -> Result<i64> {
        let rows = self.engine.query_unscoped_sql_json(&format!(
            "select source_position_json from __analytics_row_source_positions
             where analytics_table_name = '{}' and source_row_id = '{}' limit 1",
            table.replace('\'', "''"),
            record_key_hex(row_id).replace('\'', "''"),
        ))?;
        rows.first()
            .and_then(|row| row.get("source_position_json"))
            .map(source_position_cursor)
            .transpose()
            .map(|value| value.unwrap_or(0))
    }
}

fn source_position_cursor(value: &serde_json::Value) -> Result<i64> {
    let cursor = value
        .get("cursor")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("production source position has no cursor: {value}"))?;
    cursor
        .parse::<i64>()
        .map_err(|error| anyhow::anyhow!("invalid production source cursor {cursor:?}: {error}"))
}

fn record_key_hex(row_id: &str) -> String {
    row_id
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn decode_record_key(value: &str) -> Result<String> {
    if !value.len().is_multiple_of(2) {
        return Ok(value.to_string());
    }
    let bytes = value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|chunk| {
            let pair = std::str::from_utf8(chunk).map_err(|error| {
                anyhow::anyhow!("invalid production record-key encoding {value:?}: {error}")
            })?;
            u8::from_str_radix(pair, 16).map_err(|error| {
                anyhow::anyhow!("invalid production record-key encoding {value:?}: {error}")
            })
        })
        .collect::<Result<Vec<_>>>()?;
    String::from_utf8(bytes)
        .map_err(|error| anyhow::anyhow!("production record key was not utf-8: {error}"))
}

fn manifest() -> AnalyticsManifest {
    AnalyticsManifest::new(vec![
        TableRegistration {
            source_table_name: "tenant_01".to_string(),
            analytics_table_name: "users".to_string(),
            source_table_name_prefix: None,
            row_expansion: None,
            tenant_id: Some("tenant_01".to_string()),
            tenant_selector: TenantSelector::TableName,
            row_identity: RowIdentity::RecordKey,
            document_column: Some("item".to_string()),
            skip_delete: false,
            retention: None,
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            projection_attribute_names: None,
            projection_columns: Some(vec![
                projection_column("user_id", "user_id"),
                projection_column("email", "profile.email"),
            ]),
            columns: Vec::new(),
            partition_keys: Vec::new(),
            clustering_keys: Vec::new(),
            table_scope: analytics_contract::TableScope::default(),
            join_policy: analytics_contract::JoinPolicy::default(),
        },
        TableRegistration {
            source_table_name: "audit_stream".to_string(),
            analytics_table_name: "audit_events".to_string(),
            source_table_name_prefix: None,
            row_expansion: None,
            tenant_id: Some("tenant_01".to_string()),
            tenant_selector: TenantSelector::TableName,
            row_identity: RowIdentity::RecordKey,
            document_column: Some("item".to_string()),
            skip_delete: true,
            retention: None,
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            projection_attribute_names: None,
            projection_columns: Some(vec![
                projection_column("audit_id", "audit_id"),
                projection_column("action", "action"),
            ]),
            columns: Vec::new(),
            partition_keys: Vec::new(),
            clustering_keys: Vec::new(),
            table_scope: analytics_contract::TableScope::default(),
            join_policy: analytics_contract::JoinPolicy::default(),
        },
    ])
}

fn lifecycle_manifest() -> AnalyticsManifest {
    AnalyticsManifest::new(vec![
        lifecycle_table("shared_items", "users", "USER", false),
        lifecycle_table("shared_items", "audit_events", "USER", true),
    ])
}

fn lifecycle_table(
    source_table_name: &str,
    analytics_table_name: &str,
    entity_type: &str,
    skip_delete: bool,
) -> TableRegistration {
    TableRegistration {
        source_table_name: source_table_name.to_string(),
        analytics_table_name: analytics_table_name.to_string(),
        source_table_name_prefix: None,
        row_expansion: None,
        tenant_id: None,
        tenant_selector: TenantSelector::None,
        row_identity: RowIdentity::Attribute {
            attribute_name: "entity_id".to_string(),
        },
        document_column: Some("item".to_string()),
        skip_delete,
        retention: None,
        condition_expression: Some("#entity_type = :entity_type".to_string()),
        expression_attribute_names: Some(VecMap::from_iter([(
            "#entity_type".to_string(),
            "entity_type".to_string(),
        )])),
        expression_attribute_values: Some(VecMap::from_iter([(
            ":entity_type".to_string(),
            StorageValue::S(entity_type.to_string()),
        )])),
        projection_attribute_names: Some(vec!["entity_id".to_string(), "email".to_string()]),
        projection_columns: None,
        columns: Vec::new(),
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
        table_scope: analytics_contract::TableScope::default(),
        join_policy: analytics_contract::JoinPolicy::default(),
    }
}

fn lifecycle_record(
    row_id: &str,
    old_kind: Option<LifecycleRecordKind>,
    new_kind: Option<LifecycleRecordKind>,
) -> StorageStreamRecord {
    StorageStreamRecord {
        sequence_number: row_id.to_string(),
        keys: storage_key("ENTITY", row_id),
        old_image: old_kind.map(|kind| lifecycle_item(row_id, kind)),
        new_image: new_kind.map(|kind| lifecycle_item(row_id, kind)),
    }
}

fn lifecycle_position(sequence: i64) -> SourcePosition {
    SourcePosition::AuxStorageStreamCursor {
        stream_name: "lifecycle".to_string(),
        cursor: sequence.to_string(),
    }
}

fn lifecycle_item(row_id: &str, kind: LifecycleRecordKind) -> StorageItem {
    let entity_type = match kind {
        LifecycleRecordKind::Matching => "USER",
        LifecycleRecordKind::FilteredOut => "ORDER",
    };
    VecMap::from_iter([
        ("entity_id".to_string(), StorageValue::S(row_id.to_string())),
        (
            "entity_type".to_string(),
            StorageValue::S(entity_type.to_string()),
        ),
        (
            "email".to_string(),
            StorageValue::S(format!("{row_id}@example.com")),
        ),
    ])
}

fn projection_column(column_name: &str, attribute_path: &str) -> ProjectionColumn {
    ProjectionColumn {
        column_name: column_name.to_string(),
        attribute_path: attribute_path.to_string(),
        column_type: Some(AnalyticsColumnType::Primitive {
            primitive: PrimitiveColumnType::VarChar,
        }),
    }
}

fn storage_key(entity_name: &str, id: &str) -> StorageItem {
    VecMap::from_iter([(
        "pk".to_string(),
        StorageValue::S(format!("{entity_name}#{id}")),
    )])
}

fn user_item(user_id: &str, email: &str) -> StorageItem {
    VecMap::from_iter([
        ("user_id".to_string(), StorageValue::S(user_id.to_string())),
        (
            "profile".to_string(),
            StorageValue::M(VecMap::from_iter([(
                "email".to_string(),
                StorageValue::S(email.to_string()),
            )])),
        ),
    ])
}

fn audit_item(audit_id: &str, action_name: &str) -> StorageItem {
    VecMap::from_iter([
        (
            "audit_id".to_string(),
            StorageValue::S(audit_id.to_string()),
        ),
        (
            "action".to_string(),
            StorageValue::S(action_name.to_string()),
        ),
    ])
}
