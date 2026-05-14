use std::collections::{BTreeMap, HashMap};

use analytics_contract::{
    AnalyticsColumnType, AnalyticsManifest, PrimitiveColumnType, ProjectionColumn, RowIdentity,
    StorageStreamRecord, StorageValue, TableRegistration, TenantSelector,
};
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

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
fn quint_ingestion_model_matches_engine() -> impl Driver {
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
#[allow(clippy::struct_excessive_bools)]
struct LifecycleState {
    rows: BTreeMap<String, bool>,
    append_rows: BTreeMap<String, bool>,
    rejected_missing_table: bool,
    missing_table_accepted: bool,
    stale_overwrite: bool,
    filtered_row_retained: bool,
    append_delete_removed: bool,
    duplicate_created_extra_row: bool,
}

impl State<LifecycleDriver> for LifecycleState {
    fn from_driver(driver: &LifecycleDriver) -> Result<Self> {
        Ok(Self {
            rows: driver.rows.clone(),
            append_rows: driver.append_rows.clone(),
            rejected_missing_table: driver.rejected_missing_table,
            missing_table_accepted: driver.missing_table_accepted,
            stale_overwrite: driver.stale_overwrite,
            filtered_row_retained: driver.filtered_row_retained,
            append_delete_removed: driver.append_delete_removed,
            duplicate_created_extra_row: driver.duplicate_created_extra_row,
        })
    }
}

#[allow(clippy::struct_excessive_bools)]
struct LifecycleDriver {
    engine: AnalyticsEngine,
    manifest: AnalyticsManifest,
    rows: BTreeMap<String, bool>,
    append_rows: BTreeMap<String, bool>,
    rejected_missing_table: bool,
    missing_table_accepted: bool,
    stale_overwrite: bool,
    filtered_row_retained: bool,
    append_delete_removed: bool,
    duplicate_created_extra_row: bool,
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
            rows: BTreeMap::new(),
            append_rows: BTreeMap::new(),
            rejected_missing_table: false,
            missing_table_accepted: false,
            stale_overwrite: false,
            filtered_row_retained: false,
            append_delete_removed: false,
            duplicate_created_extra_row: false,
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
            UpdateRecord(row_id, old_seq, new_seq, new_kind) => {
                let row_id: String = row_id;
                self.update_record(row_id.as_str(), old_seq, new_seq, new_kind)?;
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
fn quint_ingestion_lifecycle_bughunt_matches_engine() -> impl Driver {
    LifecycleDriver::default()
}

impl LifecycleDriver {
    fn ingest_record(
        &mut self,
        row_id: &str,
        table_kind: LifecycleTableKind,
        record_kind: LifecycleRecordKind,
    ) -> Result {
        match table_kind {
            LifecycleTableKind::Mutable => {
                self.engine.ingest_stream_record(
                    &self.manifest,
                    "users",
                    row_id.as_bytes(),
                    lifecycle_record(row_id, None, Some(record_kind)),
                )?;
                if record_kind == LifecycleRecordKind::Matching {
                    self.rows
                        .insert(row_id.to_string(), self.mutable_row_exists(row_id)?);
                }
            }
            LifecycleTableKind::AppendOnly => {
                self.engine.ingest_stream_record(
                    &self.manifest,
                    "audit_events",
                    row_id.as_bytes(),
                    lifecycle_record(row_id, None, Some(record_kind)),
                )?;
                if record_kind == LifecycleRecordKind::Matching {
                    self.append_rows
                        .insert(row_id.to_string(), self.append_row_exists(row_id)?);
                }
            }
            LifecycleTableKind::Missing => {}
        }
        Ok(())
    }

    fn duplicate_insert(&mut self, row_id: &str) -> Result {
        self.engine.ingest_stream_record(
            &self.manifest,
            "users",
            row_id.as_bytes(),
            lifecycle_record(row_id, None, Some(LifecycleRecordKind::Matching)),
        )?;
        self.engine.ingest_stream_record(
            &self.manifest,
            "users",
            row_id.as_bytes(),
            lifecycle_record(row_id, None, Some(LifecycleRecordKind::Matching)),
        )?;
        let row_count = self.mutable_row_count(row_id)?;
        self.rows.insert(row_id.to_string(), row_count > 0);
        self.duplicate_created_extra_row = self.duplicate_created_extra_row || row_count > 1;
        Ok(())
    }

    fn update_record(
        &mut self,
        row_id: &str,
        old_seq: i64,
        new_seq: i64,
        new_kind: LifecycleRecordKind,
    ) -> Result {
        self.engine.ingest_stream_record(
            &self.manifest,
            "users",
            row_id.as_bytes(),
            lifecycle_record(row_id, Some(LifecycleRecordKind::Matching), Some(new_kind)),
        )?;
        let exists = self.mutable_row_exists(row_id)?;
        self.rows.insert(row_id.to_string(), exists);
        let _ = (old_seq, new_seq);
        self.filtered_row_retained =
            self.filtered_row_retained || (new_kind == LifecycleRecordKind::FilteredOut && exists);
        Ok(())
    }

    fn delete_record(&mut self, row_id: &str, table_kind: LifecycleTableKind) -> Result {
        match table_kind {
            LifecycleTableKind::Mutable => {
                self.engine.ingest_stream_record(
                    &self.manifest,
                    "users",
                    row_id.as_bytes(),
                    lifecycle_record(row_id, Some(LifecycleRecordKind::Matching), None),
                )?;
                self.rows
                    .insert(row_id.to_string(), self.mutable_row_exists(row_id)?);
            }
            LifecycleTableKind::AppendOnly => {
                self.engine.ingest_stream_record(
                    &self.manifest,
                    "audit_events",
                    row_id.as_bytes(),
                    lifecycle_record(row_id, Some(LifecycleRecordKind::Matching), None),
                )?;
                let exists = self.append_row_exists(row_id)?;
                self.append_delete_removed = self.append_delete_removed
                    || (self.append_rows.get(row_id).copied().unwrap_or(false) && !exists);
                if exists {
                    self.append_rows.insert(row_id.to_string(), true);
                }
            }
            LifecycleTableKind::Missing => {
                self.apply_missing_table_delete(row_id)?;
            }
        }
        Ok(())
    }

    fn apply_missing_table_delete(&mut self, row_id: &str) -> Result {
        match self.engine.ingest_stream_record(
            &self.manifest,
            "missing",
            row_id.as_bytes(),
            lifecycle_record(row_id, Some(LifecycleRecordKind::Matching), None),
        ) {
            Err(AnalyticsEngineError::TableNotRegistered(_)) => {
                self.rejected_missing_table = true;
                Ok(())
            }
            Err(err) => Err(err.into()),
            Ok(_) => {
                self.missing_table_accepted = true;
                Ok(())
            }
        }
    }

    fn mutable_row_exists(&self, row_id: &str) -> Result<bool> {
        Ok(self.mutable_row_count(row_id)? > 0)
    }

    fn mutable_row_count(&self, row_id: &str) -> Result<usize> {
        let rows = self.engine.query_unscoped_sql_json(
            format!("select entity_id from users where entity_id = '{row_id}'").as_str(),
        )?;
        Ok(rows.len())
    }

    fn append_row_exists(&self, row_id: &str) -> Result<bool> {
        let rows = self.engine.query_unscoped_sql_json(
            format!("select entity_id from audit_events where entity_id = '{row_id}'").as_str(),
        )?;
        Ok(!rows.is_empty())
    }
}

fn manifest() -> AnalyticsManifest {
    AnalyticsManifest::new(vec![
        TableRegistration {
            source_table_name: "tenant_01".to_string(),
            analytics_table_name: "users".to_string(),
            source_table_name_prefix: None,
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
        },
        TableRegistration {
            source_table_name: "audit_stream".to_string(),
            analytics_table_name: "audit_events".to_string(),
            source_table_name_prefix: None,
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
        tenant_id: None,
        tenant_selector: TenantSelector::None,
        row_identity: RowIdentity::Attribute {
            attribute_name: "entity_id".to_string(),
        },
        document_column: Some("item".to_string()),
        skip_delete,
        retention: None,
        condition_expression: Some("#entity_type = :entity_type".to_string()),
        expression_attribute_names: Some(HashMap::from([(
            "#entity_type".to_string(),
            "entity_type".to_string(),
        )])),
        expression_attribute_values: Some(BTreeMap::from([(
            ":entity_type".to_string(),
            StorageValue::S(entity_type.to_string()),
        )])),
        projection_attribute_names: Some(vec!["entity_id".to_string(), "email".to_string()]),
        projection_columns: None,
        columns: Vec::new(),
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
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

fn lifecycle_item(row_id: &str, kind: LifecycleRecordKind) -> HashMap<String, StorageValue> {
    let entity_type = match kind {
        LifecycleRecordKind::Matching => "USER",
        LifecycleRecordKind::FilteredOut => "ORDER",
    };
    HashMap::from([
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

fn storage_key(entity_name: &str, id: &str) -> HashMap<String, StorageValue> {
    HashMap::from([(
        "pk".to_string(),
        StorageValue::S(format!("{entity_name}#{id}")),
    )])
}

fn user_item(user_id: &str, email: &str) -> HashMap<String, StorageValue> {
    HashMap::from([
        ("user_id".to_string(), StorageValue::S(user_id.to_string())),
        (
            "profile".to_string(),
            StorageValue::M(HashMap::from([(
                "email".to_string(),
                StorageValue::S(email.to_string()),
            )])),
        ),
    ])
}

fn audit_item(audit_id: &str, action_name: &str) -> HashMap<String, StorageValue> {
    HashMap::from([
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
