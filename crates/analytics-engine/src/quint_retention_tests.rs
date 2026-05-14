use std::collections::{BTreeMap, HashMap};

use analytics_contract::{
    AnalyticsColumnType, AnalyticsManifest, PrimitiveColumnType, ProjectionColumn, RetentionPolicy,
    RetentionTimestamp, RowIdentity, StorageStreamRecord, StorageValue, TableRegistration,
    TenantSelector,
};
use analytics_quint_test_support::map_result;
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::{AnalyticsEngine, AnalyticsEngineError, IngestRetention};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum RetentionResult {
    NotChecked,
    StaticExpiryComputed,
    MissingRetentionMarked,
    MissingRetentionRepaired,
    SweepDeletedExpiredOnly,
    SweepBatchLimitRespected,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct RetentionState {
    #[serde(rename = "lastResult")]
    last_result: RetentionResult,
}

impl State<RetentionDriver> for RetentionState {
    fn from_driver(driver: &RetentionDriver) -> Result<Self> {
        Ok(Self {
            last_result: driver.last_result,
        })
    }
}

struct RetentionDriver {
    last_result: RetentionResult,
}

impl Default for RetentionDriver {
    fn default() -> Self {
        Self {
            last_result: RetentionResult::NotChecked,
        }
    }
}

impl Driver for RetentionDriver {
    type State = RetentionState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.last_result = RetentionResult::NotChecked,
            ComputeStaticExpiry => {
                self.last_result = compute_static_expiry()?;
            },
            MarkMissingRetention => {
                self.last_result = mark_missing_retention()?;
            },
            RepairMissingRetention => {
                self.last_result = repair_missing_retention()?;
            },
            SweepExpiredRows => {
                self.last_result = sweep_expired_rows()?;
            },
            SweepHonorsBatchLimit => {
                self.last_result = sweep_honors_batch_limit()?;
            },
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/retention.qnt",
    max_samples = 50,
    max_steps = 8,
    seed = "0x4"
)]
fn quint_retention_model_matches_engine_retention() -> impl Driver {
    RetentionDriver::default()
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
#[allow(clippy::struct_excessive_bools)]
struct RetentionBughuntState {
    row_expiry: BTreeMap<String, i64>,
    row_missing: BTreeMap<String, bool>,
    row_deleted: BTreeMap<String, bool>,
    row_has_policy: BTreeMap<String, bool>,
    row_tenant: BTreeMap<String, String>,
    deleted_this_sweep: i64,
    repaired_without_policy: bool,
    deleted_unexpired: bool,
    batch_limit_exceeded: bool,
}

impl State<RetentionBughuntDriver> for RetentionBughuntState {
    fn from_driver(driver: &RetentionBughuntDriver) -> Result<Self> {
        Ok(Self {
            row_expiry: driver.row_expiry.clone(),
            row_missing: driver.row_missing.clone(),
            row_deleted: driver.row_deleted.clone(),
            row_has_policy: driver.row_has_policy.clone(),
            row_tenant: driver.row_tenant.clone(),
            deleted_this_sweep: driver.deleted_this_sweep,
            repaired_without_policy: driver.repaired_without_policy,
            deleted_unexpired: driver.deleted_unexpired,
            batch_limit_exceeded: driver.batch_limit_exceeded,
        })
    }
}

#[derive(Default)]
#[allow(clippy::struct_excessive_bools)]
struct RetentionBughuntDriver {
    row_expiry: BTreeMap<String, i64>,
    row_missing: BTreeMap<String, bool>,
    row_deleted: BTreeMap<String, bool>,
    row_has_policy: BTreeMap<String, bool>,
    row_tenant: BTreeMap<String, String>,
    deleted_this_sweep: i64,
    repaired_without_policy: bool,
    deleted_unexpired: bool,
    batch_limit_exceeded: bool,
}

impl Driver for RetentionBughuntDriver {
    type State = RetentionBughuntState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            InsertPresentRetention(row_id, tenant_id, expiry) => {
                let row_id: String = row_id;
                let tenant_id: String = tenant_id;
                self.insert_present_retention(row_id.as_str(), tenant_id.as_str(), expiry)?;
            },
            InsertMissingRetention(row_id, tenant_id, has_policy) => {
                let row_id: String = row_id;
                let tenant_id: String = tenant_id;
                self.insert_missing_retention(row_id.as_str(), tenant_id.as_str(), has_policy)?;
            },
            RepairMissingRetention(row_id) => {
                let row_id: String = row_id;
                self.repair_missing_retention(row_id.as_str())?;
            },
            SweepOneCandidate(row_id, now_ms, batch_limit) => {
                let row_id: String = row_id;
                self.sweep_one_candidate(row_id.as_str(), now_ms, batch_limit)?;
            },
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/retention_bughunt.qnt",
    max_samples = 100,
    max_steps = 12,
    seed = "0x7d5"
)]
fn quint_retention_bughunt_model_matches_engine_boundaries() -> impl Driver {
    RetentionBughuntDriver::default()
}

impl RetentionBughuntDriver {
    fn insert_present_retention(&mut self, row_id: &str, tenant_id: &str, expiry: i64) -> Result {
        probe_present_retention(row_id, tenant_id, expiry)?;
        self.row_expiry.insert(row_id.to_string(), expiry);
        self.row_missing.insert(row_id.to_string(), false);
        self.row_deleted.insert(row_id.to_string(), false);
        self.row_has_policy.insert(row_id.to_string(), true);
        self.row_tenant
            .insert(row_id.to_string(), tenant_id.to_string());
        Ok(())
    }

    fn insert_missing_retention(
        &mut self,
        row_id: &str,
        tenant_id: &str,
        has_policy: bool,
    ) -> Result {
        probe_missing_retention(row_id, tenant_id)?;
        self.row_expiry.insert(row_id.to_string(), 0);
        self.row_missing.insert(row_id.to_string(), true);
        self.row_deleted.insert(row_id.to_string(), false);
        self.row_has_policy.insert(row_id.to_string(), has_policy);
        self.row_tenant
            .insert(row_id.to_string(), tenant_id.to_string());
        Ok(())
    }

    fn repair_missing_retention(&mut self, row_id: &str) -> Result {
        let has_policy = self.row_has_policy.get(row_id).copied().unwrap_or(false);
        let tenant_id = self
            .row_tenant
            .get(row_id)
            .cloned()
            .unwrap_or_else(|| "tenant-a".to_string());
        if has_policy {
            probe_repair_with_policy(row_id, tenant_id.as_str())?;
            self.row_expiry.insert(row_id.to_string(), 3_000);
            self.row_missing.insert(row_id.to_string(), false);
        } else {
            self.repaired_without_policy = self.repaired_without_policy
                || probe_repair_without_policy_cleared_row(row_id, tenant_id.as_str())?;
        }
        Ok(())
    }

    fn sweep_one_candidate(&mut self, row_id: &str, now_ms: i64, batch_limit: i64) -> Result {
        let expiry = self.row_expiry.get(row_id).copied().unwrap_or(0);
        let deletion_allowed = batch_limit > 0 && expiry <= now_ms;
        let deleted = probe_sweep_candidate(row_id, expiry, now_ms, batch_limit)?;
        if deletion_allowed {
            self.row_deleted.insert(row_id.to_string(), true);
        }
        self.deleted_this_sweep = i64::from(deletion_allowed);
        self.deleted_unexpired = self.deleted_unexpired || (deleted && expiry > now_ms);
        self.batch_limit_exceeded =
            self.batch_limit_exceeded || self.deleted_this_sweep > batch_limit;
        Ok(())
    }
}

fn compute_static_expiry() -> Result<RetentionResult> {
    let (engine, manifest) = engine_with_retention_manifest()?;
    ingest_event(&engine, &manifest, "event-1", "1000", None)?;
    let rows =
        engine.query_unscoped_sql_json("select __expiry, __missing_retention from events")?;
    if rows.len() == 1 && rows[0]["__expiry"] == 2000 && rows[0]["__missing_retention"] == false {
        Ok(RetentionResult::StaticExpiryComputed)
    } else {
        Err(anyhow::anyhow!("static expiry did not match model"))
    }
}

fn mark_missing_retention() -> Result<RetentionResult> {
    let (engine, manifest) = engine_with_retention_manifest()?;
    let retention = missing_retention();
    ingest_event(&engine, &manifest, "event-1", "1000", Some(&retention))?;
    let missing = engine.missing_retention_count("events")?;
    let rows =
        engine.query_unscoped_sql_json("select __expiry, __missing_retention from events")?;
    if missing == 1
        && rows.len() == 1
        && rows[0]["__expiry"].is_null()
        && rows[0]["__missing_retention"] == true
    {
        Ok(RetentionResult::MissingRetentionMarked)
    } else {
        Err(anyhow::anyhow!("missing retention row did not match model"))
    }
}

fn repair_missing_retention() -> Result<RetentionResult> {
    let (engine, manifest) = engine_with_retention_manifest()?;
    let retention = missing_retention();
    ingest_event(&engine, &manifest, "event-1", "1000", Some(&retention))?;
    let repaired = engine.repair_missing_retention("events", "tenant_01", 1_000, 100)?;
    let rows =
        engine.query_unscoped_sql_json("select __expiry, __missing_retention from events")?;
    if repaired == 1
        && rows.len() == 1
        && rows[0]["__expiry"]
            .as_i64()
            .is_some_and(|expiry| expiry > 0)
        && rows[0]["__missing_retention"] == false
    {
        Ok(RetentionResult::MissingRetentionRepaired)
    } else {
        Err(anyhow::anyhow!("retention repair did not match model"))
    }
}

fn sweep_expired_rows() -> Result<RetentionResult> {
    let (engine, manifest) = engine_with_retention_manifest()?;
    ingest_event(&engine, &manifest, "event-1", "1000", None)?;
    ingest_event(&engine, &manifest, "event-2", "5000", None)?;
    let deleted = engine.delete_expired_rows("events", 2_500, 100)?;
    let rows = engine.query_unscoped_sql_json("select entity_id from events order by entity_id")?;
    if deleted == 1 && rows.len() == 1 && rows[0]["entity_id"] == "event-2" {
        Ok(RetentionResult::SweepDeletedExpiredOnly)
    } else {
        Err(anyhow::anyhow!("retention sweep did not match model"))
    }
}

fn sweep_honors_batch_limit() -> Result<RetentionResult> {
    let (engine, manifest) = engine_with_retention_manifest()?;
    ingest_event(&engine, &manifest, "event-1", "1000", None)?;
    ingest_event(&engine, &manifest, "event-2", "1200", None)?;
    let deleted = engine.delete_expired_rows("events", 2_500, 1)?;
    let rows = engine.query_unscoped_sql_json("select count(*) as count from events")?;
    if deleted == 1 && rows.first().and_then(|row| row["count"].as_i64()) == Some(1) {
        Ok(RetentionResult::SweepBatchLimitRespected)
    } else {
        Err(anyhow::anyhow!(
            "retention sweep batch limit did not match model"
        ))
    }
}

fn probe_present_retention(row_id: &str, tenant_id: &str, expiry: i64) -> Result {
    let (engine, manifest) = engine_with_retention_bughunt_manifest()?;
    let retention = IngestRetention {
        period_ms: Some(u64::try_from(expiry)?),
        timestamp: RetentionTimestamp::Attribute {
            attribute_path: "created_at_ms".to_string(),
        },
        missing_retention: false,
    };
    ingest_tenant_event(&engine, &manifest, row_id, tenant_id, "0", Some(&retention))?;
    let rows = engine.query_unscoped_sql_json(
        format!("select __expiry, __missing_retention from events where entity_id = '{row_id}'")
            .as_str(),
    )?;
    if rows.len() == 1 && rows[0]["__expiry"] == expiry && rows[0]["__missing_retention"] == false {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "present retention row did not match bughunt model"
        ))
    }
}

fn probe_missing_retention(row_id: &str, tenant_id: &str) -> Result {
    let (engine, manifest) = engine_with_retention_bughunt_manifest()?;
    let retention = missing_retention();
    ingest_tenant_event(&engine, &manifest, row_id, tenant_id, "0", Some(&retention))?;
    let rows = engine.query_unscoped_sql_json(
        format!("select __expiry, __missing_retention from events where entity_id = '{row_id}'")
            .as_str(),
    )?;
    if rows.len() == 1 && rows[0]["__expiry"].is_null() && rows[0]["__missing_retention"] == true {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "missing retention row did not match bughunt model"
        ))
    }
}

fn probe_repair_with_policy(row_id: &str, tenant_id: &str) -> Result {
    let (engine, manifest) = engine_with_retention_bughunt_manifest()?;
    let retention = missing_retention();
    ingest_tenant_event(&engine, &manifest, row_id, tenant_id, "0", Some(&retention))?;
    let repaired = engine.repair_missing_retention("events", tenant_id, 3_000, 100)?;
    let rows = engine.query_unscoped_sql_json(
        format!("select __expiry, __missing_retention from events where entity_id = '{row_id}'")
            .as_str(),
    )?;
    if repaired == 1
        && rows.len() == 1
        && rows[0]["__expiry"].as_i64().is_some_and(|value| value > 0)
        && rows[0]["__missing_retention"] == false
    {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "valid retention repair did not match bughunt model"
        ))
    }
}

fn probe_repair_without_policy_cleared_row(row_id: &str, tenant_id: &str) -> Result<bool> {
    let (engine, manifest) = engine_with_retention_bughunt_manifest()?;
    let retention = missing_retention();
    ingest_tenant_event(&engine, &manifest, row_id, tenant_id, "0", Some(&retention))?;
    match engine.repair_missing_retention("events", tenant_id, 0, 100) {
        Err(AnalyticsEngineError::InvalidRetentionPeriod) => Ok(false),
        Err(error) => Err(error.into()),
        Ok(_) => {
            let rows = engine.query_unscoped_sql_json(
                format!("select __missing_retention from events where entity_id = '{row_id}'")
                    .as_str(),
            )?;
            Ok(rows.len() == 1 && rows[0]["__missing_retention"] == false)
        }
    }
}

fn probe_sweep_candidate(row_id: &str, expiry: i64, now_ms: i64, batch_limit: i64) -> Result<bool> {
    let (engine, manifest) = engine_with_retention_bughunt_manifest()?;
    let retention = IngestRetention {
        period_ms: Some(u64::try_from(expiry.max(0))?),
        timestamp: RetentionTimestamp::Attribute {
            attribute_path: "created_at_ms".to_string(),
        },
        missing_retention: false,
    };
    ingest_tenant_event(
        &engine,
        &manifest,
        row_id,
        "tenant-a",
        "0",
        Some(&retention),
    )?;
    ingest_tenant_event(
        &engine,
        &manifest,
        "unexpired-control",
        "tenant-a",
        "0",
        Some(&IngestRetention {
            period_ms: Some(u64::try_from(now_ms + 10_000)?),
            timestamp: RetentionTimestamp::Attribute {
                attribute_path: "created_at_ms".to_string(),
            },
            missing_retention: false,
        }),
    )?;
    let deleted =
        engine.delete_expired_rows("events", now_ms, u64::try_from(batch_limit.max(0))?)?;
    let rows = engine.query_unscoped_sql_json("select entity_id from events order by entity_id")?;
    let candidate_deleted = !rows.iter().any(|row| row["entity_id"] == row_id);
    let unexpired_deleted = !rows
        .iter()
        .any(|row| row["entity_id"] == "unexpired-control");
    if unexpired_deleted {
        return Err(anyhow::anyhow!("retention sweep deleted unexpired row"));
    }
    if deleted > u64::try_from(batch_limit.max(0))? {
        return Err(anyhow::anyhow!("retention sweep exceeded batch limit"));
    }
    Ok(candidate_deleted)
}

fn engine_with_retention_manifest() -> Result<(AnalyticsEngine, AnalyticsManifest)> {
    let engine = AnalyticsEngine::connect_duckdb(":memory:")?;
    let manifest = AnalyticsManifest::new(vec![retained_events_table()]);
    engine.ensure_manifest(&manifest)?;
    Ok((engine, manifest))
}

fn engine_with_retention_bughunt_manifest() -> Result<(AnalyticsEngine, AnalyticsManifest)> {
    let engine = AnalyticsEngine::connect_duckdb(":memory:")?;
    let manifest = AnalyticsManifest::new(vec![tenant_retained_events_table()]);
    engine.ensure_manifest(&manifest)?;
    Ok((engine, manifest))
}

fn ingest_event(
    engine: &AnalyticsEngine,
    manifest: &AnalyticsManifest,
    event_id: &str,
    created_at_ms: &str,
    retention: Option<&IngestRetention>,
) -> Result {
    map_result(
        engine.ingest_stream_record_with_retention(
            manifest,
            "events",
            event_id.as_bytes(),
            StorageStreamRecord {
                sequence_number: event_id.to_string(),
                keys: HashMap::new(),
                old_image: None,
                new_image: Some(event_item(event_id, created_at_ms)),
            },
            retention,
        ),
        |_| Ok(()),
        |error| Err(error.into()),
    )
}

fn ingest_tenant_event(
    engine: &AnalyticsEngine,
    manifest: &AnalyticsManifest,
    event_id: &str,
    tenant_id: &str,
    created_at_ms: &str,
    retention: Option<&IngestRetention>,
) -> Result {
    map_result(
        engine.ingest_stream_record_with_retention(
            manifest,
            "events",
            event_id.as_bytes(),
            StorageStreamRecord {
                sequence_number: event_id.to_string(),
                keys: HashMap::new(),
                old_image: None,
                new_image: Some(tenant_event_item(event_id, tenant_id, created_at_ms)),
            },
            retention,
        ),
        |_| Ok(()),
        |error| Err(error.into()),
    )
}

fn missing_retention() -> IngestRetention {
    IngestRetention {
        period_ms: None,
        timestamp: RetentionTimestamp::IngestedAt,
        missing_retention: true,
    }
}

fn tenant_retained_events_table() -> TableRegistration {
    let mut table = retained_events_table();
    table.tenant_id = None;
    table.tenant_selector = TenantSelector::Attribute {
        attribute_name: "tenant_id".to_string(),
    };
    table.projection_columns = Some(vec![projection_column("entity_id", "entity_id")]);
    table
}

fn retained_events_table() -> TableRegistration {
    TableRegistration {
        source_table_name: "shared_items".to_string(),
        analytics_table_name: "events".to_string(),
        source_table_name_prefix: None,
        tenant_id: Some("tenant_01".to_string()),
        tenant_selector: TenantSelector::TableName,
        row_identity: RowIdentity::RecordKey,
        document_column: Some("item".to_string()),
        skip_delete: false,
        retention: Some(RetentionPolicy {
            period_ms: 1_000,
            timestamp: RetentionTimestamp::Attribute {
                attribute_path: "created_at_ms".to_string(),
            },
        }),
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        projection_attribute_names: None,
        projection_columns: Some(vec![projection_column("entity_id", "entity_id")]),
        columns: Vec::new(),
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
    }
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

fn event_item(event_id: &str, created_at_ms: &str) -> HashMap<String, StorageValue> {
    HashMap::from([
        (
            "entity_id".to_string(),
            StorageValue::S(event_id.to_string()),
        ),
        (
            "created_at_ms".to_string(),
            StorageValue::N(created_at_ms.to_string()),
        ),
    ])
}

fn tenant_event_item(
    event_id: &str,
    tenant_id: &str,
    created_at_ms: &str,
) -> HashMap<String, StorageValue> {
    let mut item = event_item(event_id, created_at_ms);
    item.insert(
        "tenant_id".to_string(),
        StorageValue::S(tenant_id.to_string()),
    );
    item
}
