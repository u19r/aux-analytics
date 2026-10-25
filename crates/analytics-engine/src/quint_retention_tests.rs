use analytics_contract::{
    AnalyticsColumnType, AnalyticsManifest, PrimitiveColumnType, ProjectionColumn, RetentionPolicy,
    RetentionTimestamp, RowIdentity, StorageStreamRecord, StorageValue, TableRegistration,
    TenantSelector,
};
use analytics_quint_test_support::map_result;
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;
use serde_json::Value;
use storage_types::VecMap;

use crate::{
    AnalyticsEngine, AnalyticsEngineError, IngestRetention, RetentionEffectAuthority,
    RetentionEffectDisposition, RetentionEffectRequest, RetentionEffectRequestInput,
    RetentionEffectScope,
};

const EVENTS_TABLE: &str = "events";
const CUTOFF_MS: i64 = 2_500;
const BATCH_SIZE: u64 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum RetentionResult {
    NotChecked,
    StaticExpiryResult,
    MissingMarkerResult,
    RepairRejectedResult,
    RepairAppliedResult,
    SweepCompleteResult,
    SweepIncompleteResult,
    StaleRejectedResult,
    ReplayAppliedResult,
    ConflictRejectedResult,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct RetentionState {
    last_result: RetentionResult,
    row_count: i64,
    missing_count: i64,
    static_expiry_exact: bool,
    deleted_count: i64,
    remaining_expired_count: i64,
    receipt_count: i64,
    complete: bool,
    authority_fence: i64,
}

impl State<RetentionDriver> for RetentionState {
    fn from_driver(driver: &RetentionDriver) -> Result<Self> {
        driver.observed_state()
    }
}

struct RetentionDriver {
    engine: AnalyticsEngine,
    manifest: AnalyticsManifest,
    last_result: RetentionResult,
}

impl Default for RetentionDriver {
    fn default() -> Self {
        let (engine, manifest) = fixture().expect("retention fixture");
        Self {
            engine,
            manifest,
            last_result: RetentionResult::NotChecked,
        }
    }
}

impl Driver for RetentionDriver {
    type State = RetentionState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.reset()?,
            StaticExpiry => self.static_expiry()?,
            MissingMarker => self.missing_marker()?,
            RepairWithoutPolicy => self.repair_without_policy()?,
            RepairWithPolicy => self.repair_with_policy()?,
            BoundedComplete => self.bounded_complete()?,
            BoundedIncomplete => self.bounded_incomplete()?,
            StaleAuthority => self.stale_authority()?,
            ReplayEffect => self.replay_effect()?,
            ConflictingPayload => self.conflicting_payload()?,
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/retention_mbt.qnt",
    max_samples = 50,
    max_steps = 10,
    seed = "0x4"
)]
fn given_quint_retention_model_when_replayed_then_engine_retention_matches() -> impl Driver {
    RetentionDriver::default()
}

impl RetentionDriver {
    fn reset(&mut self) -> Result {
        (self.engine, self.manifest) = fixture()?;
        self.last_result = RetentionResult::NotChecked;
        Ok(())
    }

    fn static_expiry(&mut self) -> Result {
        self.reset()?;
        ingest_event(&self.engine, &self.manifest, "event-1", "1000", None)?;
        if !static_expiry_exact(&self.engine)? {
            return Err(anyhow::anyhow!("static expiry observation was not exact"));
        }
        self.last_result = RetentionResult::StaticExpiryResult;
        Ok(())
    }

    fn missing_marker(&mut self) -> Result {
        self.reset()?;
        ingest_event(
            &self.engine,
            &self.manifest,
            "event-1",
            "1000",
            Some(&missing_retention()),
        )?;
        if row_counts(&self.engine)? != (1, 1) {
            return Err(anyhow::anyhow!(
                "missing retention marker was not persisted"
            ));
        }
        self.last_result = RetentionResult::MissingMarkerResult;
        Ok(())
    }

    fn repair_without_policy(&mut self) -> Result {
        self.reset()?;
        ingest_event(
            &self.engine,
            &self.manifest,
            "event-1",
            "1000",
            Some(&missing_retention()),
        )?;
        match self
            .engine
            .repair_missing_retention(EVENTS_TABLE, "tenant_01", 0, 100)
        {
            Err(AnalyticsEngineError::InvalidRetentionPeriod) => {}
            Ok(_) => return Err(anyhow::anyhow!("repair without policy was accepted")),
            Err(error) => return Err(error.into()),
        }
        if row_counts(&self.engine)? != (1, 1) || receipt_count(&self.engine)? != 0 {
            return Err(anyhow::anyhow!(
                "rejected repair changed the destination state"
            ));
        }
        self.last_result = RetentionResult::RepairRejectedResult;
        Ok(())
    }

    fn repair_with_policy(&mut self) -> Result {
        self.reset()?;
        ingest_event(
            &self.engine,
            &self.manifest,
            "event-1",
            "1000",
            Some(&missing_retention()),
        )?;
        let changed =
            self.engine
                .repair_missing_retention(EVENTS_TABLE, "tenant_01", 1_000, 100)?;
        if changed != 1 || row_counts(&self.engine)? != (1, 0) {
            return Err(anyhow::anyhow!("authorized repair was not applied"));
        }
        self.last_result = RetentionResult::RepairAppliedResult;
        Ok(())
    }

    fn bounded_complete(&mut self) -> Result {
        self.reset()?;
        ingest_event(&self.engine, &self.manifest, "event-1", "1000", None)?;
        let summary = apply_effect(&self.engine, 1, 1)?;
        if summary != (1, true) {
            return Err(anyhow::anyhow!("complete bounded sweep did not match"));
        }
        self.last_result = RetentionResult::SweepCompleteResult;
        Ok(())
    }

    fn bounded_incomplete(&mut self) -> Result {
        self.reset()?;
        for id in ["event-1", "event-2"] {
            ingest_event(&self.engine, &self.manifest, id, "1000", None)?;
        }
        let summary = apply_effect(&self.engine, 2, 1)?;
        if summary != (1, false) {
            return Err(anyhow::anyhow!("incomplete bounded sweep did not match"));
        }
        self.last_result = RetentionResult::SweepIncompleteResult;
        Ok(())
    }

    fn stale_authority(&mut self) -> Result {
        self.reset()?;
        ingest_event(&self.engine, &self.manifest, "event-1", "1000", None)?;
        let applied = apply_effect(&self.engine, 3, 1)?;
        if applied != (1, true) {
            return Err(anyhow::anyhow!("stale-fence fixture did not apply"));
        }
        let stale = apply_effect(&self.engine, 4, 1)?;
        if stale != (0, false) || row_counts(&self.engine)?.0 != 0 {
            return Err(anyhow::anyhow!("stale authority changed destination state"));
        }
        self.last_result = RetentionResult::StaleRejectedResult;
        Ok(())
    }

    fn replay_effect(&mut self) -> Result {
        self.reset()?;
        ingest_event(&self.engine, &self.manifest, "event-1", "1000", None)?;
        let first = apply_effect(&self.engine, 5, 1)?;
        let replay = apply_effect(&self.engine, 5, 1)?;
        if first != (1, true) || replay != (1, true) || receipt_count(&self.engine)? != 1 {
            return Err(anyhow::anyhow!("replayed effect was not idempotent"));
        }
        self.last_result = RetentionResult::ReplayAppliedResult;
        Ok(())
    }

    fn conflicting_payload(&mut self) -> Result {
        self.reset()?;
        ingest_event(&self.engine, &self.manifest, "event-1", "1000", None)?;
        let first = apply_effect(&self.engine, 6, 1)?;
        let conflict = conflicting_effect(&self.engine, 6, 2);
        if first != (1, true)
            || !matches!(
                conflict,
                Err(AnalyticsEngineError::ConflictingRetentionEffectReceipt)
            )
            || row_counts(&self.engine)?.0 != 0
            || receipt_count(&self.engine)? != 1
        {
            return Err(anyhow::anyhow!("conflicting payload changed state"));
        }
        self.last_result = RetentionResult::ConflictRejectedResult;
        Ok(())
    }

    fn observed_state(&self) -> Result<RetentionState> {
        let (row_count, missing_count) = row_counts(&self.engine)?;
        let receipt = latest_receipt(&self.engine)?;
        let (deleted_count, remaining_expired_count, complete) = match receipt {
            Some(receipt) => (
                receipt.rows_deleted,
                i64::try_from(
                    self.engine
                        .expired_retention_count(EVENTS_TABLE, receipt.cutoff_ms)?,
                )?,
                receipt.complete,
            ),
            None => (0, 0, false),
        };
        Ok(RetentionState {
            last_result: self.last_result,
            row_count,
            missing_count,
            static_expiry_exact: self.last_result == RetentionResult::StaticExpiryResult
                && static_expiry_exact(&self.engine)?,
            deleted_count,
            remaining_expired_count,
            receipt_count: receipt_count(&self.engine)?,
            complete,
            authority_fence: authority_fence(&self.engine)?,
        })
    }
}

#[derive(Clone, Copy)]
struct ReceiptObservation {
    cutoff_ms: i64,
    rows_deleted: i64,
    complete: bool,
}

fn apply_effect(engine: &AnalyticsEngine, execution_seed: u8, fence: u64) -> Result<(i64, bool)> {
    let request = RetentionEffectRequest::try_new(RetentionEffectRequestInput {
        authority: RetentionEffectAuthority::try_new(execution_id(execution_seed), fence)?,
        scope: RetentionEffectScope::try_new(EVENTS_TABLE)?,
        cutoff_ms: CUTOFF_MS,
        schedule_at_ms: CUTOFF_MS,
        server_time_ms: u64::try_from(CUTOFF_MS).expect("cutoff fits server time"),
        batch_size: BATCH_SIZE,
        cursor: None,
    })?;
    match engine.delete_expired_rows_fenced(request)? {
        RetentionEffectDisposition::Applied { summary }
        | RetentionEffectDisposition::AlreadyApplied { summary } => {
            Ok((i64::try_from(summary.rows_deleted())?, summary.complete()))
        }
        RetentionEffectDisposition::StaleFence => Ok((0, false)),
    }
}

fn conflicting_effect(
    engine: &AnalyticsEngine,
    execution_seed: u8,
    fence: u64,
) -> crate::AnalyticsEngineResult<RetentionEffectDisposition> {
    let request = RetentionEffectRequest::try_new(RetentionEffectRequestInput {
        authority: RetentionEffectAuthority::try_new(execution_id(execution_seed), fence)?,
        scope: RetentionEffectScope::try_new(EVENTS_TABLE)?,
        cutoff_ms: CUTOFF_MS,
        schedule_at_ms: CUTOFF_MS,
        server_time_ms: u64::try_from(CUTOFF_MS).expect("cutoff fits server time"),
        // Reclaims may increase the fence, but cannot change the batch
        // contents.
        batch_size: BATCH_SIZE + 1,
        cursor: None,
    })?;
    engine.delete_expired_rows_fenced(request)
}

fn static_expiry_exact(engine: &AnalyticsEngine) -> Result<bool> {
    let rows = engine.query_unscoped_sql_json(
        "SELECT __expiry, __missing_retention FROM events ORDER BY entity_id",
    )?;
    Ok(rows.len() == 1
        && rows[0]["__expiry"].as_i64() == Some(2_000)
        && rows[0]["__missing_retention"] == false)
}

fn row_counts(engine: &AnalyticsEngine) -> Result<(i64, i64)> {
    let rows = engine.query_unscoped_sql_json(
        "SELECT COUNT(*) AS row_count,
                COALESCE(SUM(CASE WHEN __missing_retention THEN 1 ELSE 0 END), 0)
                    AS missing_count
         FROM events",
    )?;
    let row = rows
        .first()
        .ok_or_else(|| anyhow::anyhow!("retention count query returned no row"))?;
    Ok((
        integer_field(row, "row_count")?,
        integer_field(row, "missing_count")?,
    ))
}

fn latest_receipt(engine: &AnalyticsEngine) -> Result<Option<ReceiptObservation>> {
    let rows = optional_internal_rows(
        engine,
        "SELECT cutoff_ms, rows_deleted, complete
         FROM __analytics_retention_effect_receipts
         WHERE table_name = 'events'
         ORDER BY CAST(fence AS BIGINT) DESC LIMIT 1",
    )?;
    rows.first()
        .map(|row| {
            Ok(ReceiptObservation {
                cutoff_ms: integer_field(row, "cutoff_ms")?,
                rows_deleted: integer_field(row, "rows_deleted")?,
                complete: bool_field(row, "complete")?,
            })
        })
        .transpose()
}

fn receipt_count(engine: &AnalyticsEngine) -> Result<i64> {
    let rows = optional_internal_rows(
        engine,
        "SELECT COUNT(*) AS count
         FROM __analytics_retention_effect_receipts
         WHERE table_name = 'events'",
    )?;
    rows.first()
        .map(|row| integer_field(row, "count"))
        .transpose()
        .map(|count| count.unwrap_or(0))
}

fn authority_fence(engine: &AnalyticsEngine) -> Result<i64> {
    let rows = optional_internal_rows(
        engine,
        "SELECT fence
         FROM __analytics_retention_effect_authority
         WHERE table_name = 'events'
         LIMIT 2",
    )?;
    match rows.as_slice() {
        [] => Ok(0),
        [row] => {
            let value = row
                .get("fence")
                .and_then(Value::as_str)
                .and_then(|value| value.parse().ok())
                .or_else(|| row.get("fence").and_then(Value::as_i64))
                .ok_or_else(|| anyhow::anyhow!("retention fence was not numeric"))?;
            Ok(value)
        }
        _ => Err(anyhow::anyhow!("retention authority is ambiguous")),
    }
}

fn optional_internal_rows(engine: &AnalyticsEngine, query: &str) -> Result<Vec<Value>> {
    match engine.query_unscoped_sql_json(query) {
        Ok(rows) => Ok(rows),
        Err(error) if error.to_string().contains("does not exist") => Ok(Vec::new()),
        Err(error) => Err(error.into()),
    }
}

fn integer_field(row: &Value, name: &str) -> Result<i64> {
    row.get(name)
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow::anyhow!("retention field {name} was not an integer"))
}

fn bool_field(row: &Value, name: &str) -> Result<bool> {
    row.get(name)
        .and_then(Value::as_bool)
        .ok_or_else(|| anyhow::anyhow!("retention field {name} was not boolean"))
}

fn execution_id(seed: u8) -> [u8; 16] {
    let mut execution_id = [0; 16];
    execution_id[0] = seed;
    execution_id
}

fn fixture() -> Result<(AnalyticsEngine, AnalyticsManifest)> {
    let engine = AnalyticsEngine::connect_duckdb(":memory:")?;
    let manifest = AnalyticsManifest::new(vec![retained_events_table()]);
    engine.ensure_manifest(&manifest)?;
    engine.authorize_retention_schedule_occurrence(
        EVENTS_TABLE,
        CUTOFF_MS,
        CUTOFF_MS,
        u64::try_from(CUTOFF_MS).expect("cutoff fits server time"),
    )?;
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
            EVENTS_TABLE,
            event_id.as_bytes(),
            StorageStreamRecord {
                sequence_number: event_id.to_string(),
                keys: VecMap::new(),
                old_image: None,
                new_image: Some(event_item(event_id, created_at_ms)),
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

fn retained_events_table() -> TableRegistration {
    TableRegistration {
        source_table_name: "shared_items".to_string(),
        analytics_table_name: EVENTS_TABLE.to_string(),
        source_table_name_prefix: None,
        row_expansion: None,
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
        table_scope: analytics_contract::TableScope::default(),
        join_policy: analytics_contract::JoinPolicy::default(),
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

fn event_item(event_id: &str, created_at_ms: &str) -> VecMap<String, StorageValue> {
    VecMap::from_iter([
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
