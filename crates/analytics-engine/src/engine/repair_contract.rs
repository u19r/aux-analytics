use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
};

use super::{AnalyticsEngineError, AnalyticsEngineResult, IngestOutcome};
use crate::row::batch::BatchRow;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RepairReceipt {
    pub repair_key: String,
    pub analytics_table_name: String,
    pub source_table_name: String,
    pub tenant_id: String,
    pub source_row_id: String,
    pub source_revision: String,
    pub expected_row_json: String,
    pub row_visible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RepairEffect {
    pub execution_id: String,
    pub fence: String,
    pub repair_key: String,
    pub analytics_table_name: String,
    pub source_revision: String,
    pub expected_row_json: String,
    pub row_visible: bool,
    pub consumed: bool,
}

impl RepairEffect {
    pub(super) fn validate_authority(
        &self,
        authority: &crate::maintenance_effect::MaintenanceEffectAuthority,
    ) -> AnalyticsEngineResult<()> {
        if self.execution_id != authority.execution_id() {
            return Err(AnalyticsEngineError::ConflictingMaintenanceEffectReceipt(
                self.repair_key.clone(),
            ));
        }
        let fence = self.fence.parse::<u64>().map_err(|_| {
            AnalyticsEngineError::AmbiguousMaintenanceEffectReceipt(self.repair_key.clone())
        })?;
        if fence > authority.fence().get() {
            return Err(AnalyticsEngineError::StaleMaintenanceEffectFence(
                authority.execution_id().to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn as_receipt(&self) -> AnalyticsEngineResult<RepairReceipt> {
        let row =
            serde_json::from_str::<BTreeMap<String, serde_json::Value>>(&self.expected_row_json)?;
        RepairReceipt::from_row(
            &self.repair_key,
            &self.analytics_table_name,
            &self.source_revision,
            &row,
            self.row_visible,
        )
    }

    pub(crate) fn matches_request(
        &self,
        analytics_table_name: &str,
        source_revision: &str,
        old_row: Option<&BTreeMap<String, serde_json::Value>>,
        new_row: Option<&BTreeMap<String, serde_json::Value>>,
    ) -> bool {
        self.analytics_table_name == analytics_table_name
            && self.source_revision == source_revision
            && if self.row_visible {
                new_row
                    .is_some_and(|row| repair_row_matches_exact_json(&self.expected_row_json, row))
            } else {
                old_row
                    .is_some_and(|row| repair_row_matches_exact_json(&self.expected_row_json, row))
            }
    }
}

pub(crate) struct RepairReceiptFileGuard<'a> {
    pub file: &'a File,
}

impl Drop for RepairReceiptFileGuard<'_> {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

impl RepairReceipt {
    pub(crate) fn from_row(
        repair_key: &str,
        analytics_table_name: &str,
        source_revision: &str,
        row: &BTreeMap<String, serde_json::Value>,
        row_visible: bool,
    ) -> AnalyticsEngineResult<Self> {
        let source_table_name = row
            .get("table_name")
            .and_then(serde_json::Value::as_str)
            .ok_or(AnalyticsEngineError::MissingIdentifier("table_name"))?;
        let tenant_id = row
            .get("tenant_id")
            .and_then(serde_json::Value::as_str)
            .ok_or(AnalyticsEngineError::MissingIdentifier("tenant_id"))?;
        let source_row_id = row
            .get("__id")
            .and_then(serde_json::Value::as_str)
            .ok_or(AnalyticsEngineError::MissingIdentifier("__id"))?;
        Ok(Self {
            repair_key: repair_key.to_string(),
            analytics_table_name: analytics_table_name.to_string(),
            source_table_name: source_table_name.to_string(),
            tenant_id: tenant_id.to_string(),
            source_row_id: source_row_id.to_string(),
            source_revision: source_revision.to_string(),
            expected_row_json: serde_json::to_string(row)?,
            row_visible,
        })
    }

    pub(crate) fn matches_row(&self, row: &BTreeMap<String, serde_json::Value>) -> bool {
        row.get("table_name").and_then(serde_json::Value::as_str)
            == Some(self.source_table_name.as_str())
            && row.get("tenant_id").and_then(serde_json::Value::as_str)
                == Some(self.tenant_id.as_str())
            && row.get("__id").and_then(serde_json::Value::as_str)
                == Some(self.source_row_id.as_str())
            && repair_row_matches_exact_json(&self.expected_row_json, row)
    }
}

pub(crate) fn validate_repair_receipt_inputs(
    repair_key: &str,
    source_revision: &str,
) -> AnalyticsEngineResult<()> {
    if repair_key.trim().is_empty() {
        return Err(AnalyticsEngineError::EmptyRepairReceiptKey);
    }
    if source_revision.trim().is_empty() {
        return Err(AnalyticsEngineError::EmptyRepairSourceRevision);
    }
    Ok(())
}

pub(crate) fn repair_row_matches_json(expected_json: &str, actual: &serde_json::Value) -> bool {
    let Ok(expected) = serde_json::from_str::<BTreeMap<String, serde_json::Value>>(expected_json)
    else {
        return false;
    };
    let Some(actual) = actual.as_object() else {
        return false;
    };
    expected
        .iter()
        .all(|(column, value)| actual.get(column).is_some_and(|actual| actual == value))
}

pub(crate) fn repair_json_values_equal(left: &str, right: &str) -> bool {
    let Ok(left) = serde_json::from_str::<serde_json::Value>(left) else {
        return false;
    };
    let Ok(right) = serde_json::from_str::<serde_json::Value>(right) else {
        return false;
    };
    left == right
}

pub(crate) fn repair_row_matches_exact_json(
    expected_json: &str,
    actual: &BTreeMap<String, serde_json::Value>,
) -> bool {
    serde_json::from_str::<BTreeMap<String, serde_json::Value>>(expected_json)
        .is_ok_and(|expected| expected == *actual)
}

pub(crate) struct ExpandedStreamRows {
    pub(crate) old: Vec<BatchRow>,
    pub(crate) new: Vec<BatchRow>,
    pub(crate) ignored: Vec<BatchRow>,
    pub(crate) new_identities: BTreeSet<(String, String, String)>,
    pub(crate) old_matched: bool,
}

pub(crate) fn unique_expanded_row_identities(
    rows: &[BatchRow],
) -> AnalyticsEngineResult<BTreeSet<(String, String, String)>> {
    let mut identities = BTreeSet::new();
    for row in rows {
        if !identities.insert(expanded_row_identity(row)?) {
            return Err(AnalyticsEngineError::DuplicateExpandedRowIdentity);
        }
    }
    Ok(identities)
}

pub(crate) fn expanded_row_identity(
    row: &BatchRow,
) -> AnalyticsEngineResult<(String, String, String)> {
    let identity = row.identity()?;
    Ok((
        identity.table_name.to_owned(),
        identity.tenant_id.to_owned(),
        identity.row_id.to_owned(),
    ))
}

pub(crate) fn combine_expanded_outcomes(outcomes: &[IngestOutcome]) -> IngestOutcome {
    let inserted = outcomes.contains(&IngestOutcome::Inserted);
    let updated = outcomes.contains(&IngestOutcome::Updated);
    let deleted = outcomes.contains(&IngestOutcome::Deleted);
    match (inserted, updated, deleted) {
        (false, false, false) => IngestOutcome::Skipped,
        (true, false, false) => IngestOutcome::Inserted,
        (false, false, true) => IngestOutcome::Deleted,
        _ => IngestOutcome::Updated,
    }
}
