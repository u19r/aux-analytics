use std::path::{Path, PathBuf};

use duckdb::{Connection, params};

use crate::{
    ExecutionAttempt, OperationActor, OperationId, OperationStore, OperationStoreResult,
    store::OperationReportKind,
};

/// Test-owned access to the public operation-store boundary.
///
/// The production store deliberately exposes only lifecycle operations and
/// atomic report/effect commits. Tests that need a deterministic lease or a
/// report-only fixture use this type instead of adding test methods to
/// `OperationStore`.
#[derive(Clone)]
pub(crate) struct OperationStoreTestFixture {
    database_path: Option<PathBuf>,
}

impl OperationStoreTestFixture {
    pub(crate) const fn new() -> Self {
        Self {
            database_path: None,
        }
    }

    pub(crate) fn for_database(path: impl AsRef<Path>) -> Self {
        Self {
            database_path: Some(path.as_ref().to_path_buf()),
        }
    }

    pub(crate) fn prepare(
        &self,
        store: &OperationStore,
        operation_id: &OperationId,
        actor: &OperationActor,
    ) -> OperationStoreResult<ExecutionAttempt> {
        store.prepare_for_execution(operation_id, actor)
    }

    pub(crate) fn prepare_at(
        &self,
        store: &OperationStore,
        operation_id: &OperationId,
        actor: &OperationActor,
        now_ms: i64,
    ) -> OperationStoreResult<ExecutionAttempt> {
        let current = store.show_operation(operation_id)?;
        if current.execution_owner.is_some() && current.execution_lease_until_ms <= now_ms {
            self.expire_lease(operation_id)?;
        }
        let attempt = self.prepare(store, operation_id, actor)?;
        if self.database_path.is_some() {
            self.set_lease_until(operation_id, now_ms.saturating_add(300_000))?;
        }
        Ok(attempt)
    }

    pub(crate) fn transition(
        &self,
        store: &OperationStore,
        operation_id: &OperationId,
        transition: crate::OperationTransition<'_>,
    ) -> OperationStoreResult<()> {
        let expected = store.show_operation(operation_id)?;
        store.transition_with_counts_from(&expected, transition)
    }

    pub(crate) fn save_report<T: serde::Serialize>(
        &self,
        operation_id: &OperationId,
        report_kind: OperationReportKind,
        report: &T,
    ) -> OperationStoreResult<()> {
        let path = self
            .database_path
            .as_deref()
            .expect("report fixtures require a file-backed operation store");
        let conn = Connection::open(path)?;
        conn.execute(
            "INSERT OR REPLACE INTO __analytics_operation_reports
                (operation_id, report_kind, report_json, created_at_ms)
             VALUES (?, ?, ?, now_ms())",
            params![
                operation_id.as_str(),
                report_kind_name(report_kind),
                serde_json::to_string(report)?
            ],
        )?;
        Ok(())
    }

    fn expire_lease(&self, operation_id: &OperationId) -> OperationStoreResult<()> {
        self.set_lease_until(operation_id, 0)
    }

    fn set_lease_until(
        &self,
        operation_id: &OperationId,
        lease_until_ms: i64,
    ) -> OperationStoreResult<()> {
        let path = self
            .database_path
            .as_deref()
            .expect("lease fixtures require a file-backed operation store");
        let conn = Connection::open(path)?;
        conn.execute_batch("BEGIN TRANSACTION")?;
        conn.execute(
            "UPDATE __analytics_operations
             SET execution_lease_until_ms = ?
             WHERE operation_id = ?",
            params![lease_until_ms, operation_id.as_str()],
        )?;
        conn.execute_batch("COMMIT")?;
        conn.execute_batch("CHECKPOINT")?;
        Ok(())
    }
}

fn report_kind_name(report_kind: OperationReportKind) -> &'static str {
    match report_kind {
        OperationReportKind::Backfill => "backfill",
        OperationReportKind::Check => "check",
        OperationReportKind::TableFix => "table_fix",
        OperationReportKind::PrivacyFix => "privacy_fix",
        OperationReportKind::Trim => "trim",
        OperationReportKind::RawBackup => "raw_backup",
    }
}
