use analytics_contract::{PrivacyPolicy, PrivacyPolicyMatcher, TableRegistration};
use duckdb::arrow::{
    array::{Array, BooleanArray, StringArray},
    record_batch::RecordBatch,
};

use super::{
    AnalyticsEngine, AnalyticsEngineError, AnalyticsEngineResult, PrivacyRowIdentity,
    PrivacyTableRemediationMode, PrivacyTableRemediationReport, filter_json_value,
    json_value_to_duckdb_value, privacy_remediation_scan_sql,
    privacy_scrubbable_projection_columns, transaction::EngineTransaction,
};

impl AnalyticsEngine {
    pub fn scrub_table_with_privacy_policy(
        &self,
        table: &TableRegistration,
        policy: &PrivacyPolicy,
        dry_run: bool,
    ) -> AnalyticsEngineResult<PrivacyTableRemediationReport> {
        self.remediate_table_with_privacy_policy(
            table,
            policy,
            dry_run,
            PrivacyTableRemediationMode::Scrub,
        )
    }

    /// One native scan fixes the selection snapshot, including duplicate row
    /// identities. Mutations use a separate connection in one transaction;
    /// any scan or mutation failure rolls back all effects. DuckDB may reject
    /// concurrent conflicting writes. The scan retains only the current native
    /// Arrow chunk and one decoded document, independently of response budgets.
    pub fn remediate_table_with_privacy_policy(
        &self,
        table: &TableRegistration,
        policy: &PrivacyPolicy,
        dry_run: bool,
        mode: PrivacyTableRemediationMode,
    ) -> AnalyticsEngineResult<PrivacyTableRemediationReport> {
        self.refresh_object_storage_credentials_if_needed()?;
        let mut remediation = PrivacyRemediation::try_new(table, policy, dry_run, mode)?;
        let sql = privacy_remediation_scan_sql(
            &table.analytics_table_name,
            &remediation.projection_columns,
            table.document_column.as_deref(),
        );
        // A streaming result must keep its own connection: executing writes on
        // that connection would terminate its active native query.
        let scan_connection = self.clone_privacy_scan_connection()?;
        let transaction = EngineTransaction::begin(self)?;
        let mut statement = scan_connection.prepare(&sql)?;
        let mut scan = statement.stream_arrow([])?;
        let result = (|| {
            while let Some(batch) = PrivacyRemediation::fetch_batch(&mut scan)? {
                remediation.apply_batch(self, &batch)?;
            }
            remediation.report.destination_mutated =
                !dry_run && remediation.report.affected_rows > 0;
            Ok(remediation.report)
        })();
        drop(scan);
        drop(statement);
        transaction.finish(result)
    }

    fn clone_privacy_scan_connection(&self) -> AnalyticsEngineResult<duckdb::Connection> {
        let search_path: String =
            self.conn
                .query_row("SELECT current_setting('search_path')", [], |row| {
                    row.get(0)
                })?;
        let connection = self.conn.try_clone()?;
        connection.execute("SET search_path = ?", [&search_path])?;
        Ok(connection)
    }
}

struct PrivacyRemediation<'a, 'policy> {
    table: &'a TableRegistration,
    matcher: PrivacyPolicyMatcher<'policy>,
    projection_columns: Vec<&'a str>,
    report: PrivacyTableRemediationReport,
}

impl<'a, 'policy> PrivacyRemediation<'a, 'policy> {
    fn try_new(
        table: &'a TableRegistration,
        policy: &'policy PrivacyPolicy,
        dry_run: bool,
        mode: PrivacyTableRemediationMode,
    ) -> AnalyticsEngineResult<Self> {
        let matcher = policy.validated()?;
        let projection_columns = privacy_scrubbable_projection_columns(table, &matcher);
        Ok(Self {
            table,
            matcher,
            projection_columns,
            report: PrivacyTableRemediationReport {
                table_name: table.analytics_table_name.clone(),
                dry_run,
                mode,
                scanned_rows: 0,
                affected_rows: 0,
                projected_values_scrubbed: 0,
                document_values_scrubbed: 0,
                destination_mutated: false,
                policy_version: policy.version.clone(),
            },
        })
    }

    fn fetch_batch(
        scan: &mut duckdb::ArrowStream<'_>,
    ) -> AnalyticsEngineResult<Option<RecordBatch>> {
        // The dependency exposes late fetch failures as panics rather than a
        // fallible iterator. Preserve their message as a maintenance scan
        // error.
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| scan.next())).map_err(|error| {
            let message = error
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| error.downcast_ref::<&str>().copied());
            match message {
                Some(message) => {
                    AnalyticsEngineError::Io(std::io::Error::other(message.to_owned()))
                }
                None => std::panic::resume_unwind(error),
            }
        })
    }

    fn column<T: Array + 'static>(batch: &RecordBatch, index: usize) -> AnalyticsEngineResult<&T> {
        batch.column(index).as_any().downcast_ref().ok_or_else(|| {
            AnalyticsEngineError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "privacy scan column {index} has type {}, expected {}",
                    batch.column(index).data_type(),
                    std::any::type_name::<T>()
                ),
            ))
        })
    }

    fn apply_batch(
        &mut self,
        engine: &AnalyticsEngine,
        batch: &RecordBatch,
    ) -> AnalyticsEngineResult<()> {
        let identities = [
            Self::column::<StringArray>(batch, 0)?,
            Self::column::<StringArray>(batch, 1)?,
            Self::column::<StringArray>(batch, 2)?,
        ];
        let projections = (3..3 + self.projection_columns.len())
            .map(|index| Self::column::<BooleanArray>(batch, index))
            .collect::<AnalyticsEngineResult<Vec<_>>>()?;
        let documents = self
            .table
            .document_column
            .as_deref()
            .map(|column| {
                Self::column::<StringArray>(batch, 3 + projections.len())
                    .map(|values| (column, values))
            })
            .transpose()?;
        for index in 0..batch.num_rows() {
            self.report.scanned_rows += 1;
            if identities.iter().any(|column| column.is_null(index)) {
                continue;
            }
            let identity = PrivacyRowIdentity {
                table_name: identities[0].value(index),
                tenant_id: identities[1].value(index),
                row_id: identities[2].value(index),
            };
            let assignments = self.collect_assignments(index, &projections, documents)?;
            if assignments.is_empty() {
                continue;
            }
            self.report.affected_rows += 1;
            if self.report.dry_run {
                continue;
            }
            match self.report.mode {
                PrivacyTableRemediationMode::Scrub => engine.update_privacy_scrubbed_row(
                    &self.table.analytics_table_name,
                    identity,
                    assignments,
                )?,
                PrivacyTableRemediationMode::DeleteRows => engine
                    .delete_privacy_violating_row(&self.table.analytics_table_name, identity)?,
            }
        }
        Ok(())
    }

    fn collect_assignments(
        &mut self,
        index: usize,
        projections: &[&BooleanArray],
        documents: Option<(&'a str, &StringArray)>,
    ) -> AnalyticsEngineResult<storage_types::VecMap<&'a str, duckdb::types::Value>> {
        let mut assignments = storage_types::VecMap::new();
        for (column, values) in self.projection_columns.iter().zip(projections) {
            if values.value(index) {
                assignments.insert(*column, duckdb::types::Value::Null);
                self.report.projected_values_scrubbed += 1;
            }
        }
        if let Some((column, documents)) = documents
            && !documents.is_null(index)
        {
            let document = serde_json::from_str(documents.value(index))?;
            let mut dropped = 0;
            let filtered = filter_json_value(document, "", &self.matcher, &mut dropped);
            if dropped > 0 {
                assignments.insert(column, json_value_to_duckdb_value(&filtered));
                self.report.document_values_scrubbed += dropped;
            }
        }
        Ok(assignments)
    }
}
