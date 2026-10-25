use super::{
    AnalyticsEngine, AnalyticsEngineResult, IngestOutcome, immediate_ingest::StreamRecordIngest,
};

impl AnalyticsEngine {
    pub(super) fn ingest_record_atomically(
        &self,
        request: StreamRecordIngest<'_>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        let mut transaction = EngineTransaction::begin(self)?;
        let result = transaction.ingest_record(request);
        transaction.finish(result)
    }
}

/// Mutations enter through a transaction that has successfully begun. Dropping
/// an unfinished transaction rolls it back, including during unwinding.
pub(super) struct EngineTransaction<'a> {
    engine: &'a AnalyticsEngine,
    row_writes: crate::batch_writer::BatchSqlExecutor<'a>,
    active: bool,
}

impl<'a> EngineTransaction<'a> {
    pub(super) fn begin(engine: &'a AnalyticsEngine) -> AnalyticsEngineResult<Self> {
        engine.refresh_object_storage_credentials_if_needed()?;
        engine.conn.execute_batch("BEGIN TRANSACTION")?;
        Ok(Self {
            engine,
            row_writes: crate::batch_writer::BatchSqlExecutor::new(&engine.conn),
            active: true,
        })
    }

    pub(super) fn begin_fenced(engine: &'a AnalyticsEngine) -> AnalyticsEngineResult<Self> {
        let transaction = Self::begin(engine)?;
        engine.destination_fence.enable(&engine.conn)?;
        Ok(transaction)
    }

    pub(super) fn ingest_record(
        &mut self,
        request: StreamRecordIngest<'_>,
    ) -> AnalyticsEngineResult<IngestOutcome> {
        if let Some(position) = request.execution.authority.position() {
            position.validate()?;
        }
        let table = self
            .engine
            .registered_table(request.manifest, request.analytics_table_name)?;
        if let Some((_, plan)) = request.execution.authority.owner()
            && !plan.allows_ingest()?
        {
            return Ok(IngestOutcome::Skipped);
        }
        self.engine
            .apply_registered_stream_record(table, request, &mut self.row_writes)
    }

    pub(super) fn commit(mut self) -> AnalyticsEngineResult<()> {
        self.row_writes.flush()?;
        self.engine.conn.execute_batch("COMMIT")?;
        self.active = false;
        Ok(())
    }

    pub(super) fn rollback(mut self) -> AnalyticsEngineResult<()> {
        self.engine.conn.execute_batch("ROLLBACK")?;
        self.active = false;
        Ok(())
    }

    pub(super) fn finish<T>(self, result: AnalyticsEngineResult<T>) -> AnalyticsEngineResult<T> {
        let value = result?;
        self.commit()?;
        Ok(value)
    }
}

impl Drop for EngineTransaction<'_> {
    fn drop(&mut self) {
        if self.active {
            let _ = self.engine.conn.execute_batch("ROLLBACK");
        }
    }
}
