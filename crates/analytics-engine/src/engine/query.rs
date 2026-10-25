use std::{
    io::{self, Write},
    time::Duration,
};

use analytics_contract::{AnalyticsManifest, MAX_STRUCTURED_QUERY_BOUND, StructuredQuery};

use super::{
    AnalyticsEngine, AnalyticsEngineError, AnalyticsEngineResult,
    query_execution::QueryExecutionWatchdog, query_validation::validate_read_only_query,
};
use crate::{
    constants::{
        DEFAULT_QUERY_TIMEOUT, JSON_ARRAY_FRAMING_BYTES, JSON_ARRAY_SEPARATOR_BYTES,
        MAX_QUERY_RESULT_BYTES, MAX_QUERY_RESULT_ROWS,
    },
    structured_query::{
        PreparedStructuredQuery, prepare_tenant_structured_query, prepare_unscoped_structured_query,
    },
};

impl AnalyticsEngine {
    pub fn query_unscoped_sql_json(
        &self,
        sql: &str,
    ) -> AnalyticsEngineResult<Vec<serde_json::Value>> {
        self.query_unscoped_sql_json_with_timeout(sql, DEFAULT_QUERY_TIMEOUT)
    }

    pub fn query_unscoped_sql_json_without_timeout(
        &self,
        sql: &str,
    ) -> AnalyticsEngineResult<Vec<serde_json::Value>> {
        self.query_unscoped_sql_json_with_optional_timeout(sql, None)
    }

    pub fn query_unscoped_sql_json_with_timeout(
        &self,
        sql: &str,
        timeout: Duration,
    ) -> AnalyticsEngineResult<Vec<serde_json::Value>> {
        self.query_unscoped_sql_json_with_optional_timeout(sql, Some(timeout))
    }

    fn query_unscoped_sql_json_with_optional_timeout(
        &self,
        sql: &str,
        timeout: Option<Duration>,
    ) -> AnalyticsEngineResult<Vec<serde_json::Value>> {
        self.query_sql_json_with_optional_timeout(sql, &[], timeout, None)
    }

    pub(super) fn query_sql_json_with_optional_timeout(
        &self,
        sql: &str,
        parameters: &[duckdb::types::Value],
        timeout: Option<Duration>,
        capacity_hint: Option<usize>,
    ) -> AnalyticsEngineResult<Vec<serde_json::Value>> {
        self.materialize_query_sql_json(
            sql,
            parameters,
            timeout,
            capacity_hint,
            JsonByteCounter::count_json_bytes,
        )
        .map(|result| result.values)
    }

    #[cfg(test)]
    pub(crate) fn performance_query_sql_json_baseline(
        &self,
        sql: &str,
    ) -> AnalyticsEngineResult<(Vec<serde_json::Value>, usize)> {
        self.materialize_query_sql_json(sql, &[], None, None, |value| {
            serde_json::to_vec(value).map(|bytes| bytes.len())
        })
        .map(|result| (result.values, result.encoded_bytes))
    }

    #[cfg(test)]
    pub(crate) fn performance_query_sql_json_candidate(
        &self,
        sql: &str,
    ) -> AnalyticsEngineResult<(Vec<serde_json::Value>, usize)> {
        self.materialize_query_sql_json(sql, &[], None, None, JsonByteCounter::count_json_bytes)
            .map(|result| (result.values, result.encoded_bytes))
    }

    fn materialize_query_sql_json(
        &self,
        sql: &str,
        parameters: &[duckdb::types::Value],
        timeout: Option<Duration>,
        capacity_hint: Option<usize>,
        mut serialized_json_length: impl FnMut(&serde_json::Value) -> Result<usize, serde_json::Error>,
    ) -> AnalyticsEngineResult<QueryMaterialization> {
        self.refresh_object_storage_credentials_if_needed()?;
        let statement = validate_read_only_query(sql)?;
        if timeout.is_some_and(|timeout| timeout.is_zero()) {
            return Err(AnalyticsEngineError::QueryTimeout { timeout_ms: 0 });
        }
        let wrapped = format!(
            "SELECT to_json(struct_pack(*COLUMNS(*))) FROM ({}) AS result",
            statement
        );
        let timed_out = QueryExecutionWatchdog::start(
            &self.conn,
            timeout,
            self.query_cancellation.borrow().clone(),
        )?;
        let query_error = |error: duckdb::Error| {
            timed_out
                .as_ref()
                .and_then(QueryExecutionWatchdog::elapsed_error)
                .unwrap_or_else(|| error.into())
        };
        let mut stmt = self.conn.prepare(wrapped.as_str()).map_err(query_error)?;
        let mut rows = stmt
            .query(duckdb::params_from_iter(parameters.iter()))
            .map_err(query_error)?;

        let mut values = capacity_hint.map_or_else(Vec::new, Vec::with_capacity);
        let mut result_budget = QueryResultBudget::new();
        while let Some(row) = rows.next().map_err(query_error)? {
            result_budget.ensure_row_available()?;
            let value = QueryMaterialization::parse_row(row).map_err(query_error)?;
            let row_bytes =
                serialized_json_length(&value).map_err(|_| duckdb::Error::InvalidColumnIndex(0))?;
            result_budget.reserve_row(row_bytes)?;
            values.push(value);
        }
        if let Some(error) = timed_out
            .as_ref()
            .and_then(QueryExecutionWatchdog::elapsed_error)
        {
            return Err(error);
        }

        Ok(QueryMaterialization {
            values,
            #[cfg(test)]
            encoded_bytes: result_budget.encoded_bytes,
        })
    }

    pub fn query_prepared_structured_json(
        &self,
        query: &PreparedStructuredQuery,
    ) -> AnalyticsEngineResult<Vec<serde_json::Value>> {
        self.query_sql_json_with_optional_timeout(
            query.sql.as_str(),
            query.parameters.as_slice(),
            Some(DEFAULT_QUERY_TIMEOUT),
            query
                .metadata()
                .limit
                .map(|limit| limit.min(MAX_STRUCTURED_QUERY_BOUND) as usize),
        )
    }

    pub fn query_prepared_structured_json_without_timeout(
        &self,
        query: &PreparedStructuredQuery,
    ) -> AnalyticsEngineResult<Vec<serde_json::Value>> {
        self.query_prepared_structured_json_with_optional_timeout(query, None)
    }

    fn query_prepared_structured_json_with_optional_timeout(
        &self,
        query: &PreparedStructuredQuery,
        timeout: Option<Duration>,
    ) -> AnalyticsEngineResult<Vec<serde_json::Value>> {
        self.query_sql_json_with_optional_timeout(
            query.sql.as_str(),
            query.parameters.as_slice(),
            timeout,
            query
                .metadata()
                .limit
                .map(|limit| limit.min(MAX_STRUCTURED_QUERY_BOUND) as usize),
        )
    }

    pub fn query_unscoped_structured_json(
        &self,
        manifest: &AnalyticsManifest,
        query: &StructuredQuery,
    ) -> AnalyticsEngineResult<Vec<serde_json::Value>> {
        let query = prepare_unscoped_structured_query(manifest, query)?;
        self.query_prepared_structured_json(&query)
    }

    pub fn query_tenant_structured_json(
        &self,
        manifest: &AnalyticsManifest,
        query: &StructuredQuery,
        target_tenant_id: &str,
    ) -> AnalyticsEngineResult<Vec<serde_json::Value>> {
        let query = prepare_tenant_structured_query(manifest, query, target_tenant_id)?;
        self.query_prepared_structured_json(&query)
    }
}

struct QueryMaterialization {
    values: Vec<serde_json::Value>,
    #[cfg(test)]
    encoded_bytes: usize,
}

impl QueryMaterialization {
    fn parse_row(row: &duckdb::Row<'_>) -> Result<serde_json::Value, duckdb::Error> {
        let text = row.get_ref(0)?;
        let text = text.as_str().map_err(|error| {
            duckdb::Error::FromSqlConversionFailure(0, duckdb::types::Type::Text, Box::new(error))
        })?;
        serde_json::from_str(text).map_err(|_| duckdb::Error::InvalidColumnIndex(0))
    }
}

#[cfg(test)]
pub(crate) fn performance_serialized_json_length(
    value: &serde_json::Value,
) -> Result<usize, serde_json::Error> {
    JsonByteCounter::count_json_bytes(value)
}

#[derive(Default)]
struct JsonByteCounter {
    bytes: usize,
}

impl JsonByteCounter {
    fn count_json_bytes(value: &serde_json::Value) -> Result<usize, serde_json::Error> {
        let mut counter = JsonByteCounter::default();
        serde_json::to_writer(&mut counter, value)?;
        Ok(counter.bytes)
    }
}

impl Write for JsonByteCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes = self.bytes.saturating_add(bytes.len());
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct QueryResultBudget {
    row_count: usize,
    encoded_bytes: usize,
}

impl QueryResultBudget {
    const fn new() -> Self {
        Self {
            row_count: 0,
            encoded_bytes: JSON_ARRAY_FRAMING_BYTES,
        }
    }

    fn ensure_row_available(&self) -> AnalyticsEngineResult<()> {
        if self.row_count >= MAX_QUERY_RESULT_ROWS {
            return Err(AnalyticsEngineError::QueryResultTooLarge {
                // Keep row exhaustion on the existing public resource-limit
                // error so API callers retain the established error code.
                actual_bytes: MAX_QUERY_RESULT_BYTES.saturating_add(1),
                maximum_bytes: MAX_QUERY_RESULT_BYTES,
            });
        }
        Ok(())
    }

    fn reserve_row(&mut self, row_bytes: usize) -> AnalyticsEngineResult<()> {
        let separator_bytes = usize::from(self.row_count > 0) * JSON_ARRAY_SEPARATOR_BYTES;
        let encoded_bytes = self
            .encoded_bytes
            .saturating_add(separator_bytes)
            .saturating_add(row_bytes);
        if encoded_bytes > MAX_QUERY_RESULT_BYTES {
            return Err(AnalyticsEngineError::QueryResultTooLarge {
                actual_bytes: encoded_bytes,
                maximum_bytes: MAX_QUERY_RESULT_BYTES,
            });
        }
        self.row_count += 1;
        self.encoded_bytes = encoded_bytes;
        Ok(())
    }
}
