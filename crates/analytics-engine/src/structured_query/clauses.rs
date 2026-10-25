//! Append ordered query clauses into the final SQL buffer.

use super::*;

impl QueryCompiler<'_> {
    pub(super) fn append_select_and_joins(
        &self,
        sql: &mut String,
        query: &StructuredQuery,
        target_tenant_id: Option<&str>,
    ) -> AnalyticsEngineResult<()> {
        Self::append_sql_fragments(
            sql,
            "",
            ", ",
            query.select.iter().map(|select| self.select_sql(select)),
        )?;
        sql.push_str(" FROM ");
        sql.push_str(&self.source_table_sql()?);
        for join in &query.joins {
            sql.push(' ');
            sql.push_str(&self.join_sql(join, target_tenant_id)?);
        }
        Ok(())
    }

    pub(super) fn append_query_filters(
        &self,
        sql: &mut String,
        query: &StructuredQuery,
        target_tenant_id: Option<&str>,
    ) -> AnalyticsEngineResult<()> {
        let mut filters = query
            .filters
            .iter()
            .map(|predicate| self.predicate_sql(predicate))
            .collect::<AnalyticsEngineResult<Vec<_>>>()?;
        filters.extend(self.tenant_filter_sql(self.primary_table()?, target_tenant_id)?);
        Self::append_sql_fragments(sql, " WHERE ", " AND ", filters.into_iter().map(Ok))
    }

    pub(super) fn tenant_filter_sql(
        &self,
        table: &QueryTable<'_>,
        tenant: Option<&str>,
    ) -> AnalyticsEngineResult<Option<String>> {
        let Some(tenant) =
            tenant.filter(|_| matches!(table.table.table_scope, TableScope::TenantScoped))
        else {
            return Ok(None);
        };
        Ok(Some(format!(
            "{} = {}",
            table.column_sql("tenant_id"),
            self.literal_sql(&serde_json::Value::String(tenant.to_string()))?
        )))
    }

    pub(super) fn append_grouping_and_order(
        &self,
        sql: &mut String,
        query: &StructuredQuery,
    ) -> AnalyticsEngineResult<()> {
        Self::append_sql_fragments(
            sql,
            " GROUP BY ",
            ", ",
            query
                .group_by
                .iter()
                .map(|expression| self.expression_sql(expression)),
        )?;
        Self::append_sql_fragments(
            sql,
            " ORDER BY ",
            ", ",
            query.order_by.iter().map(|order| self.order_sql(order)),
        )
    }

    fn append_sql_fragments(
        sql: &mut String,
        prefix: &str,
        separator: &str,
        fragments: impl IntoIterator<Item = AnalyticsEngineResult<String>>,
    ) -> AnalyticsEngineResult<()> {
        for (index, fragment) in fragments.into_iter().enumerate() {
            sql.push_str(if index == 0 { prefix } else { separator });
            sql.push_str(&fragment?);
        }
        Ok(())
    }
}
