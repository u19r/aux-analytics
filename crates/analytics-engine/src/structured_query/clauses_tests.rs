use analytics_fixtures::users_manifest;
use serde_json::json;

use super::*;

#[test]
#[ignore = "paired query assembly allocation experiment; run in the final measurement batch"]
fn given_equal_query_clauses_when_compiled_into_one_buffer_then_sql_and_parameters_match_with_less_temporary_allocation()
 {
    let manifest = users_manifest();
    for width in [1, 64] {
        let query = compilation_query(width);
        let baseline = QueryCompiler::try_new(&manifest, &query, true).unwrap();
        let candidate = QueryCompiler::try_new(&manifest, &query, true).unwrap();
        for sample in 0..8 {
            let mut results = Vec::new();
            for use_candidate in [sample % 2 == 0, sample % 2 != 0] {
                let guard = alloc_counter::AllocationGuard::start(
                    module_path!(),
                    "query clause assembly",
                    file!(),
                    line!(),
                    None,
                );
                let sql = if use_candidate {
                    candidate.compile(&query, Some("tenant-quote'λ"))
                } else {
                    baseline.compile_materialized_baseline(&query, Some("tenant-quote'λ"))
                }
                .unwrap();
                let allocations = guard.finish();
                results.push((use_candidate, sql, allocations));
            }
            let reference = results.iter().find(|(candidate, _, _)| !candidate).unwrap();
            let optimized = results.iter().find(|(candidate, _, _)| *candidate).unwrap();
            assert_eq!(reference.1, optimized.1);
            assert_eq!(
                *baseline.parameters.borrow(),
                *candidate.parameters.borrow()
            );
            assert!(optimized.2.allocation_count < reference.2.allocation_count);
            assert!(optimized.2.allocated_bytes < reference.2.allocated_bytes);
            println!(
                "query-assembly | width={width} | pair={sample} | sql_bytes={} | \
                 baseline_allocations={} | candidate_allocations={} | baseline_bytes={} | \
                 candidate_bytes={}",
                optimized.1.len(),
                reference.2.allocation_count,
                optimized.2.allocation_count,
                reference.2.allocated_bytes,
                optimized.2.allocated_bytes
            );
        }
    }
}

fn compilation_query(width: usize) -> StructuredQuery {
    StructuredQuery {
        analytics_table_name: "users".to_string(),
        table_alias: None,
        joins: Vec::new(),
        select: (0..width)
            .map(|index| QuerySelect::Column {
                table_alias: None,
                column_name: "email".to_string(),
                alias: Some(format!("email_{index}")),
            })
            .collect(),
        filters: vec![QueryPredicate::NotEq {
            expression: QueryExpression::Column {
                table_alias: None,
                column_name: "email".to_string(),
            },
            value: json!("quote'λ"),
        }],
        group_by: vec![
            QueryExpression::Column {
                table_alias: None,
                column_name: "email".to_string()
            };
            width
        ],
        order_by: vec![
            QueryOrder {
                expression: QueryExpression::Column {
                    table_alias: None,
                    column_name: "email".to_string()
                },
                direction: Some(SortOrder::Desc),
            };
            width
        ],
        limit: Some(10),
        offset: Some(2),
    }
}

// Retain the pre-change assembly only as an equal-work allocation reference.
impl QueryCompiler<'_> {
    fn compile_materialized_baseline(
        &self,
        query: &StructuredQuery,
        target_tenant_id: Option<&str>,
    ) -> AnalyticsEngineResult<String> {
        validate_grouped_selects(self, query)?;
        self.parameters.borrow_mut().clear();
        self.parameter_indexes.borrow_mut().clear();
        let select_clause = query
            .select
            .iter()
            .map(|select| self.select_sql(select))
            .collect::<AnalyticsEngineResult<Vec<_>>>()?
            .join(", ");
        let mut sql = format!("SELECT {select_clause} FROM {}", self.source_table_sql()?);
        for join in &query.joins {
            sql.push(' ');
            sql.push_str(self.join_sql(join, target_tenant_id)?.as_str());
        }

        let mut filters = query
            .filters
            .iter()
            .map(|predicate| self.predicate_sql(predicate))
            .collect::<AnalyticsEngineResult<Vec<_>>>()?;
        if let Some(target_tenant_id) = target_tenant_id {
            for table in self
                .tables
                .iter()
                .filter(|table| matches!(table.table.table_scope, TableScope::TenantScoped))
            {
                filters.push(format!(
                    "{} = {}",
                    table.column_sql("tenant_id"),
                    self.literal_sql(&serde_json::Value::String(target_tenant_id.to_string()))?
                ));
            }
        }
        if !filters.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(filters.join(" AND ").as_str());
        }
        if !query.group_by.is_empty() {
            let group_by = query
                .group_by
                .iter()
                .map(|expression| self.expression_sql(expression))
                .collect::<AnalyticsEngineResult<Vec<_>>>()?
                .join(", ");
            sql.push_str(" GROUP BY ");
            sql.push_str(group_by.as_str());
        }
        if !query.order_by.is_empty() {
            let order_by = query
                .order_by
                .iter()
                .map(|order| self.order_sql(order))
                .collect::<AnalyticsEngineResult<Vec<_>>>()?
                .join(", ");
            sql.push_str(" ORDER BY ");
            sql.push_str(order_by.as_str());
        }
        sql.push_str(" LIMIT ");
        sql.push_str(query.effective_limit().to_string().as_str());
        if let Some(offset) = query.offset {
            sql.push_str(" OFFSET ");
            sql.push_str(offset.to_string().as_str());
        }
        Ok(sql)
    }
}
