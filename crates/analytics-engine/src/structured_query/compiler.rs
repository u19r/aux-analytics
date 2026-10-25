use super::*;

impl<'a> QueryCompiler<'a> {
    pub(super) fn try_new(
        manifest: &'a AnalyticsManifest,
        query: &StructuredQuery,
        bind_literals: bool,
    ) -> AnalyticsEngineResult<Self> {
        let primary = find_registered_table(manifest, query.analytics_table_name.as_str())?;
        if !primary.join_policy.allowed_as_primary {
            return Err(AnalyticsEngineError::InvalidStructuredQuery(format!(
                "table {} is not allowed as primary",
                primary.analytics_table_name
            )));
        }

        let render_alias = query.table_alias.is_some() || !query.joins.is_empty();
        let primary_alias = query
            .table_alias
            .as_deref()
            .unwrap_or(primary.analytics_table_name.as_str())
            .to_string();
        let mut tables = vec![QueryTable::new(primary_alias, primary, render_alias)];
        for join in &query.joins {
            let table = find_registered_table(manifest, join.analytics_table_name.as_str())?;
            validate_join_policy(table)?;
            tables.push(QueryTable::new(join.table_alias.clone(), table, true));
        }
        Ok(Self {
            tables,
            parameters: RefCell::new(Vec::new()),
            parameter_indexes: RefCell::new(HashMap::new()),
            bind_literals,
        })
    }

    pub(super) fn compile(
        &self,
        query: &StructuredQuery,
        target_tenant_id: Option<&str>,
    ) -> AnalyticsEngineResult<String> {
        validate_grouped_selects(self, query)?;
        self.parameters.borrow_mut().clear();
        self.parameter_indexes.borrow_mut().clear();
        let mut sql = String::from("SELECT ");
        self.append_select_and_joins(&mut sql, query, target_tenant_id)?;
        self.append_query_filters(&mut sql, query, target_tenant_id)?;
        self.append_grouping_and_order(&mut sql, query)?;
        sql.push_str(" LIMIT ");
        sql.push_str(query.effective_limit().to_string().as_str());
        if let Some(offset) = query.offset {
            sql.push_str(" OFFSET ");
            sql.push_str(offset.to_string().as_str());
        }
        Ok(sql)
    }

    pub(super) fn source_table_sql(&self) -> AnalyticsEngineResult<String> {
        let primary = self.primary_table()?;
        Ok(primary.table_sql())
    }

    pub(super) fn join_sql(
        &self,
        join: &QueryJoin,
        target_tenant_id: Option<&str>,
    ) -> AnalyticsEngineResult<String> {
        let table = self.table_by_alias(join.table_alias.as_str())?;
        let join_kind = match join.kind {
            QueryJoinKind::Inner => "INNER JOIN",
            QueryJoinKind::Left => "LEFT JOIN",
        };
        let mut predicates = join
            .on
            .iter()
            .map(|predicate| {
                let left = self.resolve_expression(&predicate.left)?;
                let right = self.resolve_expression(&predicate.right)?;
                let (Some(left_alias), Some(right_alias)) =
                    (left.table_alias.as_deref(), right.table_alias.as_deref())
                else {
                    return Err(AnalyticsEngineError::InvalidStructuredQuery(
                        "join predicate expressions must each reference one registered table"
                            .to_string(),
                    ));
                };
                if left_alias == right_alias {
                    return Err(AnalyticsEngineError::InvalidStructuredQuery(
                        "join predicate must compare two table aliases".to_string(),
                    ));
                }
                Ok(format!("{} = {}", left.sql, right.sql))
            })
            .collect::<AnalyticsEngineResult<Vec<_>>>()?;
        predicates.extend(self.tenant_filter_sql(table, target_tenant_id)?);
        Ok(format!(
            "{join_kind} {} ON {}",
            table.table_sql(),
            predicates.join(" AND ")
        ))
    }

    pub(super) fn select_sql(&self, select: &QuerySelect) -> AnalyticsEngineResult<String> {
        match select {
            QuerySelect::Column {
                table_alias,
                column_name,
                alias,
            } => {
                let expression = self.resolve_column(table_alias.as_deref(), column_name)?;
                Ok(alias_sql(
                    expression.sql.as_str(),
                    alias.as_deref().unwrap_or(column_name),
                ))
            }
            QuerySelect::DocumentPath {
                table_alias,
                document_column,
                path,
                alias,
            } => {
                let expression =
                    self.resolve_document_path(table_alias.as_deref(), document_column, path)?;
                Ok(alias_sql(expression.sql.as_str(), alias))
            }
            QuerySelect::Expression { expression, alias } => {
                let expression = self.expression_sql(expression)?;
                Ok(alias_sql(expression.as_str(), alias))
            }
            QuerySelect::Count { alias } => Ok(alias_sql("count(*)", alias)),
            QuerySelect::Sum { expression, alias } => {
                self.aggregate_select_sql("sum", expression, alias)
            }
            QuerySelect::Min { expression, alias } => {
                self.aggregate_select_sql("min", expression, alias)
            }
            QuerySelect::Max { expression, alias } => {
                self.aggregate_select_sql("max", expression, alias)
            }
            QuerySelect::Avg { expression, alias } => {
                self.aggregate_select_sql("avg", expression, alias)
            }
            QuerySelect::CountDistinct { expression, alias } => {
                let expression = self.expression_sql(expression)?;
                Ok(alias_sql(
                    format!("count(DISTINCT {expression})").as_str(),
                    alias,
                ))
            }
        }
    }

    pub(super) fn result_columns(
        &self,
        query: &StructuredQuery,
    ) -> AnalyticsEngineResult<Vec<PreparedQueryResultColumn>> {
        query
            .select
            .iter()
            .filter_map(|select| self.result_column(select).transpose())
            .collect()
    }

    pub(super) fn result_column(
        &self,
        select: &QuerySelect,
    ) -> AnalyticsEngineResult<Option<PreparedQueryResultColumn>> {
        let Some((table_alias, column_name)) = select_result_source(select) else {
            return Ok(None);
        };
        let table = self.resolve_table_for_column(table_alias, column_name)?;
        let value_type = table
            .registered_columns
            .get(column_name)
            .and_then(prepared_result_value_type);
        Ok(value_type.map(|value_type| PreparedQueryResultColumn {
            name: select.output_alias(),
            value_type,
        }))
    }

    pub(super) fn aggregate_select_sql(
        &self,
        function_name: &str,
        expression: &QueryExpression,
        alias: &str,
    ) -> AnalyticsEngineResult<String> {
        let expression = self.expression_sql(expression)?;
        Ok(alias_sql(
            format!("{function_name}({expression})").as_str(),
            alias,
        ))
    }

    pub(super) fn predicate_sql(
        &self,
        predicate: &QueryPredicate,
    ) -> AnalyticsEngineResult<String> {
        match predicate {
            QueryPredicate::Eq { expression, value } => {
                self.binary_predicate_sql(expression, "=", value)
            }
            QueryPredicate::NotEq { expression, value } => {
                self.binary_predicate_sql(expression, "<>", value)
            }
            QueryPredicate::Gt { expression, value } => {
                self.binary_predicate_sql(expression, ">", value)
            }
            QueryPredicate::Gte { expression, value } => {
                self.binary_predicate_sql(expression, ">=", value)
            }
            QueryPredicate::Lt { expression, value } => {
                self.binary_predicate_sql(expression, "<", value)
            }
            QueryPredicate::Lte { expression, value } => {
                self.binary_predicate_sql(expression, "<=", value)
            }
            QueryPredicate::IsNull { expression } => {
                Ok(format!("{} IS NULL", self.expression_sql(expression)?))
            }
            QueryPredicate::IsNotNull { expression } => {
                Ok(format!("{} IS NOT NULL", self.expression_sql(expression)?))
            }
            QueryPredicate::DocumentMatches {
                table_alias,
                document_column,
                predicate,
            } => {
                let table =
                    self.resolve_table_for_document_path(table_alias.as_deref(), document_column)?;
                self.document_predicate_sql(
                    table.column_sql(document_column).as_str(),
                    predicate,
                    0,
                )
            }
        }
    }

    pub(super) fn document_predicate_sql(
        &self,
        document_sql: &str,
        predicate: &QueryDocumentPredicate,
        depth: usize,
    ) -> AnalyticsEngineResult<String> {
        match predicate {
            QueryDocumentPredicate::All { predicates } => {
                self.document_predicate_group_sql(document_sql, predicates, depth, "AND")
            }
            QueryDocumentPredicate::Any { predicates } => {
                self.document_predicate_group_sql(document_sql, predicates, depth, "OR")
            }
            QueryDocumentPredicate::Not { predicate } => Ok(format!(
                "NOT ({})",
                self.document_predicate_sql(document_sql, predicate, depth + 1)?
            )),
            QueryDocumentPredicate::ArrayAny { path, predicate } => {
                self.document_array_predicate_sql(document_sql, path.as_deref(), predicate, depth)
            }
            _ => document_leaf_predicate_sql(document_sql, predicate),
        }
    }

    pub(super) fn document_array_predicate_sql(
        &self,
        document_sql: &str,
        path: Option<&str>,
        predicate: &QueryDocumentPredicate,
        depth: usize,
    ) -> AnalyticsEngineResult<String> {
        let alias = format!("document_element_{depth}");
        let quoted_alias = sql::quote_identifier(alias.as_str());
        let array = document_json_sql(document_sql, path);
        let element = format!("{quoted_alias}.value");
        let nested = self.document_predicate_sql(&element, predicate, depth + 1)?;
        Ok(format!(
            "EXISTS (SELECT 1 FROM json_each({array}) AS {quoted_alias} WHERE {nested})"
        ))
    }

    pub(super) fn document_predicate_group_sql(
        &self,
        document_sql: &str,
        predicates: &[QueryDocumentPredicate],
        depth: usize,
        operator: &str,
    ) -> AnalyticsEngineResult<String> {
        predicates
            .iter()
            .map(|predicate| self.document_predicate_sql(document_sql, predicate, depth + 1))
            .collect::<AnalyticsEngineResult<Vec<_>>>()
            .map(|predicates| format!("({})", predicates.join(format!(" {operator} ").as_str())))
    }

    pub(super) fn binary_predicate_sql(
        &self,
        expression: &QueryExpression,
        operator: &str,
        value: &serde_json::Value,
    ) -> AnalyticsEngineResult<String> {
        Ok(format!(
            "{} {operator} {}",
            self.expression_sql(expression)?,
            self.literal_sql(value)?
        ))
    }

    pub(super) fn order_sql(&self, order: &QueryOrder) -> AnalyticsEngineResult<String> {
        let direction = match order.direction {
            Some(SortOrder::Desc) => "DESC",
            Some(SortOrder::Asc) | None => "ASC",
        };
        Ok(format!(
            "{} {direction}",
            self.expression_sql(&order.expression)?
        ))
    }

    pub(super) fn expression_sql(
        &self,
        expression: &QueryExpression,
    ) -> AnalyticsEngineResult<String> {
        Ok(self.resolve_expression(expression)?.sql)
    }

    pub(super) fn resolve_expression(
        &self,
        expression: &QueryExpression,
    ) -> AnalyticsEngineResult<ResolvedExpression> {
        match expression {
            QueryExpression::Column {
                table_alias,
                column_name,
            } => self.resolve_column(table_alias.as_deref(), column_name),
            QueryExpression::DocumentPath {
                table_alias,
                document_column,
                path,
            } => self.resolve_document_path(table_alias.as_deref(), document_column, path),
            QueryExpression::Lower { expression } => {
                let resolved = self.resolve_expression(expression)?;
                let sql = format!("lower({})", resolved.sql);
                Ok(ResolvedExpression {
                    table_alias: resolved.table_alias,
                    signature: ExpressionSignature::Conditional { sql: sql.clone() },
                    sql,
                })
            }
            QueryExpression::Conditional {
                branches,
                else_value,
            } => self.resolve_conditional(branches, else_value),
        }
    }

    pub(super) fn resolve_conditional(
        &self,
        branches: &[QueryConditionalBranch],
        else_value: &serde_json::Value,
    ) -> AnalyticsEngineResult<ResolvedExpression> {
        let mut sql = String::from("CASE");
        for branch in branches {
            sql.push_str(" WHEN ");
            sql.push_str(self.conditional_branch_sql(branch)?.as_str());
            sql.push_str(" THEN ");
            sql.push_str(self.literal_sql(&branch.then_value)?.as_str());
        }
        sql.push_str(" ELSE ");
        sql.push_str(self.literal_sql(else_value)?.as_str());
        sql.push_str(" END");
        Ok(ResolvedExpression {
            table_alias: None,
            signature: ExpressionSignature::Conditional { sql: sql.clone() },
            sql,
        })
    }

    pub(super) fn conditional_branch_sql(
        &self,
        branch: &QueryConditionalBranch,
    ) -> AnalyticsEngineResult<String> {
        branch
            .all
            .iter()
            .map(|comparison| self.column_comparison_sql(comparison))
            .collect::<AnalyticsEngineResult<Vec<_>>>()
            .map(|comparisons| format!("({})", comparisons.join(" AND ")))
    }

    pub(super) fn column_comparison_sql(
        &self,
        comparison: &QueryColumnComparison,
    ) -> AnalyticsEngineResult<String> {
        let column = self.resolve_column(
            comparison.table_alias.as_deref(),
            comparison.column_name.as_str(),
        )?;
        let operator = match comparison.operator {
            QueryComparisonOperator::Eq => "=",
            QueryComparisonOperator::NotEq => "<>",
            QueryComparisonOperator::Gt => ">",
            QueryComparisonOperator::Gte => ">=",
            QueryComparisonOperator::Lt => "<",
            QueryComparisonOperator::Lte => "<=",
        };
        Ok(format!(
            "{} {operator} {}",
            column.sql,
            self.literal_sql(&comparison.value)?
        ))
    }

    pub(super) fn resolve_column(
        &self,
        table_alias: Option<&str>,
        column_name: &str,
    ) -> AnalyticsEngineResult<ResolvedExpression> {
        let table = self.resolve_table_for_column(table_alias, column_name)?;
        Ok(ResolvedExpression {
            table_alias: Some(table.alias.clone()),
            sql: table.column_sql(column_name),
            signature: ExpressionSignature::Column {
                table_alias: table.alias.clone(),
                column_name: column_name.to_string(),
            },
        })
    }

    pub(super) fn resolve_document_path(
        &self,
        table_alias: Option<&str>,
        document_column: &str,
        path: &str,
    ) -> AnalyticsEngineResult<ResolvedExpression> {
        let table = self.resolve_table_for_document_path(table_alias, document_column)?;
        let mut json_path = String::with_capacity(path.len() + 2);
        json_path.push_str("$.");
        json_path.push_str(path);
        Ok(ResolvedExpression {
            table_alias: Some(table.alias.clone()),
            sql: format!(
                "json_extract_string({}, '{}')",
                table.column_sql(document_column),
                escape_sql_string(json_path.as_str())
            ),
            signature: ExpressionSignature::DocumentPath {
                table_alias: table.alias.clone(),
                document_column: document_column.to_string(),
                path: path.to_string(),
            },
        })
    }
}
