use analytics_contract::{
    AnalyticsManifest, QueryExpression, QueryJoin, QueryJoinKind, QueryOrder, QueryPredicate,
    QuerySelect, SortOrder, StructuredQuery, TableRegistration, TableScope,
};

use crate::{
    engine::{AnalyticsEngineError, AnalyticsEngineResult, columns_for_registration},
    sql,
};

struct QueryTable<'a> {
    alias: String,
    table: &'a TableRegistration,
    registered_columns: std::collections::HashSet<String>,
    render_alias: bool,
}

struct ResolvedExpression {
    table_alias: String,
    sql: String,
    signature: ExpressionSignature,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ExpressionSignature {
    Column {
        table_alias: String,
        column_name: String,
    },
    DocumentPath {
        table_alias: String,
        document_column: String,
        path: String,
    },
}

struct QueryCompiler<'a> {
    tables: Vec<QueryTable<'a>>,
}

#[cfg(test)]
pub(crate) fn structured_query_sql(
    table: &TableRegistration,
    query: &StructuredQuery,
) -> AnalyticsEngineResult<String> {
    let manifest = AnalyticsManifest::new(vec![table.clone()]);
    structured_query_sql_for_manifest(&manifest, query)
}

#[cfg(test)]
pub(crate) fn tenant_scoped_structured_query_sql(
    table: &TableRegistration,
    query: &StructuredQuery,
    target_tenant_id: &str,
) -> AnalyticsEngineResult<String> {
    let manifest = AnalyticsManifest::new(vec![table.clone()]);
    tenant_scoped_structured_query_sql_for_manifest(&manifest, query, target_tenant_id)
}

pub(crate) fn structured_query_sql_for_manifest(
    manifest: &AnalyticsManifest,
    query: &StructuredQuery,
) -> AnalyticsEngineResult<String> {
    QueryCompiler::new(manifest, query)?.compile(query, None)
}

pub(crate) fn tenant_scoped_structured_query_sql_for_manifest(
    manifest: &AnalyticsManifest,
    query: &StructuredQuery,
    target_tenant_id: &str,
) -> AnalyticsEngineResult<String> {
    if target_tenant_id.is_empty() {
        return Err(AnalyticsEngineError::InvalidStructuredQuery(
            "target tenant id is required".to_string(),
        ));
    }
    QueryCompiler::new(manifest, query)?.compile(query, Some(target_tenant_id))
}

impl<'a> QueryCompiler<'a> {
    fn new(
        manifest: &'a AnalyticsManifest,
        query: &StructuredQuery,
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
        Ok(Self { tables })
    }

    fn compile(
        &self,
        query: &StructuredQuery,
        target_tenant_id: Option<&str>,
    ) -> AnalyticsEngineResult<String> {
        validate_grouped_selects(self, query)?;
        let select_clause = query
            .select
            .iter()
            .map(|select| self.select_sql(select))
            .collect::<AnalyticsEngineResult<Vec<_>>>()?
            .join(", ");
        let mut sql = format!("SELECT {select_clause} FROM {}", self.source_table_sql()?);
        for join in &query.joins {
            sql.push(' ');
            sql.push_str(self.join_sql(join)?.as_str());
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
                    literal_sql(&serde_json::Value::String(target_tenant_id.to_string()))?
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
        if let Some(limit) = query.limit {
            sql.push_str(" LIMIT ");
            sql.push_str(limit.to_string().as_str());
        }
        Ok(sql)
    }

    fn source_table_sql(&self) -> AnalyticsEngineResult<String> {
        let primary = self.primary_table()?;
        Ok(primary.table_sql())
    }

    fn join_sql(&self, join: &QueryJoin) -> AnalyticsEngineResult<String> {
        let table = self.table_by_alias(join.table_alias.as_str())?;
        let join_kind = match join.kind {
            QueryJoinKind::Inner => "INNER JOIN",
            QueryJoinKind::Left => "LEFT JOIN",
        };
        let predicates = join
            .on
            .iter()
            .map(|predicate| {
                let left = self.resolve_expression(&predicate.left)?;
                let right = self.resolve_expression(&predicate.right)?;
                if left.table_alias == right.table_alias {
                    return Err(AnalyticsEngineError::InvalidStructuredQuery(
                        "join predicate must compare two table aliases".to_string(),
                    ));
                }
                Ok(format!("{} = {}", left.sql, right.sql))
            })
            .collect::<AnalyticsEngineResult<Vec<_>>>()?;
        Ok(format!(
            "{join_kind} {} ON {}",
            table.table_sql(),
            predicates.join(" AND ")
        ))
    }

    fn select_sql(&self, select: &QuerySelect) -> AnalyticsEngineResult<String> {
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

    fn aggregate_select_sql(
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

    fn predicate_sql(&self, predicate: &QueryPredicate) -> AnalyticsEngineResult<String> {
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
        }
    }

    fn binary_predicate_sql(
        &self,
        expression: &QueryExpression,
        operator: &str,
        value: &serde_json::Value,
    ) -> AnalyticsEngineResult<String> {
        Ok(format!(
            "{} {operator} {}",
            self.expression_sql(expression)?,
            literal_sql(value)?
        ))
    }

    fn order_sql(&self, order: &QueryOrder) -> AnalyticsEngineResult<String> {
        let direction = match order.direction {
            Some(SortOrder::Desc) => "DESC",
            Some(SortOrder::Asc) | None => "ASC",
        };
        Ok(format!(
            "{} {direction}",
            self.expression_sql(&order.expression)?
        ))
    }

    fn expression_sql(&self, expression: &QueryExpression) -> AnalyticsEngineResult<String> {
        Ok(self.resolve_expression(expression)?.sql)
    }

    fn resolve_expression(
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
        }
    }

    fn resolve_column(
        &self,
        table_alias: Option<&str>,
        column_name: &str,
    ) -> AnalyticsEngineResult<ResolvedExpression> {
        let table = self.resolve_table_for_column(table_alias, column_name)?;
        Ok(ResolvedExpression {
            table_alias: table.alias.clone(),
            sql: table.column_sql(column_name),
            signature: ExpressionSignature::Column {
                table_alias: table.alias.clone(),
                column_name: column_name.to_string(),
            },
        })
    }

    fn resolve_document_path(
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
            table_alias: table.alias.clone(),
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

    fn resolve_table_for_column(
        &self,
        table_alias: Option<&str>,
        column_name: &str,
    ) -> AnalyticsEngineResult<&QueryTable<'a>> {
        if let Some(table_alias) = table_alias {
            let table = self.table_by_alias(table_alias)?;
            validate_registered_column(&table.registered_columns, column_name)?;
            return Ok(table);
        }
        let matches = self
            .tables
            .iter()
            .filter(|table| table.registered_columns.contains(column_name))
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [table] => Ok(table),
            [] => Err(AnalyticsEngineError::InvalidStructuredQuery(format!(
                "column {column_name} is not registered"
            ))),
            _ => Err(AnalyticsEngineError::InvalidStructuredQuery(format!(
                "column {column_name} is ambiguous; table_alias is required"
            ))),
        }
    }

    fn resolve_table_for_document_path(
        &self,
        table_alias: Option<&str>,
        document_column: &str,
    ) -> AnalyticsEngineResult<&QueryTable<'a>> {
        if let Some(table_alias) = table_alias {
            let table = self.table_by_alias(table_alias)?;
            validate_document_column(table.table, &table.registered_columns, document_column)?;
            return Ok(table);
        }
        let matches = self
            .tables
            .iter()
            .filter(|table| {
                validate_document_column(table.table, &table.registered_columns, document_column)
                    .is_ok()
            })
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [table] => Ok(table),
            [] => Err(AnalyticsEngineError::InvalidStructuredQuery(format!(
                "{document_column} is not a registered document column"
            ))),
            _ => Err(AnalyticsEngineError::InvalidStructuredQuery(format!(
                "document column {document_column} is ambiguous; table_alias is required"
            ))),
        }
    }

    fn table_by_alias(&self, alias: &str) -> AnalyticsEngineResult<&QueryTable<'a>> {
        self.tables
            .iter()
            .find(|table| table.alias == alias)
            .ok_or_else(|| {
                AnalyticsEngineError::InvalidStructuredQuery(format!(
                    "table alias {alias} is not registered"
                ))
            })
    }

    fn primary_table(&self) -> AnalyticsEngineResult<&QueryTable<'a>> {
        self.tables.first().ok_or_else(|| {
            AnalyticsEngineError::InvalidStructuredQuery(
                "structured query has no primary table".to_string(),
            )
        })
    }
}

impl<'a> QueryTable<'a> {
    fn new(alias: String, table: &'a TableRegistration, render_alias: bool) -> Self {
        Self {
            alias,
            table,
            registered_columns: registered_column_names(table),
            render_alias,
        }
    }

    fn table_sql(&self) -> String {
        let table_name = sql::quote_identifier(self.table.analytics_table_name.as_str());
        if self.render_alias {
            return format!(
                "{table_name} AS {}",
                sql::quote_identifier(self.alias.as_str())
            );
        }
        table_name
    }

    fn column_sql(&self, column_name: &str) -> String {
        let column = sql::quote_identifier(column_name);
        if self.render_alias {
            return format!("{}.{}", sql::quote_identifier(self.alias.as_str()), column);
        }
        column
    }
}

fn registered_column_names(table: &TableRegistration) -> std::collections::HashSet<String> {
    let mut names = std::collections::HashSet::from([
        "tenant_id".to_string(),
        "__id".to_string(),
        "table_name".to_string(),
    ]);
    for column in columns_for_registration(table) {
        names.insert(column.column_name);
    }
    names
}

fn find_registered_table<'a>(
    manifest: &'a AnalyticsManifest,
    analytics_table_name: &str,
) -> AnalyticsEngineResult<&'a TableRegistration> {
    manifest
        .tables
        .iter()
        .find(|table| table.analytics_table_name == analytics_table_name)
        .ok_or_else(|| AnalyticsEngineError::TableNotRegistered(analytics_table_name.to_string()))
}

fn validate_join_policy(table: &TableRegistration) -> AnalyticsEngineResult<()> {
    if table.join_policy.allowed_as_join {
        return Ok(());
    }
    let message = match table.table_scope {
        TableScope::GlobalReference { .. } => format!(
            "joined table {} is not allowed as reference data",
            table.analytics_table_name
        ),
        TableScope::TenantScoped => format!(
            "joined table {} is not allowed by join policy",
            table.analytics_table_name
        ),
    };
    Err(AnalyticsEngineError::InvalidStructuredQuery(message))
}

fn validate_grouped_selects(
    compiler: &QueryCompiler<'_>,
    query: &StructuredQuery,
) -> AnalyticsEngineResult<()> {
    let has_aggregate = query.select.iter().any(is_aggregate_select);
    let has_non_aggregate = query
        .select
        .iter()
        .any(|select| non_aggregate_select_expression(select).is_some());
    if !has_aggregate {
        return Ok(());
    }
    if has_non_aggregate && query.group_by.is_empty() {
        return Err(AnalyticsEngineError::InvalidStructuredQuery(
            "non-aggregate selects must appear in group_by".to_string(),
        ));
    }
    let grouped = query
        .group_by
        .iter()
        .map(|expression| {
            compiler
                .resolve_expression(expression)
                .map(|resolved| resolved.signature)
        })
        .collect::<AnalyticsEngineResult<Vec<_>>>()?;
    for select in &query.select {
        let Some(expression) = non_aggregate_select_expression(select) else {
            continue;
        };
        let signature = compiler.resolve_expression(&expression)?.signature;
        if !grouped.contains(&signature) {
            return Err(AnalyticsEngineError::InvalidStructuredQuery(format!(
                "selected expression {} must appear in group_by",
                select.output_alias()
            )));
        }
    }
    Ok(())
}

fn is_aggregate_select(select: &QuerySelect) -> bool {
    matches!(
        select,
        QuerySelect::Count { .. }
            | QuerySelect::Sum { .. }
            | QuerySelect::Min { .. }
            | QuerySelect::Max { .. }
            | QuerySelect::Avg { .. }
            | QuerySelect::CountDistinct { .. }
    )
}

fn non_aggregate_select_expression(select: &QuerySelect) -> Option<QueryExpression> {
    match select {
        QuerySelect::Column {
            table_alias,
            column_name,
            ..
        } => Some(QueryExpression::Column {
            table_alias: table_alias.clone(),
            column_name: column_name.clone(),
        }),
        QuerySelect::DocumentPath {
            table_alias,
            document_column,
            path,
            ..
        } => Some(QueryExpression::DocumentPath {
            table_alias: table_alias.clone(),
            document_column: document_column.clone(),
            path: path.clone(),
        }),
        QuerySelect::Count { .. }
        | QuerySelect::Sum { .. }
        | QuerySelect::Min { .. }
        | QuerySelect::Max { .. }
        | QuerySelect::Avg { .. }
        | QuerySelect::CountDistinct { .. } => None,
    }
}

fn validate_document_column(
    table: &TableRegistration,
    registered_columns: &std::collections::HashSet<String>,
    document_column: &str,
) -> AnalyticsEngineResult<()> {
    validate_registered_column(registered_columns, document_column)?;
    if table.document_column.as_deref() != Some(document_column) {
        return Err(AnalyticsEngineError::InvalidStructuredQuery(format!(
            "{document_column} is not the document column for {}",
            table.analytics_table_name
        )));
    }
    Ok(())
}

fn validate_registered_column(
    registered_columns: &std::collections::HashSet<String>,
    column_name: &str,
) -> AnalyticsEngineResult<()> {
    if registered_columns.contains(column_name) {
        return Ok(());
    }
    Err(AnalyticsEngineError::InvalidStructuredQuery(format!(
        "column {column_name} is not registered"
    )))
}

fn alias_sql(expression: &str, alias: &str) -> String {
    format!("{expression} AS {}", sql::quote_identifier(alias))
}

fn literal_sql(value: &serde_json::Value) -> AnalyticsEngineResult<String> {
    match value {
        serde_json::Value::Null => Ok("NULL".to_string()),
        serde_json::Value::Bool(value) => Ok(if *value { "TRUE" } else { "FALSE" }.to_string()),
        serde_json::Value::Number(value) => Ok(value.to_string()),
        serde_json::Value::String(value) => Ok(format!("'{}'", escape_sql_string(value))),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            let encoded = serde_json::to_string(value)?;
            Ok(format!("'{}'::JSON", escape_sql_string(encoded.as_str())))
        }
    }
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}
