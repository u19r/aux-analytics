use super::*;

pub(super) struct QueryTable<'a> {
    pub(super) alias: String,
    pub(super) table: &'a TableRegistration,
    pub(super) registered_columns: std::collections::HashMap<String, AnalyticsColumnType>,
    pub(super) render_alias: bool,
}

pub(super) struct ResolvedExpression {
    pub(super) table_alias: Option<String>,
    pub(super) sql: String,
    pub(super) signature: ExpressionSignature,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ExpressionSignature {
    Column {
        table_alias: String,
        column_name: String,
    },
    DocumentPath {
        table_alias: String,
        document_column: String,
        path: String,
    },
    Conditional {
        sql: String,
    },
}

pub(crate) struct QueryCompiler<'a> {
    pub(super) tables: Vec<QueryTable<'a>>,
    pub(super) parameters: RefCell<Vec<duckdb::types::Value>>,
    pub(super) parameter_indexes: RefCell<HashMap<u64, usize>>,
    pub(super) bind_literals: bool,
}
