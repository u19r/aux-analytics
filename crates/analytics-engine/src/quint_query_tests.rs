use analytics_contract::{
    QueryExpression, QueryPredicate, QuerySelect, RowIdentity, StructuredQuery, TableRegistration,
    TenantSelector,
};
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;
use serde_json::json;

use crate::{AnalyticsEngine, structured_query::structured_query_sql};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum QueryResult {
    NotChecked,
    SelectAccepted,
    WithAccepted,
    MutatingSqlRejected,
    MultipleStatementsRejected,
    StructuredQueryCompiled,
    DocumentPathCompiled,
    UnregisteredColumnRejected,
    EmptySelectRejected,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
#[allow(clippy::enum_variant_names)]
enum SqlPrefix {
    SelectPrefix,
    WithPrefix,
    InsertPrefix,
    UpdatePrefix,
    DeletePrefix,
    UnknownPrefix,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct QueryState {
    #[serde(rename = "lastResult")]
    last_result: QueryResult,
}

impl State<QueryDriver> for QueryState {
    fn from_driver(driver: &QueryDriver) -> Result<Self> {
        Ok(Self {
            last_result: driver.last_result,
        })
    }
}

struct QueryDriver {
    last_result: QueryResult,
}

impl Default for QueryDriver {
    fn default() -> Self {
        Self {
            last_result: QueryResult::NotChecked,
        }
    }
}

impl Driver for QueryDriver {
    type State = QueryState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.last_result = QueryResult::NotChecked,
            CheckSelectAccepted => self.last_result = check_select_accepted()?,
            CheckWithAccepted => self.last_result = check_with_accepted()?,
            CheckMutatingSqlRejected => self.last_result = check_mutating_sql_rejected()?,
            CheckMultipleStatementsRejected => {
                self.last_result = check_multiple_statements_rejected()?;
            },
            CheckStructuredQueryCompiled => self.last_result = check_structured_query_compiled()?,
            CheckDocumentPathCompiled => self.last_result = check_document_path_compiled()?,
            CheckUnregisteredColumnRejected => {
                self.last_result = check_unregistered_column_rejected()?;
            },
            CheckEmptySelectRejected => self.last_result = check_empty_select_rejected()?,
            GenerateRawQuery(prefix, statement_count) => {
                self.last_result = raw_query_result(prefix, statement_count);
            },
            GenerateStructuredQuery(
                select_count,
                selected_column_registered,
                uses_document_path,
                document_column_registered,
                uses_configured_document_column,
                path_valid,
                filter_expression_allowed,
                group_by_expression_allowed,
                order_by_expression_allowed,
                limit_valid
            ) => {
                self.last_result = structured_query_result(
                    select_count,
                    selected_column_registered,
                    uses_document_path,
                    document_column_registered,
                    uses_configured_document_column,
                    path_valid,
                    filter_expression_allowed,
                    group_by_expression_allowed,
                    order_by_expression_allowed,
                    limit_valid,
                );
            },
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/query.qnt",
    max_samples = 50,
    max_steps = 8,
    seed = "0xa"
)]
fn quint_query_model_matches_engine_query_rules() -> impl Driver {
    QueryDriver::default()
}

fn raw_query_result(prefix: SqlPrefix, statement_count: i64) -> QueryResult {
    if is_read_only_prefix(prefix) && statement_count == 1 {
        if prefix == SqlPrefix::SelectPrefix {
            QueryResult::SelectAccepted
        } else {
            QueryResult::WithAccepted
        }
    } else if statement_count != 1 {
        QueryResult::MultipleStatementsRejected
    } else {
        QueryResult::MutatingSqlRejected
    }
}

#[allow(clippy::fn_params_excessive_bools, clippy::too_many_arguments)]
fn structured_query_result(
    select_count: i64,
    selected_column_registered: bool,
    uses_document_path: bool,
    document_column_registered: bool,
    uses_configured_document_column: bool,
    path_valid: bool,
    filter_expression_allowed: bool,
    group_by_expression_allowed: bool,
    order_by_expression_allowed: bool,
    limit_valid: bool,
) -> QueryResult {
    if select_count == 0 {
        return QueryResult::EmptySelectRejected;
    }
    let document_path_allowed = !uses_document_path
        || (document_column_registered && uses_configured_document_column && path_valid);
    if !selected_column_registered
        || !document_path_allowed
        || !filter_expression_allowed
        || !group_by_expression_allowed
        || !order_by_expression_allowed
        || !limit_valid
    {
        return QueryResult::UnregisteredColumnRejected;
    }
    if uses_document_path {
        QueryResult::DocumentPathCompiled
    } else {
        QueryResult::StructuredQueryCompiled
    }
}

fn is_read_only_prefix(prefix: SqlPrefix) -> bool {
    matches!(prefix, SqlPrefix::SelectPrefix | SqlPrefix::WithPrefix)
}

fn check_select_accepted() -> Result<QueryResult> {
    let engine = AnalyticsEngine::connect_duckdb(":memory:")?;
    let rows = engine.query_unscoped_sql_json("select 1 as ready")?;
    if rows.first().and_then(|row| row["ready"].as_i64()) == Some(1) {
        Ok(QueryResult::SelectAccepted)
    } else {
        Err(anyhow::anyhow!("read-only SELECT query did not execute"))
    }
}

fn check_with_accepted() -> Result<QueryResult> {
    let engine = AnalyticsEngine::connect_duckdb(":memory:")?;
    let rows = engine
        .query_unscoped_sql_json("with ready as (select 1 as value) select value from ready")?;
    if rows.first().and_then(|row| row["value"].as_i64()) == Some(1) {
        Ok(QueryResult::WithAccepted)
    } else {
        Err(anyhow::anyhow!("read-only WITH query did not execute"))
    }
}

fn check_mutating_sql_rejected() -> Result<QueryResult> {
    let engine = AnalyticsEngine::connect_duckdb(":memory:")?;
    if engine
        .query_unscoped_sql_json("insert into users values (1)")
        .is_err()
    {
        Ok(QueryResult::MutatingSqlRejected)
    } else {
        Err(anyhow::anyhow!("mutating SQL was accepted"))
    }
}

fn check_multiple_statements_rejected() -> Result<QueryResult> {
    let engine = AnalyticsEngine::connect_duckdb(":memory:")?;
    if engine
        .query_unscoped_sql_json("select 1; select 2")
        .is_err()
    {
        Ok(QueryResult::MultipleStatementsRejected)
    } else {
        Err(anyhow::anyhow!("multiple SQL statements were accepted"))
    }
}

fn check_structured_query_compiled() -> Result<QueryResult> {
    let sql = structured_query_sql(
        &table(),
        &StructuredQuery {
            analytics_table_name: "users".to_string(),
            select: vec![QuerySelect::Column {
                column_name: "email".to_string(),
                alias: Some("user\"email".to_string()),
            }],
            filters: vec![QueryPredicate::Eq {
                expression: QueryExpression::Column {
                    column_name: "org_id".to_string(),
                },
                value: json!("org's"),
            }],
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: Some(10),
        },
    )?;
    if sql.contains("\"email\" AS \"user\"\"email\"")
        && sql.contains("\"org_id\" = 'org''s'")
        && sql.ends_with("LIMIT 10")
    {
        Ok(QueryResult::StructuredQueryCompiled)
    } else {
        Err(anyhow::anyhow!(
            "structured query SQL did not escape output"
        ))
    }
}

fn check_document_path_compiled() -> Result<QueryResult> {
    let sql = structured_query_sql(
        &table(),
        &StructuredQuery {
            analytics_table_name: "users".to_string(),
            select: vec![QuerySelect::DocumentPath {
                document_column: "item".to_string(),
                path: "profile.email".to_string(),
                alias: "email".to_string(),
            }],
            filters: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
        },
    )?;
    if sql.contains("json_extract_string(\"item\", '$.profile.email') AS \"email\"") {
        Ok(QueryResult::DocumentPathCompiled)
    } else {
        Err(anyhow::anyhow!("document path query was not compiled"))
    }
}

fn check_unregistered_column_rejected() -> Result<QueryResult> {
    let error = structured_query_sql(
        &table(),
        &StructuredQuery {
            analytics_table_name: "users".to_string(),
            select: vec![QuerySelect::Column {
                column_name: "password".to_string(),
                alias: None,
            }],
            filters: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
        },
    );
    if error.is_err() {
        Ok(QueryResult::UnregisteredColumnRejected)
    } else {
        Err(anyhow::anyhow!(
            "unregistered structured query column was accepted"
        ))
    }
}

fn check_empty_select_rejected() -> Result<QueryResult> {
    let query = StructuredQuery {
        analytics_table_name: "users".to_string(),
        select: Vec::new(),
        filters: Vec::new(),
        group_by: Vec::new(),
        order_by: Vec::new(),
        limit: None,
    };
    if query.validate_shape().is_err() {
        Ok(QueryResult::EmptySelectRejected)
    } else {
        Err(anyhow::anyhow!(
            "empty structured query select was accepted"
        ))
    }
}

fn table() -> TableRegistration {
    TableRegistration {
        source_table_name: "source_users".to_string(),
        analytics_table_name: "users".to_string(),
        source_table_name_prefix: None,
        tenant_id: None,
        tenant_selector: TenantSelector::None,
        row_identity: RowIdentity::RecordKey,
        document_column: Some("item".to_string()),
        skip_delete: false,
        retention: None,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        projection_attribute_names: Some(vec!["email".to_string(), "org_id".to_string()]),
        projection_columns: None,
        columns: Vec::new(),
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
    }
}
