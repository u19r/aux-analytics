use analytics_contract::{
    AnalyticsManifest, QueryExpression, QueryPredicate, QuerySelect, RowIdentity, StructuredQuery,
    TableRegistration, TenantSelector,
};
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;
use serde_json::json;

use crate::{AnalyticsEngine, AnalyticsEngineError, structured_query::structured_query_sql};

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
enum SqlPrefix {
    #[serde(rename = "SelectPrefix")]
    Select,
    #[serde(rename = "WithPrefix")]
    With,
    #[serde(rename = "InsertPrefix")]
    Insert,
    #[serde(rename = "UpdatePrefix")]
    Update,
    #[serde(rename = "DeletePrefix")]
    Delete,
    #[serde(rename = "UnknownPrefix")]
    Unknown,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct QueryState {
    #[serde(rename = "lastResult")]
    last_result: QueryResult,
    #[serde(rename = "rawAccepted")]
    raw_accepted: bool,
    #[serde(rename = "rawReadOnlyPrefix")]
    raw_read_only_prefix: bool,
    #[serde(rename = "rawSingleStatement")]
    raw_single_statement: bool,
    #[serde(rename = "structuredAccepted")]
    structured_accepted: bool,
    #[serde(rename = "structuredHasSelect")]
    structured_has_select: bool,
    #[serde(rename = "structuredHasTableName")]
    structured_has_table_name: bool,
    #[serde(rename = "selectedColumnsRegistered")]
    selected_columns_registered: bool,
    #[serde(rename = "documentPathsAllowed")]
    document_paths_allowed: bool,
    #[serde(rename = "filtersAllowed")]
    filters_allowed: bool,
    #[serde(rename = "groupByAllowed")]
    group_by_allowed: bool,
    #[serde(rename = "orderByAllowed")]
    order_by_allowed: bool,
    #[serde(rename = "limitAllowed")]
    limit_allowed: bool,
    #[serde(rename = "malformedPathUsed")]
    malformed_path_used: bool,
}

impl State<QueryDriver> for QueryState {
    fn from_driver(driver: &QueryDriver) -> Result<Self> {
        Ok(Self {
            last_result: driver.last_result,
            raw_accepted: driver.raw_accepted,
            raw_read_only_prefix: driver.raw_read_only_prefix,
            raw_single_statement: driver.raw_single_statement,
            structured_accepted: driver.structured_accepted,
            structured_has_select: driver.structured_has_select,
            structured_has_table_name: driver.structured_has_table_name,
            selected_columns_registered: driver.selected_columns_registered,
            document_paths_allowed: driver.document_paths_allowed,
            filters_allowed: driver.filters_allowed,
            group_by_allowed: driver.group_by_allowed,
            order_by_allowed: driver.order_by_allowed,
            limit_allowed: driver.limit_allowed,
            malformed_path_used: driver.malformed_path_used,
        })
    }
}

struct QueryDriver {
    engine: AnalyticsEngine,
    manifest: AnalyticsManifest,
    last_result: QueryResult,
    raw_accepted: bool,
    raw_read_only_prefix: bool,
    raw_single_statement: bool,
    structured_accepted: bool,
    structured_has_select: bool,
    structured_has_table_name: bool,
    selected_columns_registered: bool,
    document_paths_allowed: bool,
    filters_allowed: bool,
    group_by_allowed: bool,
    order_by_allowed: bool,
    limit_allowed: bool,
    malformed_path_used: bool,
}

impl Default for QueryDriver {
    fn default() -> Self {
        let manifest = AnalyticsManifest::new(vec![table()]);
        let engine = AnalyticsEngine::connect_duckdb(":memory:")
            .and_then(|engine| {
                engine.ensure_manifest(&manifest)?;
                Ok(engine)
            })
            .expect("connect query conformance engine");
        Self {
            engine,
            manifest,
            last_result: QueryResult::NotChecked,
            raw_accepted: false,
            raw_read_only_prefix: false,
            raw_single_statement: false,
            structured_accepted: false,
            structured_has_select: false,
            structured_has_table_name: false,
            selected_columns_registered: false,
            document_paths_allowed: false,
            filters_allowed: false,
            group_by_allowed: false,
            order_by_allowed: false,
            limit_allowed: false,
            malformed_path_used: false,
        }
    }
}

// These are independent model inputs, including deliberately invalid query
// witnesses. Keep them named through execution and observation rather than
// positional boolean lists.
struct StructuredQueryCase {
    select_count: i64,
    table_name_present: bool,
    selected_column_registered: bool,
    uses_document_path: bool,
    document_column_registered: bool,
    uses_configured_document_column: bool,
    path_valid: bool,
    filter_expression_allowed: bool,
    group_by_expression_allowed: bool,
    order_by_expression_allowed: bool,
    limit_valid: bool,
}

impl Default for StructuredQueryCase {
    fn default() -> Self {
        Self {
            select_count: 1,
            table_name_present: true,
            selected_column_registered: true,
            uses_document_path: false,
            document_column_registered: false,
            uses_configured_document_column: false,
            path_valid: true,
            filter_expression_allowed: true,
            group_by_expression_allowed: true,
            order_by_expression_allowed: true,
            limit_valid: true,
        }
    }
}

impl Driver for QueryDriver {
    type State = QueryState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => *self = Self::default(),
            CheckSelectAccepted => self.record_raw(SqlPrefix::Select, check_select_accepted()?),
            CheckWithAccepted => self.record_raw(SqlPrefix::With, check_with_accepted()?),
            CheckMutatingSqlRejected => self.record_raw(SqlPrefix::Insert, check_mutating_sql_rejected()?),
            CheckMultipleStatementsRejected => {
                self.record_raw(SqlPrefix::Select, check_multiple_statements_rejected()?);
            },
            CheckStructuredQueryCompiled => self.record_structured(&StructuredQueryCase::default(), check_structured_query_compiled()?),
            CheckDocumentPathCompiled => self.record_structured(&StructuredQueryCase {
                uses_document_path: true,
                document_column_registered: true,
                uses_configured_document_column: true,
                ..Default::default()
            }, check_document_path_compiled()?),
            CheckUnregisteredColumnRejected => {
                self.record_structured(&StructuredQueryCase {
                    selected_column_registered: false,
                    ..Default::default()
                }, check_unregistered_column_rejected()?);
            },
            CheckEmptySelectRejected => self.record_structured(&StructuredQueryCase {
                select_count: 0,
                ..Default::default()
            }, check_empty_select_rejected()?),
            GenerateRawQuery(prefix, statement_count) => {
                let result = self.execute_raw_query(prefix, statement_count)?;
                self.record_raw(prefix, result);
            },
            GenerateStructuredQuery(
                select_count,
                table_name_present,
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
                let case = StructuredQueryCase {
                    select_count,
                    table_name_present,
                    selected_column_registered,
                    uses_document_path,
                    document_column_registered,
                    uses_configured_document_column,
                    path_valid,
                    filter_expression_allowed,
                    group_by_expression_allowed,
                    order_by_expression_allowed,
                    limit_valid,
                };
                let result = self.execute_structured_query(&case)?;
                self.record_structured(&case, result);
            },
        })
    }
}

impl QueryDriver {
    fn execute_raw_query(&self, prefix: SqlPrefix, statement_count: i64) -> Result<QueryResult> {
        let statement = match prefix {
            SqlPrefix::Select => "select 1 as value",
            SqlPrefix::With => "with ready as (select 1 as value) select value from ready",
            SqlPrefix::Insert => "insert into users values (1)",
            SqlPrefix::Update => "update users set email = 'changed'",
            SqlPrefix::Delete => "delete from users",
            SqlPrefix::Unknown => "drop table users",
        };
        let sql = if statement_count == 2 {
            format!("{statement}; {statement}")
        } else if statement_count == 0 {
            String::new()
        } else {
            statement.to_string()
        };
        match self.engine.query_unscoped_sql_json(sql.as_str()) {
            Ok(rows) => {
                let result = match prefix {
                    SqlPrefix::Select
                        if rows.first().and_then(|row| row["value"].as_i64()) == Some(1) =>
                    {
                        QueryResult::SelectAccepted
                    }
                    SqlPrefix::With
                        if rows.first().and_then(|row| row["value"].as_i64()) == Some(1) =>
                    {
                        QueryResult::WithAccepted
                    }
                    _ => {
                        return Err(anyhow::anyhow!(
                            "production accepted unexpected raw query shape \
                             {prefix:?}/{statement_count}"
                        ));
                    }
                };
                Ok(result)
            }
            Err(AnalyticsEngineError::InvalidQuery) if statement_count != 1 => {
                Ok(QueryResult::MultipleStatementsRejected)
            }
            Err(AnalyticsEngineError::InvalidQuery) => Ok(QueryResult::MutatingSqlRejected),
            Err(error) => Err(anyhow::anyhow!(
                "raw query boundary returned an unmapped error for {prefix:?}/{statement_count}: \
                 {error:?}"
            )),
        }
    }

    fn execute_structured_query(&self, case: &StructuredQueryCase) -> Result<QueryResult> {
        let StructuredQueryCase {
            select_count,
            table_name_present,
            selected_column_registered,
            uses_document_path,
            document_column_registered,
            uses_configured_document_column,
            path_valid,
            filter_expression_allowed,
            group_by_expression_allowed,
            order_by_expression_allowed,
            limit_valid,
        } = *case;
        let analytics_table_name = if table_name_present {
            "users".to_string()
        } else {
            String::new()
        };
        let mut select = Vec::new();
        for index in 0..select_count.max(0) {
            if uses_document_path && index == 0 {
                select.push(QuerySelect::DocumentPath {
                    table_alias: None,
                    document_column: if document_column_registered {
                        if uses_configured_document_column {
                            "item".to_string()
                        } else {
                            "email".to_string()
                        }
                    } else {
                        "not_registered".to_string()
                    },
                    path: if path_valid {
                        "profile.email".to_string()
                    } else {
                        "profile..email".to_string()
                    },
                    alias: "document_value".to_string(),
                });
            } else {
                let column_name = if selected_column_registered {
                    if index == 0 { "email" } else { "org_id" }
                } else {
                    "not_registered"
                };
                select.push(QuerySelect::Column {
                    table_alias: None,
                    column_name: column_name.to_string(),
                    alias: Some(format!("column_{index}")),
                });
            }
        }
        let filters = if filter_expression_allowed {
            Vec::new()
        } else {
            vec![QueryPredicate::Eq {
                expression: QueryExpression::Column {
                    table_alias: None,
                    column_name: "not_registered".to_string(),
                },
                value: json!("rejected"),
            }]
        };
        let group_by = if group_by_expression_allowed {
            Vec::new()
        } else {
            vec![QueryExpression::Column {
                table_alias: None,
                column_name: "not_registered".to_string(),
            }]
        };
        let order_by = if order_by_expression_allowed {
            Vec::new()
        } else {
            vec![analytics_contract::QueryOrder {
                expression: QueryExpression::Column {
                    table_alias: None,
                    column_name: "not_registered".to_string(),
                },
                direction: Some(analytics_contract::SortOrder::Asc),
            }]
        };
        let query = StructuredQuery {
            analytics_table_name,
            table_alias: None,
            joins: Vec::new(),
            select,
            filters,
            group_by,
            order_by,
            // `0` is the invalid contract witness. The production boundary
            // must reject it; it must not be rendered as an executable LIMIT.
            limit: Some(if limit_valid { 10 } else { 0 }),
            offset: None,
        };
        match self
            .engine
            .query_unscoped_structured_json(&self.manifest, &query)
        {
            Ok(_) if uses_document_path => Ok(QueryResult::DocumentPathCompiled),
            Ok(_) => Ok(QueryResult::StructuredQueryCompiled),
            Err(AnalyticsEngineError::InvalidStructuredQuery(_))
                if select_count == 0 || !table_name_present =>
            {
                Ok(QueryResult::EmptySelectRejected)
            }
            Err(AnalyticsEngineError::InvalidStructuredQuery(_))
                if !selected_column_registered
                    || (uses_document_path
                        && (!document_column_registered
                            || !uses_configured_document_column
                            || !path_valid))
                    || !filter_expression_allowed
                    || !group_by_expression_allowed
                    || !order_by_expression_allowed
                    || !limit_valid =>
            {
                Ok(QueryResult::UnregisteredColumnRejected)
            }
            Err(error) => Err(anyhow::anyhow!(
                "structured query boundary returned an unmapped error for a valid shape: {error:?}"
            )),
        }
    }

    fn record_raw(&mut self, prefix: SqlPrefix, result: QueryResult) {
        self.last_result = result;
        self.raw_accepted = matches!(
            result,
            QueryResult::SelectAccepted | QueryResult::WithAccepted
        );
        self.raw_read_only_prefix = is_read_only_prefix(prefix);
        self.raw_single_statement = !matches!(result, QueryResult::MultipleStatementsRejected);
        self.structured_accepted = false;
        self.structured_has_select = true;
        self.structured_has_table_name = true;
        self.selected_columns_registered = true;
        self.document_paths_allowed = true;
        self.filters_allowed = true;
        self.group_by_allowed = true;
        self.order_by_allowed = true;
        self.limit_allowed = true;
        self.malformed_path_used = false;
    }

    fn record_structured(&mut self, case: &StructuredQueryCase, result: QueryResult) {
        let StructuredQueryCase {
            select_count,
            table_name_present,
            selected_column_registered,
            uses_document_path,
            document_column_registered,
            uses_configured_document_column,
            path_valid,
            filter_expression_allowed,
            group_by_expression_allowed,
            order_by_expression_allowed,
            limit_valid,
        } = *case;
        self.last_result = result;
        self.raw_accepted = false;
        self.raw_read_only_prefix = true;
        self.raw_single_statement = true;
        self.structured_has_select = select_count > 0;
        self.structured_has_table_name = table_name_present;
        self.selected_columns_registered = selected_column_registered;
        self.document_paths_allowed = !uses_document_path
            || (document_column_registered && uses_configured_document_column && path_valid);
        self.filters_allowed = filter_expression_allowed;
        self.group_by_allowed = group_by_expression_allowed;
        self.order_by_allowed = order_by_expression_allowed;
        self.limit_allowed = limit_valid;
        self.malformed_path_used = uses_document_path && !path_valid;
        self.structured_accepted = matches!(
            result,
            QueryResult::StructuredQueryCompiled | QueryResult::DocumentPathCompiled
        );
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/query.qnt",
    max_samples = 50,
    max_steps = 8,
    seed = "0xa"
)]
fn given_quint_query_model_when_replayed_then_engine_query_rules_matches() -> impl Driver {
    QueryDriver::default()
}

#[test]
fn given_zero_limit_when_structured_query_reaches_engine_then_boundary_rejects_without_execution() {
    let manifest = AnalyticsManifest::new(vec![table()]);
    let engine = AnalyticsEngine::connect_duckdb(":memory:").expect("connect query engine");
    engine
        .ensure_manifest(&manifest)
        .expect("ensure query manifest");
    let query = StructuredQuery {
        analytics_table_name: "users".to_string(),
        table_alias: None,
        joins: Vec::new(),
        select: vec![QuerySelect::Column {
            table_alias: None,
            column_name: "email".to_string(),
            alias: None,
        }],
        filters: Vec::new(),
        group_by: Vec::new(),
        order_by: Vec::new(),
        limit: Some(0),
        offset: None,
    };
    assert!(matches!(
        engine.query_unscoped_structured_json(&manifest, &query),
        Err(AnalyticsEngineError::InvalidStructuredQuery(message))
            if message == "limit must be greater than zero"
    ));
}

fn is_read_only_prefix(prefix: SqlPrefix) -> bool {
    matches!(prefix, SqlPrefix::Select | SqlPrefix::With)
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
            table_alias: None,
            joins: Vec::new(),
            select: vec![QuerySelect::Column {
                table_alias: None,
                column_name: "email".to_string(),
                alias: Some("user\"email".to_string()),
            }],
            filters: vec![QueryPredicate::Eq {
                expression: QueryExpression::Column {
                    table_alias: None,
                    column_name: "org_id".to_string(),
                },
                value: json!("org's"),
            }],
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: Some(10),
            offset: None,
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
            table_alias: None,
            joins: Vec::new(),
            select: vec![QuerySelect::DocumentPath {
                table_alias: None,
                document_column: "item".to_string(),
                path: "profile.email".to_string(),
                alias: "email".to_string(),
            }],
            filters: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
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
            table_alias: None,
            joins: Vec::new(),
            select: vec![QuerySelect::Column {
                table_alias: None,
                column_name: "password".to_string(),
                alias: None,
            }],
            filters: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
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
        table_alias: None,
        joins: Vec::new(),
        select: Vec::new(),
        filters: Vec::new(),
        group_by: Vec::new(),
        order_by: Vec::new(),
        limit: None,
        offset: None,
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
        row_expansion: None,
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
        table_scope: analytics_contract::TableScope::default(),
        join_policy: analytics_contract::JoinPolicy::default(),
    }
}
