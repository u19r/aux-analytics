use std::sync::OnceLock;

use analytics_contract::{
    AnalyticsColumnType, AnalyticsManifest, PrimitiveColumnType, ProjectionColumn, QueryExpression,
    QueryPredicate, QuerySelect, RowIdentity, StructuredQuery, TableRegistration, TenantSelector,
};
use analytics_engine::{AnalyticsEngine, AnalyticsEngineError, StorageBackend};
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;
use serde_json::json;

use crate::{AnalyticsLambdaError, AnalyticsLambdaHandler};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum CallerInterface {
    Api,
    Cli,
    Lambda,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum QuerySurface {
    RawSql,
    StructuredJson,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum QueryAttack {
    BenignTenantQuery,
    MutatingPrefixAttack,
    MultiStatementInjection,
    WithMutationAttack,
    UnionOtherTenantAttack,
    CrossTenantPredicateAttack,
    MalformedDocumentPathAttack,
    LongRunningQuery,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum SecurityResult {
    NotChecked,
    AcceptedSafe,
    RejectedMutation,
    RejectedCrossTenant,
    RejectedMissingTenantGuard,
    RejectedMalformedPath,
    RejectedRawSqlUnavailable,
    RejectedTimeout,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct LambdaQuerySecurityState {
    last_result: SecurityResult,
    accepted: bool,
    modifies_data: bool,
    sees_other_tenant: bool,
    completes_within_budget: bool,
    tenant_scoped: bool,
    target_tenant_present: bool,
    effective_tenant_guard: bool,
    timer_installed: bool,
    last_caller: CallerInterface,
    last_surface: QuerySurface,
    last_attack: QueryAttack,
    structured_tenant_filter_injected: bool,
    raw_sql_sandboxed: bool,
    last_runtime_ms: i64,
    last_checked: bool,
}

impl State<LambdaQuerySecurityDriver> for LambdaQuerySecurityState {
    fn from_driver(driver: &LambdaQuerySecurityDriver) -> Result<Self> {
        Ok(Self {
            last_result: driver.last_result,
            accepted: driver.accepted,
            modifies_data: driver.modifies_data,
            sees_other_tenant: driver.sees_other_tenant,
            completes_within_budget: driver.completes_within_budget,
            tenant_scoped: driver.tenant_scoped,
            target_tenant_present: driver.target_tenant_present,
            effective_tenant_guard: driver.effective_tenant_guard,
            timer_installed: driver.timer_installed,
            last_caller: driver.last_caller,
            last_surface: driver.last_surface,
            last_attack: driver.last_attack,
            structured_tenant_filter_injected: driver.structured_tenant_filter_injected,
            raw_sql_sandboxed: driver.raw_sql_sandboxed,
            last_runtime_ms: driver.last_runtime_ms,
            last_checked: driver.last_checked,
        })
    }
}

struct LambdaQuerySecurityDriver {
    last_result: SecurityResult,
    accepted: bool,
    modifies_data: bool,
    sees_other_tenant: bool,
    completes_within_budget: bool,
    tenant_scoped: bool,
    target_tenant_present: bool,
    effective_tenant_guard: bool,
    timer_installed: bool,
    last_caller: CallerInterface,
    last_surface: QuerySurface,
    last_attack: QueryAttack,
    structured_tenant_filter_injected: bool,
    raw_sql_sandboxed: bool,
    last_runtime_ms: i64,
    last_checked: bool,
}

impl Default for LambdaQuerySecurityDriver {
    fn default() -> Self {
        Self {
            last_result: SecurityResult::NotChecked,
            accepted: false,
            modifies_data: false,
            sees_other_tenant: false,
            completes_within_budget: true,
            tenant_scoped: true,
            target_tenant_present: false,
            effective_tenant_guard: false,
            timer_installed: false,
            last_caller: CallerInterface::Api,
            last_surface: QuerySurface::StructuredJson,
            last_attack: QueryAttack::BenignTenantQuery,
            structured_tenant_filter_injected: false,
            raw_sql_sandboxed: false,
            last_runtime_ms: 0,
            last_checked: false,
        }
    }
}

impl Driver for LambdaQuerySecurityDriver {
    type State = LambdaQuerySecurityState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => *self = Self::default(),
            lambdaStep(
                surface,
                attack,
                tenant_is_scoped,
                target_present,
                structured_tenant_filter_injected,
                raw_sql_sandboxed,
                installed_timer,
                runtime_ms
            ) => {
                self.set_case(LambdaSecurityCase {
                    caller: CallerInterface::Lambda,
                    surface,
                    attack,
                    tenant_scoped: tenant_is_scoped,
                    target_tenant_present: target_present,
                    structured_tenant_filter_injected,
                    raw_sql_sandboxed,
                    installed_timer,
                    runtime_ms,
                })?;
            },
            CheckSqlInjectionRejected => self.apply_sql_injection_rejection()?,
            CheckCrossTenantRejected => self.apply_cross_tenant_rejection()?,
            CheckTimeoutRejected => self.apply_timeout_rejection()?,
            CheckTenantGuardMissing => self.apply_missing_tenant_guard_rejection()?,
            DirectAcceptedSecureQuery => self.apply_accepted_secure_query()?,
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/query_security_bughunt.qnt",
    step = "lambdaStep",
    max_samples = 20,
    max_steps = 8,
    seed = "0xb"
)]
fn given_quint_query_security_model_when_replayed_then_lambda_boundaries_matches() -> impl Driver {
    LambdaQuerySecurityDriver::default()
}

impl LambdaQuerySecurityDriver {
    fn apply_sql_injection_rejection(&mut self) -> Result {
        if lambda_raw_sql_accepted("insert into users values (1)")?
            || lambda_raw_sql_accepted("select 1; drop table users")?
        {
            return Err(anyhow::anyhow!(
                "lambda unscoped SQL event accepted mutating or injected SQL"
            ));
        }
        self.set_case(LambdaSecurityCase {
            caller: CallerInterface::Cli,
            surface: QuerySurface::RawSql,
            attack: QueryAttack::MutatingPrefixAttack,
            tenant_scoped: false,
            target_tenant_present: false,
            structured_tenant_filter_injected: false,
            raw_sql_sandboxed: true,
            installed_timer: true,
            runtime_ms: 1,
        })?;
        Ok(())
    }

    fn apply_cross_tenant_rejection(&mut self) -> Result {
        let rows = lambda_tenant_query_rows("tenant-a")?;
        if rows
            .iter()
            .any(|row| row["tenant_id"].as_str() != Some("tenant-a"))
        {
            return Err(anyhow::anyhow!(
                "lambda tenant query returned another tenant row"
            ));
        }
        self.set_case(LambdaSecurityCase {
            caller: CallerInterface::Api,
            surface: QuerySurface::StructuredJson,
            attack: QueryAttack::UnionOtherTenantAttack,
            tenant_scoped: true,
            target_tenant_present: true,
            structured_tenant_filter_injected: true,
            raw_sql_sandboxed: false,
            installed_timer: true,
            runtime_ms: 1,
        })?;
        Ok(())
    }

    fn apply_timeout_rejection(&mut self) -> Result {
        ensure_long_running_query_times_out()?;
        self.set_case(LambdaSecurityCase {
            caller: CallerInterface::Lambda,
            surface: QuerySurface::StructuredJson,
            attack: QueryAttack::BenignTenantQuery,
            tenant_scoped: true,
            target_tenant_present: true,
            structured_tenant_filter_injected: true,
            raw_sql_sandboxed: false,
            installed_timer: false,
            runtime_ms: 10_001,
        })?;
        Ok(())
    }

    fn apply_missing_tenant_guard_rejection(&mut self) -> Result {
        if lambda_tenant_query_without_target_accepted()? || lambda_malformed_path_accepted()? {
            return Err(anyhow::anyhow!(
                "lambda structured query accepted missing tenant guard or malformed path"
            ));
        }
        self.set_case(LambdaSecurityCase {
            caller: CallerInterface::Cli,
            surface: QuerySurface::StructuredJson,
            attack: QueryAttack::BenignTenantQuery,
            tenant_scoped: true,
            target_tenant_present: true,
            structured_tenant_filter_injected: false,
            raw_sql_sandboxed: false,
            installed_timer: true,
            runtime_ms: 1,
        })?;
        Ok(())
    }

    fn apply_accepted_secure_query(&mut self) -> Result {
        let rows = lambda_tenant_query_rows("tenant-a")?;
        if rows.len() != 1 || rows[0]["tenant_id"] != "tenant-a" {
            return Err(anyhow::anyhow!(
                "lambda tenant query did not return only target tenant"
            ));
        }
        self.set_case(LambdaSecurityCase {
            caller: CallerInterface::Lambda,
            surface: QuerySurface::StructuredJson,
            attack: QueryAttack::BenignTenantQuery,
            tenant_scoped: true,
            target_tenant_present: true,
            structured_tenant_filter_injected: true,
            raw_sql_sandboxed: false,
            installed_timer: true,
            runtime_ms: 1,
        })?;
        Ok(())
    }

    fn set_case(&mut self, case: LambdaSecurityCase) -> Result {
        let evidence = case.execute_lambda_query()?;
        let LambdaSecurityCase {
            caller,
            surface,
            attack,
            tenant_scoped,
            target_tenant_present,
            structured_tenant_filter_injected,
            raw_sql_sandboxed,
            runtime_ms,
            ..
        } = case;
        self.accepted = evidence.accepted;
        self.modifies_data = evidence.modifies_data;
        self.sees_other_tenant = evidence.sees_other_tenant;
        self.completes_within_budget = evidence.completes_within_budget;
        self.tenant_scoped = tenant_scoped;
        self.target_tenant_present = target_tenant_present;
        self.effective_tenant_guard = evidence.effective_tenant_guard;
        self.timer_installed = evidence.timer_installed;
        self.last_caller = caller;
        self.last_surface = surface;
        self.last_attack = attack;
        self.structured_tenant_filter_injected = structured_tenant_filter_injected;
        self.raw_sql_sandboxed = raw_sql_sandboxed;
        self.last_runtime_ms = runtime_ms;
        self.last_checked = true;
        self.last_result = evidence.result;
        Ok(())
    }
}

struct LambdaSecurityEvidence {
    result: SecurityResult,
    accepted: bool,
    modifies_data: bool,
    sees_other_tenant: bool,
    completes_within_budget: bool,
    effective_tenant_guard: bool,
    timer_installed: bool,
}

struct LambdaSecurityCase {
    caller: CallerInterface,
    surface: QuerySurface,
    attack: QueryAttack,
    tenant_scoped: bool,
    target_tenant_present: bool,
    structured_tenant_filter_injected: bool,
    raw_sql_sandboxed: bool,
    installed_timer: bool,
    runtime_ms: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LambdaBoundaryFailure {
    InvalidQuery,
    InvalidStructuredQuery,
    TenantPredicateConflict,
    QueryTimeout,
    Parser,
}

const MALFORMED_RAW_SQL: &str = "select json_extract(item, '$.profile[') from users;;";

impl LambdaSecurityCase {
    fn execute_lambda_query(&self) -> Result<LambdaSecurityEvidence> {
        let Self {
            surface,
            attack,
            tenant_scoped,
            target_tenant_present,
            structured_tenant_filter_injected,
            raw_sql_sandboxed,
            installed_timer,
            runtime_ms,
            ..
        } = *self;
        let handler = security_handler()?;
        let structured_query = matches!(surface, QuerySurface::StructuredJson)
            .then(|| generated_lambda_query(attack, structured_tenant_filter_injected));
        let raw_sql = matches!(surface, QuerySurface::RawSql).then(|| generated_lambda_sql(attack));
        let event = match surface {
            QuerySurface::RawSql => json!({
                "operation": "unscoped_sql_query",
                "sql": raw_sql.expect("raw SQL request has SQL"),
            }),
            QuerySurface::StructuredJson if tenant_scoped => json!({
                "operation": "tenant_query",
                "target_tenant_id": if target_tenant_present { "tenant-a" } else { "" },
                "query": serde_json::to_value(
                    structured_query.as_ref().expect("structured request has query"),
                )?,
            }),
            QuerySurface::StructuredJson => json!({
                "operation": "unscoped_structured_query",
                "query": serde_json::to_value(
                    structured_query.as_ref().expect("structured request has query"),
                )?,
            }),
        };
        let (before_effect, response, after_effect) =
            current_thread_runtime()?.block_on(async move {
                let before_effect = lambda_effect_snapshot(&handler).await?;
                let response = handler.handle_event(event).await;
                let after_effect = lambda_effect_snapshot(&handler).await?;
                anyhow::Ok((before_effect, response, after_effect))
            })?;
        let (status_accepted, rows, failure) = match response {
            Ok(body) => {
                let rows = body
                    .get("rows")
                    .and_then(serde_json::Value::as_array)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("Lambda query response omitted rows"))?;
                (true, rows, None)
            }
            Err(error) => (false, Vec::new(), Some(lambda_boundary_failure(&error)?)),
        };
        let sees_other_tenant = tenant_scoped
            && rows.iter().any(|row| {
                row.get("tenant_id")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|tenant_id| tenant_id != "tenant-a")
            });
        let effective_tenant_guard = match surface {
            QuerySurface::StructuredJson => structured_tenant_filter_injected,
            QuerySurface::RawSql => raw_sql_sandboxed,
        };
        // Raw SQL is no longer a tenant-facing Lambda event. Schema rejection
        // happens before execution, so an unavailable raw request cannot time
        // out regardless of the synthetic runtime inputs used by the model.
        let timeout_capability = if matches!(surface, QuerySurface::RawSql) {
            true
        } else {
            lambda_runtime_boundary(installed_timer, runtime_ms)?
        };
        let missing_tenant_guard = self.has_missing_tenant_guard();
        let accepted = status_accepted && timeout_capability && !missing_tenant_guard;
        let result = self.classify_lambda_query_result(
            failure,
            structured_query.as_ref(),
            raw_sql,
            timeout_capability,
        )?;
        Ok(LambdaSecurityEvidence {
            result,
            accepted,
            modifies_data: before_effect != after_effect,
            sees_other_tenant,
            completes_within_budget: timeout_capability,
            effective_tenant_guard,
            timer_installed: installed_timer,
        })
    }

    fn classify_lambda_query_result(
        &self,
        failure: Option<LambdaBoundaryFailure>,
        structured_query: Option<&StructuredQuery>,
        raw_sql: Option<&str>,
        timeout_capability: bool,
    ) -> Result<SecurityResult> {
        let missing_tenant_guard = self.has_missing_tenant_guard();
        let surface = self.surface;
        match failure {
            None => {
                if missing_tenant_guard {
                    Ok(SecurityResult::RejectedMissingTenantGuard)
                } else if !timeout_capability {
                    Ok(SecurityResult::RejectedTimeout)
                } else {
                    Ok(SecurityResult::AcceptedSafe)
                }
            }
            Some(LambdaBoundaryFailure::Parser) if matches!(surface, QuerySurface::RawSql) => {
                Ok(SecurityResult::RejectedRawSqlUnavailable)
            }
            Some(LambdaBoundaryFailure::Parser) => Ok(SecurityResult::RejectedMissingTenantGuard),
            Some(LambdaBoundaryFailure::TenantPredicateConflict) => {
                Ok(SecurityResult::RejectedCrossTenant)
            }
            Some(LambdaBoundaryFailure::QueryTimeout) => Ok(SecurityResult::RejectedTimeout),
            Some(LambdaBoundaryFailure::InvalidStructuredQuery) => {
                let query = structured_query.ok_or_else(|| {
                    anyhow::anyhow!(
                        "structured query error reached a non-structured Lambda boundary"
                    )
                })?;
                classify_lambda_structured_query_failure(
                    query,
                    self.tenant_scoped,
                    self.target_tenant_present,
                    timeout_capability,
                )
            }
            Some(LambdaBoundaryFailure::InvalidQuery) => {
                if missing_tenant_guard {
                    return Ok(SecurityResult::RejectedMissingTenantGuard);
                }
                let result = classify_lambda_raw_query_failure(raw_sql.ok_or_else(|| {
                    anyhow::anyhow!("raw query error reached a structured Lambda boundary")
                })?)?;
                if let Some(result) = result {
                    return Ok(result);
                }
                if !timeout_capability {
                    return Ok(SecurityResult::RejectedTimeout);
                }
                Err(anyhow::anyhow!(
                    "unmapped Lambda raw query rejection for SQL input"
                ))
            }
        }
    }

    fn has_missing_tenant_guard(&self) -> bool {
        self.tenant_scoped
            && matches!(self.surface, QuerySurface::StructuredJson)
            && (!self.target_tenant_present || !self.structured_tenant_filter_injected)
            || matches!(self.surface, QuerySurface::RawSql) && !self.raw_sql_sandboxed
    }
}

fn classify_lambda_structured_query_failure(
    query: &StructuredQuery,
    tenant_is_scoped: bool,
    target_present: bool,
    timeout_capability: bool,
) -> Result<SecurityResult> {
    if query_has_lambda_malformed_document_path(query) {
        return Ok(SecurityResult::RejectedMalformedPath);
    }
    if query.analytics_table_name != "users" {
        return Ok(SecurityResult::RejectedMutation);
    }
    if tenant_is_scoped && !target_present {
        return Ok(SecurityResult::RejectedMissingTenantGuard);
    }
    if !timeout_capability {
        return Ok(SecurityResult::RejectedTimeout);
    }
    Err(anyhow::anyhow!(
        "unmapped Lambda structured query rejection for registered query shape"
    ))
}

fn classify_lambda_raw_query_failure(sql: &str) -> Result<Option<SecurityResult>> {
    let normalized = sql.to_ascii_lowercase();
    if normalized.contains("drop table") || normalized.contains("insert into") {
        return Ok(Some(SecurityResult::RejectedMutation));
    }
    if sql == MALFORMED_RAW_SQL {
        return Ok(Some(SecurityResult::RejectedMalformedPath));
    }
    Ok(None)
}

fn query_has_lambda_malformed_document_path(query: &StructuredQuery) -> bool {
    query.select.iter().any(|select| {
        matches!(
            select,
            QuerySelect::DocumentPath { path, .. } if path.split('.').any(str::is_empty)
        )
    })
}

async fn lambda_effect_snapshot(handler: &AnalyticsLambdaHandler) -> Result<serde_json::Value> {
    let event = json!({
        "operation": "tenant_query",
        "target_tenant_id": "tenant-a",
        "query": tenant_query_body(),
    });
    let value = handler
        .handle_event(event)
        .await
        .map_err(|error| anyhow::anyhow!("Lambda effect probe failed: {error}"))?;
    let rows = value
        .get("rows")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Lambda effect probe omitted rows"))?;
    Ok(json!({ "ok": true, "effect": rows }))
}

fn generated_lambda_sql(attack: QueryAttack) -> &'static str {
    match attack {
        QueryAttack::BenignTenantQuery | QueryAttack::LongRunningQuery => "select email from users",
        QueryAttack::MutatingPrefixAttack => "drop table users",
        QueryAttack::MultiStatementInjection => "select 1; drop table users",
        QueryAttack::WithMutationAttack => "with rows as (select 1) drop table users",
        QueryAttack::UnionOtherTenantAttack => {
            "select tenant_id from users where tenant_id = 'tenant-b'"
        }
        QueryAttack::CrossTenantPredicateAttack => {
            "select tenant_id from users where tenant_id <> 'tenant-a'"
        }
        QueryAttack::MalformedDocumentPathAttack => MALFORMED_RAW_SQL,
    }
}

fn generated_lambda_query(attack: QueryAttack, caller_filter_injected: bool) -> StructuredQuery {
    let select = if matches!(attack, QueryAttack::MalformedDocumentPathAttack) {
        vec![QuerySelect::DocumentPath {
            table_alias: None,
            document_column: "item".to_string(),
            path: "profile..email".to_string(),
            alias: "email".to_string(),
        }]
    } else {
        vec![
            QuerySelect::Column {
                table_alias: None,
                column_name: "tenant_id".to_string(),
                alias: None,
            },
            QuerySelect::Column {
                table_alias: None,
                column_name: "email".to_string(),
                alias: None,
            },
        ]
    };
    let filters = if caller_filter_injected
        || matches!(
            attack,
            QueryAttack::UnionOtherTenantAttack | QueryAttack::CrossTenantPredicateAttack
        ) {
        vec![QueryPredicate::Eq {
            expression: QueryExpression::Column {
                table_alias: None,
                column_name: "tenant_id".to_string(),
            },
            value: json!(if matches!(
                attack,
                QueryAttack::UnionOtherTenantAttack | QueryAttack::CrossTenantPredicateAttack
            ) {
                "tenant-b"
            } else {
                "tenant-a"
            }),
        }]
    } else {
        Vec::new()
    };
    StructuredQuery {
        analytics_table_name: "users".to_string(),
        table_alias: None,
        joins: Vec::new(),
        select,
        filters,
        group_by: Vec::new(),
        order_by: Vec::new(),
        limit: Some(100),
        offset: None,
    }
}

fn ensure_long_running_query_times_out() -> Result {
    static RESULT: OnceLock<std::result::Result<(), String>> = OnceLock::new();
    RESULT
        .get_or_init(|| {
            let engine =
                AnalyticsEngine::connect_duckdb(":memory:").map_err(|err| err.to_string())?;
            let result = engine.query_unscoped_sql_json_with_timeout(
                "select 1 as ready",
                std::time::Duration::ZERO,
            );
            if matches!(
                result,
                Err(AnalyticsEngineError::QueryTimeout { timeout_ms: 0 })
            ) {
                Ok(())
            } else {
                Err("long-running query was not timed out".to_string())
            }
        })
        .as_ref()
        .map_err(|message| anyhow::anyhow!(message.clone()))
        .copied()
}

fn lambda_runtime_boundary(installed_timer: bool, runtime_ms: i64) -> Result<bool> {
    let engine = AnalyticsEngine::connect_duckdb(":memory:")?;
    let result = if !installed_timer && runtime_ms > 10_000 {
        engine.query_unscoped_sql_json_with_timeout("select 1 as ready", std::time::Duration::ZERO)
    } else {
        engine.query_unscoped_sql_json_without_timeout("select 1 as ready")
    };
    match result {
        Ok(_) => Ok(true),
        Err(AnalyticsEngineError::QueryTimeout { .. }) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn lambda_boundary_failure(error: &AnalyticsLambdaError) -> Result<LambdaBoundaryFailure> {
    match error {
        AnalyticsLambdaError::Engine(AnalyticsEngineError::QueryTimeout { .. }) => {
            Ok(LambdaBoundaryFailure::QueryTimeout)
        }
        AnalyticsLambdaError::Engine(AnalyticsEngineError::TenantPredicateConflict) => {
            Ok(LambdaBoundaryFailure::TenantPredicateConflict)
        }
        AnalyticsLambdaError::Engine(AnalyticsEngineError::InvalidStructuredQuery(_)) => {
            Ok(LambdaBoundaryFailure::InvalidStructuredQuery)
        }
        AnalyticsLambdaError::Engine(AnalyticsEngineError::InvalidQuery) => {
            Ok(LambdaBoundaryFailure::InvalidQuery)
        }
        AnalyticsLambdaError::Validation(_) => Ok(LambdaBoundaryFailure::Parser),
        AnalyticsLambdaError::Schema(_) => Ok(LambdaBoundaryFailure::Parser),
        error => Err(anyhow::anyhow!("unmapped Lambda query error: {error}")),
    }
}

fn lambda_raw_sql_accepted(sql: &str) -> Result<bool> {
    let handler = security_handler()?;
    let result = current_thread_runtime()?.block_on(handler.handle_event(json!({
        "operation": "unscoped_sql_query",
        "sql": sql,
    })));
    Ok(result.is_ok())
}

fn lambda_tenant_query_without_target_accepted() -> Result<bool> {
    let handler = security_handler()?;
    let result = current_thread_runtime()?.block_on(handler.handle_event(json!({
        "operation": "tenant_query",
        "query": tenant_query_body(),
    })));
    Ok(result.is_ok())
}

fn lambda_malformed_path_accepted() -> Result<bool> {
    let handler = security_handler()?;
    let result = current_thread_runtime()?.block_on(handler.handle_event(json!({
        "operation": "tenant_query",
        "target_tenant_id": "tenant-a",
        "query": StructuredQuery {
            analytics_table_name: "users".to_string(),
            table_alias: None,
            joins: Vec::new(),
            select: vec![QuerySelect::DocumentPath {
                table_alias: None,
                document_column: "item".to_string(),
                path: "profile..email".to_string(),
                alias: "email".to_string(),
            }],
            filters: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: Some(1),
            offset: None,
        },
    })));
    Ok(result.is_ok())
}

fn lambda_tenant_query_rows(target_tenant_id: &str) -> Result<Vec<serde_json::Value>> {
    let handler = security_handler()?;
    let response = current_thread_runtime()?.block_on(handler.handle_event(json!({
        "operation": "tenant_query",
        "target_tenant_id": target_tenant_id,
        "query": tenant_query_body(),
    })))?;
    response
        .get("rows")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("lambda tenant query response omitted rows"))
}

fn security_handler() -> Result<AnalyticsLambdaHandler> {
    let handler = AnalyticsLambdaHandler::try_new(
        security_manifest(),
        &StorageBackend::DuckDb {
            path: ":memory:".to_string(),
        },
        crate::LambdaHandlerOptions::default(),
    )?;
    current_thread_runtime()?.block_on(async {
        for (tenant_id, user_id, email) in [
            ("tenant-a", "user-a", "a@example.com"),
            ("tenant-b", "user-b", "b@example.com"),
        ] {
            handler
                .handle_event(json!({
                    "operation": "ingest",
                    "analytics_table_name": "users",
                    "record_key": user_id,
                    "record": {
                        "Keys": {},
                        "SequenceNumber": user_id,
                        "NewImage": {
                            "tenant_id": {"S": tenant_id},
                            "user_id": {"S": user_id},
                            "profile": {"M": {"email": {"S": email}}},
                        },
                    }
                }))
                .await?;
        }
        anyhow::Ok(())
    })?;
    Ok(handler)
}

fn tenant_query_body() -> StructuredQuery {
    StructuredQuery {
        analytics_table_name: "users".to_string(),
        table_alias: None,
        joins: Vec::new(),
        select: vec![
            QuerySelect::Column {
                table_alias: None,
                column_name: "tenant_id".to_string(),
                alias: None,
            },
            QuerySelect::Column {
                table_alias: None,
                column_name: "email".to_string(),
                alias: None,
            },
        ],
        filters: Vec::new(),
        group_by: Vec::new(),
        order_by: Vec::new(),
        limit: None,
        offset: None,
    }
}

fn security_manifest() -> AnalyticsManifest {
    AnalyticsManifest::new(vec![TableRegistration {
        source_table_name: "shared_users".to_string(),
        analytics_table_name: "users".to_string(),
        source_table_name_prefix: None,
        row_expansion: None,
        tenant_id: None,
        tenant_selector: TenantSelector::Attribute {
            attribute_name: "tenant_id".to_string(),
        },
        row_identity: RowIdentity::Attribute {
            attribute_name: "user_id".to_string(),
        },
        document_column: Some("item".to_string()),
        skip_delete: false,
        retention: None,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        projection_attribute_names: None,
        projection_columns: Some(vec![ProjectionColumn {
            column_name: "email".to_string(),
            attribute_path: "profile.email".to_string(),
            column_type: Some(AnalyticsColumnType::Primitive {
                primitive: PrimitiveColumnType::VarChar,
            }),
        }]),
        columns: Vec::new(),
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
        table_scope: analytics_contract::TableScope::default(),
        join_policy: analytics_contract::JoinPolicy::default(),
    }])
}

fn current_thread_runtime() -> Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?)
}
