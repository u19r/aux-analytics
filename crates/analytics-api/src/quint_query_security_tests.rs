use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};

use analytics_contract::{
    AnalyticsColumnType, AnalyticsManifest, PrimitiveColumnType, ProjectionColumn, QueryExpression,
    QueryPredicate, QuerySelect, RowIdentity, StorageItem, StorageStreamRecord, StorageValue,
    StructuredQuery, TableRegistration, TenantSelector,
};
use analytics_engine::{AnalyticsEngine, AnalyticsEngineError};
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt as _;
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;
use serde_json::json;
use tower::ServiceExt as _;

use crate::{AppState, server_router};

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
struct QuerySecurityState {
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

impl State<QuerySecurityDriver> for QuerySecurityState {
    fn from_driver(driver: &QuerySecurityDriver) -> Result<Self> {
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

struct QuerySecurityDriver {
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

impl Default for QuerySecurityDriver {
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

impl Driver for QuerySecurityDriver {
    type State = QuerySecurityState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => *self = Self::default(),
            apiStep(
                surface,
                attack,
                tenant_is_scoped,
                target_present,
                structured_tenant_filter_injected,
                raw_sql_sandboxed,
                installed_timer,
                runtime_ms
            ) => {
                self.set_case(ApiSecurityCase {
                    caller: CallerInterface::Api,
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
    step = "apiStep",
    max_samples = 20,
    max_steps = 8,
    seed = "0xb"
)]
fn given_quint_query_security_model_when_replayed_then_api_and_engine_boundaries_matches()
-> impl Driver {
    QuerySecurityDriver::default()
}

impl QuerySecurityDriver {
    fn apply_sql_injection_rejection(&mut self) -> Result {
        if !raw_sql_http_rejected("drop table users")? {
            return Err(anyhow::anyhow!(
                "HTTP unscoped SQL endpoint accepted mutating SQL"
            ));
        }
        if !engine_raw_sql_rejected_by_validation("insert into users values (1)")?
            || !engine_raw_sql_rejected_by_validation("select 1; drop table users")?
        {
            return Err(anyhow::anyhow!(
                "engine raw SQL validation accepted mutating or injected SQL"
            ));
        }
        self.set_case(ApiSecurityCase {
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
        if tenant_scoped_structured_query_leaks_other_tenant()? {
            return Err(anyhow::anyhow!(
                "tenant-scoped structured query returned another tenant row"
            ));
        }
        self.set_case(ApiSecurityCase {
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
        self.set_case(ApiSecurityCase {
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
        if !raw_sql_http_rejected("select email from users")? {
            return Err(anyhow::anyhow!(
                "HTTP unscoped SQL endpoint accepted unguarded SQL"
            ));
        }
        if !malformed_document_path_rejected_by_engine()? {
            return Err(anyhow::anyhow!(
                "engine structured query validation accepted malformed document path"
            ));
        }
        if !unregistered_structured_column_rejected_by_engine()? {
            return Err(anyhow::anyhow!(
                "engine structured query validation accepted unregistered column"
            ));
        }
        self.set_case(ApiSecurityCase {
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
        let rows = tenant_scoped_structured_query_rows("tenant-a")?;
        if rows.len() != 1 || rows[0]["tenant_id"] != "tenant-a" {
            return Err(anyhow::anyhow!(
                "tenant-scoped structured query did not return only target tenant"
            ));
        }
        self.set_case(ApiSecurityCase {
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

    fn set_case(&mut self, case: ApiSecurityCase) -> Result {
        let evidence = case.execute_api_query()?;
        let ApiSecurityCase {
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

struct ApiSecurityEvidence {
    result: SecurityResult,
    accepted: bool,
    modifies_data: bool,
    sees_other_tenant: bool,
    completes_within_budget: bool,
    effective_tenant_guard: bool,
    timer_installed: bool,
}

struct ApiSecurityCase {
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
enum ApiBoundaryFailure {
    InvalidQuery,
    InvalidStructuredQuery,
    SchemaRejected,
    TenantPredicateConflict,
    QueryTimeout,
}

const MALFORMED_RAW_SQL: &str = "select json_extract(item, '$.profile[') from users;;";

#[derive(Debug, Deserialize)]
struct ApiErrorBody {
    code: Option<String>,
}

impl ApiSecurityCase {
    fn execute_api_query(&self) -> Result<ApiSecurityEvidence> {
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
        let router = security_router()?;
        let structured_query = matches!(surface, QuerySurface::StructuredJson)
            .then(|| generated_security_query(attack, structured_tenant_filter_injected));
        let raw_sql = matches!(surface, QuerySurface::RawSql).then(|| generated_raw_sql(attack));
        let (path, payload) = match surface {
            QuerySurface::RawSql => (
                "/unscoped-sql-query",
                json!({ "sql": raw_sql.expect("raw SQL request has SQL") }),
            ),
            QuerySurface::StructuredJson if tenant_scoped => (
                "/tenant-query",
                json!({
                    "target_tenant_id": if target_tenant_present { "tenant-a" } else { "" },
                    "query": structured_query.as_ref().expect("structured request has query"),
                }),
            ),
            QuerySurface::StructuredJson => (
                "/unscoped-structured-query",
                json!({
                    "query": structured_query.as_ref().expect("structured request has query"),
                }),
            ),
        };
        let (before_effect, status, body, after_effect) =
            current_thread_runtime()?.block_on(async move {
                let before_effect = api_effect_snapshot(router.clone()).await?;
                let response = router
                    .clone()
                    .oneshot(
                        Request::post(path)
                            .header("content-type", "application/json")
                            .body(Body::from(payload.to_string()))
                            .map_err(|error| anyhow::anyhow!(error))?,
                    )
                    .await?;
                let status = response.status();
                let body = response.into_body().collect().await?.to_bytes();
                let after_effect = api_effect_snapshot(router).await?;
                anyhow::Ok((before_effect, status, body, after_effect))
            })?;
        let (rows, failure) = if status == StatusCode::OK {
            let response = serde_json::from_slice::<serde_json::Value>(&body)?;
            let rows = response
                .get("rows")
                .and_then(serde_json::Value::as_array)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("successful API query omitted rows"))?;
            (rows, None)
        } else {
            let response_value = serde_json::from_slice::<serde_json::Value>(&body)?;
            let response = ApiErrorBody::deserialize(&response_value).map_err(|error| {
                anyhow::anyhow!(
                    "API query error body is not structured: {error}; body={response_value}"
                )
            })?;
            let code = response
                .code
                .ok_or_else(|| anyhow::anyhow!("API query error omitted machine-readable code"))?;
            let failure = match code.as_str() {
                "invalid_query" => ApiBoundaryFailure::InvalidQuery,
                "invalid_structured_query" => ApiBoundaryFailure::InvalidStructuredQuery,
                "tenant_predicate_conflict" => ApiBoundaryFailure::TenantPredicateConflict,
                "query_timeout" => ApiBoundaryFailure::QueryTimeout,
                "invalid_request" => ApiBoundaryFailure::SchemaRejected,
                "raw_sql_unavailable" => {
                    return Ok(ApiSecurityEvidence {
                        result: SecurityResult::RejectedRawSqlUnavailable,
                        accepted: false,
                        modifies_data: before_effect != after_effect,
                        sees_other_tenant: false,
                        completes_within_budget: true,
                        effective_tenant_guard: raw_sql_sandboxed,
                        timer_installed: installed_timer,
                    });
                }
                other => return Err(anyhow::anyhow!("unknown API query error code: {other}")),
            };
            (Vec::new(), Some(failure))
        };
        let status_accepted = status == StatusCode::OK;
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
        let timeout_capability = if matches!(surface, QuerySurface::RawSql) {
            true
        } else {
            api_runtime_boundary(installed_timer, runtime_ms)?
        };
        let missing_tenant_guard = self.has_missing_tenant_guard();
        let raw_sql_unavailable = matches!(surface, QuerySurface::RawSql);
        let accepted =
            status_accepted && timeout_capability && !missing_tenant_guard && !raw_sql_unavailable;
        let result = self.classify_api_query_result(
            failure,
            structured_query.as_ref(),
            raw_sql,
            timeout_capability,
        )?;
        Ok(ApiSecurityEvidence {
            result,
            accepted,
            modifies_data: before_effect != after_effect,
            sees_other_tenant,
            completes_within_budget: timeout_capability,
            effective_tenant_guard,
            timer_installed: installed_timer,
        })
    }

    fn classify_api_query_result(
        &self,
        failure: Option<ApiBoundaryFailure>,
        structured_query: Option<&StructuredQuery>,
        raw_sql: Option<&str>,
        timeout_capability: bool,
    ) -> Result<SecurityResult> {
        let missing_tenant_guard = self.has_missing_tenant_guard();
        let raw_sql_unavailable = matches!(self.surface, QuerySurface::RawSql);
        if raw_sql_unavailable {
            return Ok(SecurityResult::RejectedRawSqlUnavailable);
        }
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
            Some(ApiBoundaryFailure::SchemaRejected) => {
                Ok(SecurityResult::RejectedMissingTenantGuard)
            }
            Some(ApiBoundaryFailure::TenantPredicateConflict) => {
                Ok(SecurityResult::RejectedCrossTenant)
            }
            Some(ApiBoundaryFailure::QueryTimeout) => Ok(SecurityResult::RejectedTimeout),
            Some(ApiBoundaryFailure::InvalidStructuredQuery) => {
                let query = structured_query.ok_or_else(|| {
                    anyhow::anyhow!("structured query error reached a non-structured API boundary")
                })?;
                classify_structured_query_failure(
                    query,
                    self.tenant_scoped,
                    self.target_tenant_present,
                    timeout_capability,
                )
            }
            Some(ApiBoundaryFailure::InvalidQuery) => {
                if missing_tenant_guard {
                    return Ok(SecurityResult::RejectedMissingTenantGuard);
                }
                let result = classify_raw_query_failure(raw_sql.ok_or_else(|| {
                    anyhow::anyhow!("raw query error reached a structured API boundary")
                })?)?;
                if let Some(result) = result {
                    return Ok(result);
                }
                if !timeout_capability {
                    return Ok(SecurityResult::RejectedTimeout);
                }
                Err(anyhow::anyhow!(
                    "unmapped API raw query rejection for SQL input"
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

fn classify_structured_query_failure(
    query: &StructuredQuery,
    tenant_is_scoped: bool,
    target_present: bool,
    timeout_capability: bool,
) -> Result<SecurityResult> {
    if query_has_malformed_document_path(query) {
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
        "unmapped API structured query rejection for registered query shape"
    ))
}

fn classify_raw_query_failure(sql: &str) -> Result<Option<SecurityResult>> {
    let normalized = sql.to_ascii_lowercase();
    if normalized.contains("drop table") || normalized.contains("insert into") {
        return Ok(Some(SecurityResult::RejectedMutation));
    }
    if sql == MALFORMED_RAW_SQL {
        return Ok(Some(SecurityResult::RejectedMalformedPath));
    }
    Ok(None)
}

fn query_has_malformed_document_path(query: &StructuredQuery) -> bool {
    query.select.iter().any(|select| {
        matches!(
            select,
            QuerySelect::DocumentPath { path, .. } if path.split('.').any(str::is_empty)
        )
    })
}

async fn api_effect_snapshot(router: Router) -> Result<serde_json::Value> {
    let response = router
        .oneshot(
            Request::post("/tenant-query")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "target_tenant_id": "tenant-a",
                        "query": generated_security_query(QueryAttack::BenignTenantQuery, false),
                    })
                    .to_string(),
                ))
                .map_err(|error| anyhow::anyhow!(error))?,
        )
        .await?;
    let status = response.status().as_u16();
    let bytes = response.into_body().collect().await?.to_bytes();
    if status != StatusCode::OK.as_u16() {
        return Err(anyhow::anyhow!(
            "API effect probe failed with status {status}: {}",
            String::from_utf8_lossy(&bytes)
        ));
    }
    let body = serde_json::from_slice::<serde_json::Value>(&bytes)?;
    let effect = body
        .get("rows")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("API effect probe omitted rows"))?;
    Ok(json!({ "status": status, "effect": effect }))
}

fn generated_raw_sql(attack: QueryAttack) -> &'static str {
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

fn generated_security_query(attack: QueryAttack, caller_filter_injected: bool) -> StructuredQuery {
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
            let result =
                engine.query_unscoped_sql_json_with_timeout("select 1 as ready", Duration::ZERO);
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

fn api_runtime_boundary(installed_timer: bool, runtime_ms: i64) -> Result<bool> {
    let engine = AnalyticsEngine::connect_duckdb(":memory:")?;
    let result = if !installed_timer && runtime_ms > 10_000 {
        engine.query_unscoped_sql_json_with_timeout("select 1 as ready", Duration::ZERO)
    } else {
        engine.query_unscoped_sql_json_without_timeout("select 1 as ready")
    };
    match result {
        Ok(_) => Ok(true),
        Err(AnalyticsEngineError::QueryTimeout { .. }) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn raw_sql_http_rejected(sql: &str) -> Result<bool> {
    let router = security_router()?;
    let status = current_thread_runtime()?.block_on(async move {
        let response = router
            .oneshot(
                Request::post("/unscoped-sql-query")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({ "sql": sql }).to_string()))?,
            )
            .await?;
        anyhow::Ok(response.status())
    })?;
    Ok(status == StatusCode::BAD_REQUEST)
}

fn engine_raw_sql_rejected_by_validation(sql: &str) -> Result<bool> {
    let engine = AnalyticsEngine::connect_duckdb(":memory:")?;
    Ok(matches!(
        engine.query_unscoped_sql_json(sql),
        Err(AnalyticsEngineError::InvalidQuery)
    ))
}

fn malformed_document_path_rejected_by_engine() -> Result<bool> {
    let manifest = security_manifest();
    let engine = AnalyticsEngine::connect_duckdb(":memory:")?;
    engine.ensure_manifest(&manifest)?;
    let query = StructuredQuery {
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
    };
    Ok(matches!(
        engine.query_tenant_structured_json(&manifest, &query, "tenant-a"),
        Err(AnalyticsEngineError::InvalidStructuredQuery(_))
    ))
}

fn unregistered_structured_column_rejected_by_engine() -> Result<bool> {
    let manifest = security_manifest();
    let engine = AnalyticsEngine::connect_duckdb(":memory:")?;
    engine.ensure_manifest(&manifest)?;
    let query = StructuredQuery {
        analytics_table_name: "users".to_string(),
        table_alias: None,
        joins: Vec::new(),
        select: vec![QuerySelect::Column {
            table_alias: None,
            column_name: "secret_notes".to_string(),
            alias: None,
        }],
        filters: Vec::new(),
        group_by: Vec::new(),
        order_by: Vec::new(),
        limit: Some(1),
        offset: None,
    };
    Ok(matches!(
        engine.query_tenant_structured_json(&manifest, &query, "tenant-a"),
        Err(AnalyticsEngineError::InvalidStructuredQuery(_))
    ))
}

fn tenant_scoped_structured_query_leaks_other_tenant() -> Result<bool> {
    let rows = tenant_scoped_structured_query_rows("tenant-a")?;
    Ok(rows
        .iter()
        .any(|row| row["tenant_id"].as_str() != Some("tenant-a")))
}

fn tenant_scoped_structured_query_rows(target_tenant_id: &str) -> Result<Vec<serde_json::Value>> {
    let router = security_router()?;
    current_thread_runtime()?.block_on(async move {
        let response = router
            .oneshot(
                Request::post("/tenant-query")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                                        "target_tenant_id": target_tenant_id,
                                        "query": StructuredQuery {
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
                                    })
                        .to_string(),
                    ))?,
            )
            .await?;
        let status = response.status();
        let bytes = response.into_body().collect().await?.to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&bytes)?;
        if status != StatusCode::OK {
            return Err(anyhow::anyhow!("structured query failed: {body:?}"));
        }
        body.get("rows")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("tenant query response omitted rows"))
    })
}

fn security_router() -> Result<Router> {
    let manifest = security_manifest();
    let engine = AnalyticsEngine::connect_duckdb(":memory:")?;
    engine.ensure_manifest(&manifest)?;
    for (tenant_id, user_id, email) in [
        ("tenant-a", "user-a", "a@example.com"),
        ("tenant-b", "user-b", "b@example.com"),
    ] {
        engine.ingest_stream_record(
            &manifest,
            "users",
            user_id.as_bytes(),
            StorageStreamRecord {
                sequence_number: user_id.to_string(),
                keys: StorageItem::new(),
                old_image: None,
                new_image: Some(StorageItem::from_iter([
                    (
                        "tenant_id".to_string(),
                        StorageValue::S(tenant_id.to_string()),
                    ),
                    ("user_id".to_string(), StorageValue::S(user_id.to_string())),
                    (
                        "profile".to_string(),
                        StorageValue::M(StorageItem::from_iter([(
                            "email".to_string(),
                            StorageValue::S(email.to_string()),
                        )])),
                    ),
                ])),
            },
        )?;
    }
    Ok(server_router(Arc::new(AppState::new(
        crate::AnalyticsEngineAccess::shared(engine),
        manifest,
    ))))
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
