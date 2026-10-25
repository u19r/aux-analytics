use std::{error::Error as _, sync::OnceLock};

use analytics_contract::{
    AnalyticsColumnType, AnalyticsManifest, PrimitiveColumnType, ProjectionColumn, QueryExpression,
    QueryPredicate, QuerySelect, RowIdentity, StorageItem, StorageStreamRecord, StorageValue,
    StructuredQuery, TableRegistration, TenantSelector,
};
use analytics_engine::{AnalyticsEngine, AnalyticsEngineError};
use clap::{Parser as _, error::ErrorKind};
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::{
    app::run,
    cli::Cli,
    error::{CliError, CliErrorKind},
};

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
struct CliQuerySecurityState {
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

impl State<CliQuerySecurityDriver> for CliQuerySecurityState {
    fn from_driver(driver: &CliQuerySecurityDriver) -> Result<Self> {
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

struct CliQuerySecurityDriver {
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

impl Default for CliQuerySecurityDriver {
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

impl Driver for CliQuerySecurityDriver {
    type State = CliQuerySecurityState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => *self = Self::default(),
            cliStep(
                surface,
                attack,
                tenant_is_scoped,
                target_present,
                structured_tenant_filter_injected,
                raw_sql_sandboxed,
                installed_timer,
                runtime_ms
            ) => {
                self.set_case(CliSecurityCase {
                    caller: CallerInterface::Cli,
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
    step = "cliStep",
    max_samples = 20,
    max_steps = 8,
    seed = "0xb"
)]
fn given_quint_query_security_model_when_replayed_then_cli_boundaries_matches() -> impl Driver {
    CliQuerySecurityDriver::default()
}

impl CliQuerySecurityDriver {
    fn apply_sql_injection_rejection(&mut self) -> Result {
        if cli_generated_raw_sql_result("insert into users values (1)")?.0
            || cli_generated_raw_sql_result("select 1; drop table users")?.0
        {
            return Err(anyhow::anyhow!(
                "CLI unscoped SQL command accepted mutating or injected SQL"
            ));
        }
        self.set_case(CliSecurityCase {
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
        let rows = cli_tenant_query_rows("tenant-a")?;
        if rows
            .iter()
            .any(|row| row["tenant_id"].as_str() != Some("tenant-a"))
        {
            return Err(anyhow::anyhow!(
                "CLI tenant query returned another tenant row"
            ));
        }
        self.set_case(CliSecurityCase {
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
        self.set_case(CliSecurityCase {
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
        if cli_tenant_query_without_target_accepted()? || cli_malformed_path_accepted()? {
            return Err(anyhow::anyhow!(
                "CLI structured query accepted missing tenant guard or malformed path"
            ));
        }
        self.set_case(CliSecurityCase {
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
        let rows = cli_tenant_query_rows("tenant-a")?;
        if rows.len() != 1 || rows[0]["tenant_id"] != "tenant-a" {
            return Err(anyhow::anyhow!(
                "CLI tenant query did not return only target tenant"
            ));
        }
        self.set_case(CliSecurityCase {
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

    fn set_case(&mut self, case: CliSecurityCase) -> Result {
        let evidence = case.execute_cli_query()?;
        let CliSecurityCase {
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

struct CliSecurityEvidence {
    result: SecurityResult,
    accepted: bool,
    modifies_data: bool,
    sees_other_tenant: bool,
    completes_within_budget: bool,
    effective_tenant_guard: bool,
    timer_installed: bool,
}

struct CliSecurityCase {
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
enum CliBoundaryFailure {
    InvalidQuery,
    InvalidStructuredQuery,
    TenantPredicateConflict,
    QueryTimeout,
    Parser(ErrorKind),
}

const MALFORMED_RAW_SQL: &str = "select json_extract(item, '$.profile[') from users;;";

impl CliSecurityCase {
    fn execute_cli_query(&self) -> Result<CliSecurityEvidence> {
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
        let structured_query = matches!(surface, QuerySurface::StructuredJson)
            .then(|| generated_cli_query(attack, structured_tenant_filter_injected));
        let raw_sql = matches!(surface, QuerySurface::RawSql).then(|| generated_cli_sql(attack));
        let (status_accepted, rows, failure, modifies_data) = match surface {
            QuerySurface::RawSql => {
                cli_generated_raw_sql_result(raw_sql.expect("raw SQL request has SQL"))?
            }
            QuerySurface::StructuredJson if tenant_scoped && !target_tenant_present => {
                let (accepted, rows, failure, modifies_data) =
                    cli_tenant_query_without_target_result(
                        structured_query
                            .as_ref()
                            .expect("structured request has query"),
                    )?;
                (accepted, rows, failure, modifies_data)
            }
            QuerySurface::StructuredJson if !tenant_scoped => cli_generated_unscoped_query_result(
                structured_query
                    .as_ref()
                    .expect("structured request has query"),
            )?,
            QuerySurface::StructuredJson => cli_generated_tenant_query_result(
                "tenant-a",
                structured_query
                    .as_ref()
                    .expect("structured request has query"),
            )?,
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
        let timeout_capability = cli_runtime_boundary(installed_timer, runtime_ms)?;
        let missing_tenant_guard = self.has_missing_tenant_guard();
        let accepted = status_accepted && timeout_capability && !missing_tenant_guard;
        let result = self.classify_cli_query_result(
            failure,
            structured_query.as_ref(),
            raw_sql,
            timeout_capability,
        )?;
        Ok(CliSecurityEvidence {
            result,
            accepted,
            modifies_data,
            sees_other_tenant,
            completes_within_budget: timeout_capability,
            effective_tenant_guard,
            timer_installed: installed_timer,
        })
    }

    fn classify_cli_query_result(
        &self,
        failure: Option<CliBoundaryFailure>,
        structured_query: Option<&StructuredQuery>,
        raw_sql: Option<&str>,
        timeout_capability: bool,
    ) -> Result<SecurityResult> {
        let missing_tenant_guard = self.has_missing_tenant_guard();
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
            Some(CliBoundaryFailure::Parser(_)) => Ok(SecurityResult::RejectedMissingTenantGuard),
            Some(CliBoundaryFailure::TenantPredicateConflict) => {
                Ok(SecurityResult::RejectedCrossTenant)
            }
            Some(CliBoundaryFailure::QueryTimeout) => Ok(SecurityResult::RejectedTimeout),
            Some(CliBoundaryFailure::InvalidStructuredQuery) => {
                let query = structured_query.ok_or_else(|| {
                    anyhow::anyhow!("structured query error reached a non-structured CLI boundary")
                })?;
                classify_cli_structured_query_failure(
                    query,
                    self.tenant_scoped,
                    self.target_tenant_present,
                    timeout_capability,
                )
            }
            Some(CliBoundaryFailure::InvalidQuery) => {
                if missing_tenant_guard {
                    return Ok(SecurityResult::RejectedMissingTenantGuard);
                }
                let result = classify_cli_raw_query_failure(raw_sql.ok_or_else(|| {
                    anyhow::anyhow!("raw query error reached a structured CLI boundary")
                })?)?;
                if let Some(result) = result {
                    return Ok(result);
                }
                if !timeout_capability {
                    return Ok(SecurityResult::RejectedTimeout);
                }
                Err(anyhow::anyhow!(
                    "unmapped CLI raw query rejection for SQL input"
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

fn classify_cli_structured_query_failure(
    query: &StructuredQuery,
    tenant_is_scoped: bool,
    target_present: bool,
    timeout_capability: bool,
) -> Result<SecurityResult> {
    if query_has_cli_malformed_document_path(query) {
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
        "unmapped CLI structured query rejection for registered query shape"
    ))
}

fn classify_cli_raw_query_failure(sql: &str) -> Result<Option<SecurityResult>> {
    let normalized = sql.to_ascii_lowercase();
    if normalized.contains("drop table") || normalized.contains("insert into") {
        return Ok(Some(SecurityResult::RejectedMutation));
    }
    if sql == MALFORMED_RAW_SQL {
        return Ok(Some(SecurityResult::RejectedMalformedPath));
    }
    Ok(None)
}

fn query_has_cli_malformed_document_path(query: &StructuredQuery) -> bool {
    query.select.iter().any(|select| {
        matches!(
            select,
            QuerySelect::DocumentPath { path, .. } if path.split('.').any(str::is_empty)
        )
    })
}

fn generated_cli_query(attack: QueryAttack, caller_filter_injected: bool) -> StructuredQuery {
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
            value: serde_json::json!(if matches!(
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

fn generated_cli_sql(attack: QueryAttack) -> &'static str {
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

fn cli_runtime_boundary(installed_timer: bool, runtime_ms: i64) -> Result<bool> {
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

fn cli_boundary_failure(error: &CliError) -> Result<CliBoundaryFailure> {
    if let Some(source) = error.source()
        && let Some(engine_error) = source.downcast_ref::<AnalyticsEngineError>()
    {
        return match engine_error {
            AnalyticsEngineError::QueryTimeout { .. } => Ok(CliBoundaryFailure::QueryTimeout),
            AnalyticsEngineError::TenantPredicateConflict => {
                Ok(CliBoundaryFailure::TenantPredicateConflict)
            }
            AnalyticsEngineError::InvalidStructuredQuery(_) => {
                Ok(CliBoundaryFailure::InvalidStructuredQuery)
            }
            AnalyticsEngineError::InvalidQuery => Ok(CliBoundaryFailure::InvalidQuery),
            error => Err(anyhow::anyhow!(
                "unmapped CLI engine query error: {error:?}"
            )),
        };
    }
    match error.kind {
        CliErrorKind::StructuredQueryValidation | CliErrorKind::ManifestValidation => {
            Ok(CliBoundaryFailure::InvalidStructuredQuery)
        }
        kind => Err(anyhow::anyhow!(
            "unmapped CLI query error kind: {kind:?}: {error}"
        )),
    }
}

fn cli_generated_raw_sql_result(
    sql: &str,
) -> Result<(
    bool,
    Vec<serde_json::Value>,
    Option<CliBoundaryFailure>,
    bool,
)> {
    let environment = SecurityCliEnvironment::try_new()?;
    let before_effect = environment.effect_snapshot()?;
    let cli = Cli::parse_from([
        "aux-analytics",
        "unscoped-sql-query",
        "--duckdb",
        environment.db_path(),
        "--sql",
        sql,
    ]);
    let result = match current_thread_runtime()?.block_on(run(cli)) {
        Ok(output) => {
            let output = output.ok_or_else(|| anyhow::anyhow!("CLI raw query omitted output"))?;
            let value = serde_json::from_str::<serde_json::Value>(&output)?;
            let rows = value
                .as_array()
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("CLI raw query output was not an array"))?;
            (true, rows, None)
        }
        Err(error) => (false, Vec::new(), Some(cli_boundary_failure(&error)?)),
    };
    let after_effect = environment.effect_snapshot()?;
    Ok((result.0, result.1, result.2, before_effect != after_effect))
}

fn cli_tenant_query_without_target_accepted() -> Result<bool> {
    Ok(cli_tenant_query_without_target_result(&tenant_query_body())?.0)
}

fn cli_tenant_query_without_target_result(
    query: &StructuredQuery,
) -> Result<(
    bool,
    Vec<serde_json::Value>,
    Option<CliBoundaryFailure>,
    bool,
)> {
    let environment = SecurityCliEnvironment::try_new()?;
    let query_path = environment.write_query(query)?;
    let before_effect = environment.effect_snapshot()?;
    let result = match Cli::try_parse_from([
        "aux-analytics",
        "tenant-query",
        "--manifest",
        environment.manifest_path(),
        "--query",
        path_str(query_path.path())?,
        "--duckdb",
        environment.db_path(),
    ]) {
        Ok(_) => (true, Vec::new(), None),
        Err(error) => (
            false,
            Vec::new(),
            Some(CliBoundaryFailure::Parser(error.kind())),
        ),
    };
    let after_effect = environment.effect_snapshot()?;
    Ok((result.0, result.1, result.2, before_effect != after_effect))
}

fn cli_generated_tenant_query_result(
    target_tenant_id: &str,
    query: &StructuredQuery,
) -> Result<(
    bool,
    Vec<serde_json::Value>,
    Option<CliBoundaryFailure>,
    bool,
)> {
    let environment = SecurityCliEnvironment::try_new()?;
    let before_effect = environment.effect_snapshot()?;
    let query_path = environment.write_query(query)?;
    let cli = Cli::parse_from([
        "aux-analytics",
        "tenant-query",
        "--target-tenant-id",
        target_tenant_id,
        "--manifest",
        environment.manifest_path(),
        "--query",
        path_str(query_path.path())?,
        "--duckdb",
        environment.db_path(),
    ]);
    let result = match current_thread_runtime()?.block_on(run(cli)) {
        Ok(output) => {
            let output =
                output.ok_or_else(|| anyhow::anyhow!("CLI tenant query omitted output"))?;
            let value = serde_json::from_str::<serde_json::Value>(&output)?;
            let rows = value
                .get("rows")
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("CLI tenant query omitted rows"))
                .and_then(|rows| {
                    serde_json::from_value::<Vec<serde_json::Value>>(rows)
                        .map_err(anyhow::Error::from)
                })?;
            (true, rows, None)
        }
        Err(error) => (false, Vec::new(), Some(cli_boundary_failure(&error)?)),
    };
    let after_effect = environment.effect_snapshot()?;
    Ok((result.0, result.1, result.2, before_effect != after_effect))
}

fn cli_generated_unscoped_query_result(
    query: &StructuredQuery,
) -> Result<(
    bool,
    Vec<serde_json::Value>,
    Option<CliBoundaryFailure>,
    bool,
)> {
    let environment = SecurityCliEnvironment::try_new()?;
    let before_effect = environment.effect_snapshot()?;
    let query_path = environment.write_query(query)?;
    let cli = Cli::parse_from([
        "aux-analytics",
        "unscoped-structured-query",
        "--manifest",
        environment.manifest_path(),
        "--query",
        path_str(query_path.path())?,
        "--duckdb",
        environment.db_path(),
    ]);
    let result = match current_thread_runtime()?.block_on(run(cli)) {
        Ok(output) => {
            let output =
                output.ok_or_else(|| anyhow::anyhow!("CLI unscoped query omitted output"))?;
            let value = serde_json::from_str::<serde_json::Value>(&output)?;
            let rows = value
                .get("rows")
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("CLI unscoped query omitted rows"))
                .and_then(|rows| {
                    serde_json::from_value::<Vec<serde_json::Value>>(rows)
                        .map_err(anyhow::Error::from)
                })?;
            (true, rows, None)
        }
        Err(error) => (false, Vec::new(), Some(cli_boundary_failure(&error)?)),
    };
    let after_effect = environment.effect_snapshot()?;
    Ok((result.0, result.1, result.2, before_effect != after_effect))
}

fn cli_malformed_path_accepted() -> Result<bool> {
    let environment = SecurityCliEnvironment::try_new()?;
    let malformed_query = malformed_path_query();
    let malformed_query_path = environment.write_query(&malformed_query)?;
    let cli = Cli::parse_from([
        "aux-analytics",
        "tenant-query",
        "--target-tenant-id",
        "tenant-a",
        "--manifest",
        environment.manifest_path(),
        "--query",
        path_str(malformed_query_path.path())?,
        "--duckdb",
        environment.db_path(),
    ]);
    Ok(current_thread_runtime()?.block_on(run(cli)).is_ok())
}

fn cli_tenant_query_rows(target_tenant_id: &str) -> Result<Vec<serde_json::Value>> {
    let environment = SecurityCliEnvironment::try_new()?;
    let cli = Cli::parse_from([
        "aux-analytics",
        "tenant-query",
        "--target-tenant-id",
        target_tenant_id,
        "--manifest",
        environment.manifest_path(),
        "--query",
        environment.query_path(),
        "--duckdb",
        environment.db_path(),
    ]);
    let output = current_thread_runtime()?
        .block_on(run(cli))?
        .ok_or_else(|| anyhow::anyhow!("tenant query returned no output"))?;
    let response = serde_json::from_str::<serde_json::Value>(output.as_str())?;
    response
        .get("rows")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("tenant query response did not include rows"))
        .and_then(|rows| Ok(serde_json::from_value::<Vec<serde_json::Value>>(rows)?))
}

struct SecurityCliEnvironment {
    tempdir: tempfile::TempDir,
    db_path: String,
    manifest_path: String,
    query_path: String,
}

impl SecurityCliEnvironment {
    fn try_new() -> Result<Self> {
        let tempdir = tempfile::tempdir()?;
        let db_path = path_str(&tempdir.path().join("analytics.duckdb"))?.to_string();
        let manifest_path = path_str(&tempdir.path().join("manifest.json"))?.to_string();
        let query_path = path_str(&tempdir.path().join("query.json"))?.to_string();
        let manifest = security_manifest();
        std::fs::write(manifest_path.as_str(), serde_json::to_string(&manifest)?)?;
        std::fs::write(
            query_path.as_str(),
            serde_json::to_string(&tenant_query_body())?,
        )?;
        let engine = AnalyticsEngine::connect_duckdb(db_path.as_str())?;
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
        drop(engine);
        Ok(Self {
            tempdir,
            db_path,
            manifest_path,
            query_path,
        })
    }

    fn write_query(&self, query: &StructuredQuery) -> Result<tempfile::NamedTempFile> {
        let file = tempfile::NamedTempFile::new_in(self.tempdir.path())?;
        std::fs::write(file.path(), serde_json::to_string(&query)?)?;
        Ok(file)
    }

    fn db_path(&self) -> &str {
        self.db_path.as_str()
    }

    fn manifest_path(&self) -> &str {
        self.manifest_path.as_str()
    }

    fn query_path(&self) -> &str {
        self.query_path.as_str()
    }

    fn effect_snapshot(&self) -> Result<serde_json::Value> {
        let engine = AnalyticsEngine::connect_duckdb(self.db_path())?;
        let rows = engine.query_unscoped_sql_json(
            "select tenant_id, email, __source_position from users
                 order by tenant_id, email, __source_position",
        )?;
        Ok(serde_json::json!({ "rows": rows }))
    }
}

fn path_str(path: &std::path::Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| anyhow::anyhow!("path is not UTF-8: {path:?}"))
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

fn malformed_path_query() -> StructuredQuery {
    StructuredQuery {
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
