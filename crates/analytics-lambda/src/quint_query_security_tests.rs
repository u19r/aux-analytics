use analytics_contract::{
    AnalyticsColumnType, AnalyticsManifest, PrimitiveColumnType, ProjectionColumn, QuerySelect,
    RowIdentity, StructuredQuery, TableRegistration, TenantSelector,
};
use analytics_engine::{AnalyticsEngine, AnalyticsEngineError, StorageBackend};
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;
use serde_json::json;

use crate::AnalyticsLambdaHandler;

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
    RejectedTimeout,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
#[allow(clippy::struct_excessive_bools)]
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
        })
    }
}

#[allow(clippy::struct_excessive_bools)]
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
        }
    }
}

impl Driver for LambdaQuerySecurityDriver {
    type State = LambdaQuerySecurityState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => *self = Self::default(),
            GenerateSecurityQuery(
                surface,
                attack,
                tenant_is_scoped,
                target_present,
                structured_tenant_filter_injected,
                raw_sql_sandboxed,
                installed_timer,
                runtime_ms
            ) => {
                self.apply_generated_case(
                    surface,
                    attack,
                    tenant_is_scoped,
                    target_present,
                    structured_tenant_filter_injected,
                    raw_sql_sandboxed,
                    installed_timer,
                    runtime_ms,
                );
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
    max_samples = 20,
    max_steps = 8,
    seed = "0xb"
)]
fn quint_query_security_model_matches_lambda_boundaries() -> impl Driver {
    LambdaQuerySecurityDriver::default()
}

impl LambdaQuerySecurityDriver {
    #[allow(clippy::fn_params_excessive_bools, clippy::too_many_arguments)]
    fn apply_generated_case(
        &mut self,
        surface: QuerySurface,
        attack: QueryAttack,
        tenant_is_scoped: bool,
        target_present: bool,
        structured_tenant_filter_injected: bool,
        raw_sql_sandboxed: bool,
        installed_timer: bool,
        runtime_ms: i64,
    ) {
        let accepted = query_accepted_by_security_policy(
            surface,
            attack,
            tenant_is_scoped,
            target_present,
            structured_tenant_filter_injected,
            raw_sql_sandboxed,
            installed_timer,
            runtime_ms,
        );
        self.accepted = accepted;
        self.modifies_data = accepted && attack_attempts_mutation(attack);
        self.sees_other_tenant =
            accepted && tenant_is_scoped && attack_attempts_other_tenant(attack);
        self.completes_within_budget = runtime_within_budget(runtime_ms, installed_timer);
        self.tenant_scoped = tenant_is_scoped;
        self.target_tenant_present = target_present;
        self.effective_tenant_guard = tenant_guard_effective(
            surface,
            structured_tenant_filter_injected,
            raw_sql_sandboxed,
        );
        self.timer_installed = installed_timer;
        self.last_result = security_result(
            surface,
            attack,
            tenant_is_scoped,
            target_present,
            structured_tenant_filter_injected,
            raw_sql_sandboxed,
            installed_timer,
            runtime_ms,
        );
    }

    fn apply_sql_injection_rejection(&mut self) -> Result {
        if lambda_raw_sql_accepted("insert into users values (1)")?
            || lambda_raw_sql_accepted("select 1; drop table users")?
        {
            return Err(anyhow::anyhow!(
                "lambda unscoped SQL event accepted mutating or injected SQL"
            ));
        }
        self.rejected(SecurityResult::RejectedMutation, true, true, true, true);
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
        self.rejected(SecurityResult::RejectedCrossTenant, true, true, true, true);
        Ok(())
    }

    fn apply_timeout_rejection(&mut self) -> Result {
        let engine = AnalyticsEngine::connect_duckdb(":memory:")?;
        let result = engine.query_unscoped_sql_json_with_timeout(
            "select sum(i * j) as total from range(100000000) a(i), range(100000000) b(j)",
            std::time::Duration::from_millis(1),
        );
        if !matches!(
            result,
            Err(AnalyticsEngineError::QueryTimeout { timeout_ms: 1 })
        ) {
            return Err(anyhow::anyhow!("long-running query was not timed out"));
        }
        self.accepted = false;
        self.modifies_data = false;
        self.sees_other_tenant = false;
        self.completes_within_budget = false;
        self.tenant_scoped = true;
        self.target_tenant_present = true;
        self.effective_tenant_guard = true;
        self.timer_installed = false;
        self.last_result = SecurityResult::RejectedTimeout;
        Ok(())
    }

    fn apply_missing_tenant_guard_rejection(&mut self) -> Result {
        if lambda_tenant_query_without_target_accepted()? || lambda_malformed_path_accepted()? {
            return Err(anyhow::anyhow!(
                "lambda structured query accepted missing tenant guard or malformed path"
            ));
        }
        self.rejected(
            SecurityResult::RejectedMissingTenantGuard,
            true,
            true,
            false,
            true,
        );
        Ok(())
    }

    fn apply_accepted_secure_query(&mut self) -> Result {
        let rows = lambda_tenant_query_rows("tenant-a")?;
        if rows.len() != 1 || rows[0]["tenant_id"] != "tenant-a" {
            return Err(anyhow::anyhow!(
                "lambda tenant query did not return only target tenant"
            ));
        }
        self.accepted = true;
        self.modifies_data = false;
        self.sees_other_tenant = false;
        self.completes_within_budget = true;
        self.tenant_scoped = true;
        self.target_tenant_present = true;
        self.effective_tenant_guard = true;
        self.timer_installed = true;
        self.last_result = SecurityResult::AcceptedSafe;
        Ok(())
    }

    #[allow(clippy::fn_params_excessive_bools)]
    fn rejected(
        &mut self,
        result: SecurityResult,
        tenant_scoped: bool,
        target_tenant_present: bool,
        effective_tenant_guard: bool,
        timer_installed: bool,
    ) {
        self.accepted = false;
        self.modifies_data = false;
        self.sees_other_tenant = false;
        self.completes_within_budget = true;
        self.tenant_scoped = tenant_scoped;
        self.target_tenant_present = target_tenant_present;
        self.effective_tenant_guard = effective_tenant_guard;
        self.timer_installed = timer_installed;
        self.last_result = result;
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
            select: vec![QuerySelect::DocumentPath {
                document_column: "item".to_string(),
                path: "profile..email".to_string(),
                alias: "email".to_string(),
            }],
            filters: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: Some(1),
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
    Ok(response["rows"].as_array().cloned().unwrap_or_default())
}

fn security_handler() -> Result<AnalyticsLambdaHandler> {
    let handler = AnalyticsLambdaHandler::new(
        security_manifest(),
        &StorageBackend::DuckDb {
            path: ":memory:".to_string(),
        },
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
        select: vec![
            QuerySelect::Column {
                column_name: "tenant_id".to_string(),
                alias: None,
            },
            QuerySelect::Column {
                column_name: "email".to_string(),
                alias: None,
            },
        ],
        filters: Vec::new(),
        group_by: Vec::new(),
        order_by: Vec::new(),
        limit: None,
    }
}

fn security_manifest() -> AnalyticsManifest {
    AnalyticsManifest::new(vec![TableRegistration {
        source_table_name: "shared_users".to_string(),
        analytics_table_name: "users".to_string(),
        source_table_name_prefix: None,
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
    }])
}

#[allow(clippy::fn_params_excessive_bools, clippy::too_many_arguments)]
fn query_accepted_by_security_policy(
    surface: QuerySurface,
    attack: QueryAttack,
    tenant_is_scoped: bool,
    target_present: bool,
    structured_tenant_filter_injected: bool,
    raw_sql_sandboxed: bool,
    installed_timer: bool,
    runtime_ms: i64,
) -> bool {
    !attack_attempts_mutation(attack)
        && !attack_uses_malformed_path(attack)
        && runtime_within_budget(runtime_ms, installed_timer)
        && (!tenant_is_scoped
            || (target_present
                && tenant_guard_effective(
                    surface,
                    structured_tenant_filter_injected,
                    raw_sql_sandboxed,
                )
                && !attack_attempts_other_tenant(attack)))
}

#[allow(clippy::fn_params_excessive_bools, clippy::too_many_arguments)]
fn security_result(
    surface: QuerySurface,
    attack: QueryAttack,
    tenant_is_scoped: bool,
    target_present: bool,
    structured_tenant_filter_injected: bool,
    raw_sql_sandboxed: bool,
    installed_timer: bool,
    runtime_ms: i64,
) -> SecurityResult {
    if attack_attempts_mutation(attack) {
        SecurityResult::RejectedMutation
    } else if attack_uses_malformed_path(attack) {
        SecurityResult::RejectedMalformedPath
    } else if !runtime_within_budget(runtime_ms, installed_timer) {
        SecurityResult::RejectedTimeout
    } else if tenant_is_scoped
        && (!target_present
            || !tenant_guard_effective(
                surface,
                structured_tenant_filter_injected,
                raw_sql_sandboxed,
            ))
    {
        SecurityResult::RejectedMissingTenantGuard
    } else if tenant_is_scoped && attack_attempts_other_tenant(attack) {
        SecurityResult::RejectedCrossTenant
    } else {
        SecurityResult::AcceptedSafe
    }
}

fn attack_attempts_mutation(attack: QueryAttack) -> bool {
    matches!(
        attack,
        QueryAttack::MutatingPrefixAttack
            | QueryAttack::MultiStatementInjection
            | QueryAttack::WithMutationAttack
    )
}

fn attack_attempts_other_tenant(attack: QueryAttack) -> bool {
    matches!(
        attack,
        QueryAttack::UnionOtherTenantAttack | QueryAttack::CrossTenantPredicateAttack
    )
}

fn attack_uses_malformed_path(attack: QueryAttack) -> bool {
    matches!(attack, QueryAttack::MalformedDocumentPathAttack)
}

fn runtime_within_budget(runtime_ms: i64, installed_timer: bool) -> bool {
    runtime_ms <= 10_000 || installed_timer
}

fn tenant_guard_effective(
    surface: QuerySurface,
    structured_tenant_filter_injected: bool,
    raw_sql_sandboxed: bool,
) -> bool {
    match surface {
        QuerySurface::StructuredJson => structured_tenant_filter_injected,
        QuerySurface::RawSql => raw_sql_sandboxed,
    }
}

fn current_thread_runtime() -> Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?)
}
