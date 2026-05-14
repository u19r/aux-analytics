module query_security_bughunt {
  type QuerySurface = RawSql | StructuredJson
  type QueryAttack =
    | BenignTenantQuery
    | MutatingPrefixAttack
    | MultiStatementInjection
    | WithMutationAttack
    | UnionOtherTenantAttack
    | CrossTenantPredicateAttack
    | MalformedDocumentPathAttack
    | LongRunningQuery

  type SecurityResult =
    | NotChecked
    | AcceptedSafe
    | RejectedMutation
    | RejectedCrossTenant
    | RejectedMissingTenantGuard
    | RejectedMalformedPath
    | RejectedTimeout

  var lastResult: SecurityResult
  var accepted: bool
  var modifiesData: bool
  var seesOtherTenant: bool
  var completesWithinBudget: bool
  var tenantScoped: bool
  var targetTenantPresent: bool
  var effectiveTenantGuard: bool
  var timerInstalled: bool

  pure val bools: Set[bool] = Set(true, false)
  pure val querySurfaces: Set[QuerySurface] = Set(RawSql, StructuredJson)
  pure val attacks: Set[QueryAttack] = Set(
    BenignTenantQuery,
    MutatingPrefixAttack,
    MultiStatementInjection,
    WithMutationAttack,
    UnionOtherTenantAttack,
    CrossTenantPredicateAttack,
    MalformedDocumentPathAttack,
    LongRunningQuery
  )
  pure val runtimesMs: Set[int] = Set(1, 9999, 10000, 10001)

  pure def attackAttemptsMutation(attack: QueryAttack): bool =
    attack == MutatingPrefixAttack
      or attack == MultiStatementInjection
      or attack == WithMutationAttack

  pure def attackAttemptsOtherTenant(attack: QueryAttack): bool =
    attack == UnionOtherTenantAttack
      or attack == CrossTenantPredicateAttack

  pure def attackUsesMalformedPath(attack: QueryAttack): bool =
    attack == MalformedDocumentPathAttack

  pure def runtimeWithinBudget(runtimeMs: int, installedTimer: bool): bool =
    runtimeMs <= 10000 or installedTimer

  pure def tenantGuardEffective(
    surface: QuerySurface,
    structuredTenantFilterInjected: bool,
    rawSqlSandboxed: bool,
  ): bool =
    if (surface == StructuredJson) {
      structuredTenantFilterInjected
    } else {
      rawSqlSandboxed
    }

  pure def queryAcceptedBySecurityPolicy(
    surface: QuerySurface,
    attack: QueryAttack,
    tenantIsScoped: bool,
    targetPresent: bool,
    structuredTenantFilterInjected: bool,
    rawSqlSandboxed: bool,
    installedTimer: bool,
    runtimeMs: int,
  ): bool =
    not(attackAttemptsMutation(attack))
      and not(attackUsesMalformedPath(attack))
      and runtimeWithinBudget(runtimeMs, installedTimer)
      and (
        not(tenantIsScoped)
          or (
            targetPresent
              and tenantGuardEffective(surface, structuredTenantFilterInjected, rawSqlSandboxed)
              and not(attackAttemptsOtherTenant(attack))
          )
      )

  pure def securityResult(
    surface: QuerySurface,
    attack: QueryAttack,
    tenantIsScoped: bool,
    targetPresent: bool,
    structuredTenantFilterInjected: bool,
    rawSqlSandboxed: bool,
    installedTimer: bool,
    runtimeMs: int,
  ): SecurityResult =
    if (attackAttemptsMutation(attack)) {
      RejectedMutation
    } else if (attackUsesMalformedPath(attack)) {
      RejectedMalformedPath
    } else if (not(runtimeWithinBudget(runtimeMs, installedTimer))) {
      RejectedTimeout
    } else if (
      tenantIsScoped
        and (not(targetPresent) or not(tenantGuardEffective(surface, structuredTenantFilterInjected, rawSqlSandboxed)))
    ) {
      RejectedMissingTenantGuard
    } else if (tenantIsScoped and attackAttemptsOtherTenant(attack)) {
      RejectedCrossTenant
    } else {
      AcceptedSafe
    }

  action init = all {
    lastResult' = NotChecked,
    accepted' = false,
    modifiesData' = false,
    seesOtherTenant' = false,
    completesWithinBudget' = true,
    tenantScoped' = true,
    targetTenantPresent' = false,
    effectiveTenantGuard' = false,
    timerInstalled' = false,
  }

  action GenerateSecurityQuery = {
    nondet surface = querySurfaces.oneOf()
    nondet attack = attacks.oneOf()
    nondet tenant_is_scoped = bools.oneOf()
    nondet target_present = bools.oneOf()
    nondet structured_tenant_filter_injected = bools.oneOf()
    nondet raw_sql_sandboxed = bools.oneOf()
    nondet installed_timer = bools.oneOf()
    nondet runtime_ms = runtimesMs.oneOf()
    all {
      accepted' = queryAcceptedBySecurityPolicy(
        surface,
        attack,
        tenant_is_scoped,
        target_present,
        structured_tenant_filter_injected,
        raw_sql_sandboxed,
        installed_timer,
        runtime_ms
      ),
      modifiesData' = queryAcceptedBySecurityPolicy(
        surface,
        attack,
        tenant_is_scoped,
        target_present,
        structured_tenant_filter_injected,
        raw_sql_sandboxed,
        installed_timer,
        runtime_ms
      ) and attackAttemptsMutation(attack),
      seesOtherTenant' = queryAcceptedBySecurityPolicy(
        surface,
        attack,
        tenant_is_scoped,
        target_present,
        structured_tenant_filter_injected,
        raw_sql_sandboxed,
        installed_timer,
        runtime_ms
      ) and tenant_is_scoped and attackAttemptsOtherTenant(attack),
      completesWithinBudget' = runtimeWithinBudget(runtime_ms, installed_timer),
      tenantScoped' = tenant_is_scoped,
      targetTenantPresent' = target_present,
      effectiveTenantGuard' = tenantGuardEffective(surface, structured_tenant_filter_injected, raw_sql_sandboxed),
      timerInstalled' = installed_timer,
      lastResult' = securityResult(
        surface,
        attack,
        tenant_is_scoped,
        target_present,
        structured_tenant_filter_injected,
        raw_sql_sandboxed,
        installed_timer,
        runtime_ms
      ),
    }
  }

  action CheckSqlInjectionRejected = all {
    accepted' = false,
    modifiesData' = false,
    seesOtherTenant' = false,
    completesWithinBudget' = true,
    tenantScoped' = true,
    targetTenantPresent' = true,
    effectiveTenantGuard' = true,
    timerInstalled' = true,
    lastResult' = RejectedMutation,
  }

  action CheckCrossTenantRejected = all {
    accepted' = false,
    modifiesData' = false,
    seesOtherTenant' = false,
    completesWithinBudget' = true,
    tenantScoped' = true,
    targetTenantPresent' = true,
    effectiveTenantGuard' = true,
    timerInstalled' = true,
    lastResult' = RejectedCrossTenant,
  }

  action CheckTimeoutRejected = all {
    accepted' = false,
    modifiesData' = false,
    seesOtherTenant' = false,
    completesWithinBudget' = false,
    tenantScoped' = true,
    targetTenantPresent' = true,
    effectiveTenantGuard' = true,
    timerInstalled' = false,
    lastResult' = RejectedTimeout,
  }

  action CheckTenantGuardMissing = all {
    accepted' = false,
    modifiesData' = false,
    seesOtherTenant' = false,
    completesWithinBudget' = true,
    tenantScoped' = true,
    targetTenantPresent' = true,
    effectiveTenantGuard' = false,
    timerInstalled' = true,
    lastResult' = RejectedMissingTenantGuard,
  }

  action DirectAcceptedSecureQuery = all {
    accepted' = true,
    modifiesData' = false,
    seesOtherTenant' = false,
    completesWithinBudget' = true,
    tenantScoped' = true,
    targetTenantPresent' = true,
    effectiveTenantGuard' = true,
    timerInstalled' = true,
    lastResult' = AcceptedSafe,
  }

  action step = any {
    GenerateSecurityQuery,
    CheckSqlInjectionRejected,
    CheckCrossTenantRejected,
    CheckTimeoutRejected,
    CheckTenantGuardMissing,
    DirectAcceptedSecureQuery,
  }

  /* Properties */

  val acceptedQueriesNeverModifyData =
    accepted implies not(modifiesData)

  val acceptedQueriesNeverSeeOtherTenants =
    accepted implies not(seesOtherTenant)

  val acceptedQueriesCompleteWithinBudget =
    accepted implies completesWithinBudget

  val tenantScopedQueriesRequireTargetTenant =
    accepted and tenantScoped implies targetTenantPresent

  val tenantScopedQueriesRequireEffectiveGuard =
    accepted and tenantScoped implies effectiveTenantGuard

  val inv = and {
    acceptedQueriesNeverModifyData,
    acceptedQueriesNeverSeeOtherTenants,
    acceptedQueriesCompleteWithinBudget,
    tenantScopedQueriesRequireTargetTenant,
    tenantScopedQueriesRequireEffectiveGuard,
  }

  /* Example Runs */

  run sqlInjectionAttackTest =
    all {
      assert(attackAttemptsMutation(MutatingPrefixAttack)),
      assert(attackAttemptsMutation(MultiStatementInjection)),
      assert(attackAttemptsMutation(WithMutationAttack)),
      assert(queryAcceptedBySecurityPolicy(RawSql, MutatingPrefixAttack, true, true, true, true, true, 1) == false),
      assert(securityResult(RawSql, MultiStatementInjection, true, true, true, true, true, 1) == RejectedMutation),
    }

  run tenantIsolationTest =
    all {
      assert(attackAttemptsOtherTenant(UnionOtherTenantAttack)),
      assert(attackAttemptsOtherTenant(CrossTenantPredicateAttack)),
      assert(queryAcceptedBySecurityPolicy(StructuredJson, BenignTenantQuery, true, true, true, false, true, 1)),
      assert(queryAcceptedBySecurityPolicy(StructuredJson, UnionOtherTenantAttack, true, true, true, false, true, 1) == false),
      assert(securityResult(RawSql, BenignTenantQuery, true, true, false, false, true, 1) == RejectedMissingTenantGuard),
    }

  run queryTimeoutTest =
    all {
      assert(runtimeWithinBudget(10000, false)),
      assert(runtimeWithinBudget(10001, false) == false),
      assert(runtimeWithinBudget(10001, true)),
      assert(queryAcceptedBySecurityPolicy(StructuredJson, BenignTenantQuery, true, true, true, false, false, 10001) == false),
      assert(securityResult(StructuredJson, BenignTenantQuery, true, true, true, false, false, 10001) == RejectedTimeout),
    }

  /* Witnesses */

  val sql_injection_rejected_witness =
    lastResult == RejectedMutation

  val cross_tenant_rejected_witness =
    lastResult == RejectedCrossTenant

  val timeout_rejected_witness =
    lastResult == RejectedTimeout

  val tenant_guard_missing_witness =
    lastResult == RejectedMissingTenantGuard

  val accepted_secure_query_witness =
    lastResult == AcceptedSafe
}
