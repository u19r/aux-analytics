// Security bug-hunt contract for tenant-scoped and unscoped query boundaries.
// Actors are query callers and an enforcement boundary. The shared reducer
// remains parameterized by caller interface, while each adapter-facing action
// fixes that interface to its physical API, CLI, or Lambda boundary and then
// supplies query surface, attack, tenant scope, guard/sandbox installation,
// and runtime. Acceptance is a policy decision, while modification,
// cross-tenant visibility, and deadline completion are independently
// observable outcomes.
// A cross-tenant attack attempt is input evidence; returned rows are the
// production-observed visibility outcome. Rejections are explicit failure
// states. The model abstracts SQL execution and row contents but keeps the
// forbidden outcomes independent of acceptance so weakened guards produce
// counterexamples.
module query_security_bughunt {
  type CallerInterface = Api | Cli | Lambda
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
    | RejectedRawSqlUnavailable
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
  var lastCaller: CallerInterface
  var lastSurface: QuerySurface
  var lastAttack: QueryAttack
  var structuredTenantFilterInjected: bool
  var rawSqlSandboxed: bool
  var lastRuntimeMs: int
  var lastChecked: bool

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
  // StructuredQuery is a typed read-only SELECT AST. Mutation-shaped SQL is
  // executable only through the local CLI RawSql boundary. Keep the attack
  // enum shared, but delete impossible structured mutation inputs from every
  // physical action domain.
  pure val structuredAttacks: Set[QueryAttack] = Set(
    BenignTenantQuery,
    UnionOtherTenantAttack,
    CrossTenantPredicateAttack,
    MalformedDocumentPathAttack,
    LongRunningQuery
  )
  pure def attackChoices(surface: QuerySurface): Set[QueryAttack] =
    if (surface == StructuredJson) { structuredAttacks } else { attacks }

  pure def attackBelongsToSurface(surface: QuerySurface, attack: QueryAttack): bool =
    attackChoices(surface).contains(attack)
  pure val runtimesMs: Set[int] = Set(1, 9999, 10000, 10001)

  // The adapter-facing action domain is physical: RawSql is the local CLI
  // unscoped boundary, while StructuredJson carries the tenant scope and
  // target fields. API's validated request schema requires a non-empty target,
  // whereas CLI's typed parser and Lambda's structured event each retain their
  // own boundary precedence for a missing target.
  pure def scopedChoices(surface: QuerySurface): Set[bool] =
    if (surface == RawSql) { Set(false) } else { bools }

  pure def targetChoices(
    caller: CallerInterface,
    surface: QuerySurface,
    tenantIsScoped: bool,
  ): Set[bool] =
    if (surface == StructuredJson and tenantIsScoped) {
      if (caller == Api) { Set(true) } else { bools }
    } else {
      Set(false)
    }

  pure def structuredFilterChoices(surface: QuerySurface, tenantIsScoped: bool): Set[bool] =
    if (surface == StructuredJson and tenantIsScoped) { bools } else { Set(false) }

  pure def rawSandboxChoices(surface: QuerySurface): Set[bool] =
    if (surface == RawSql) { bools } else { Set(false) }

  pure def validBoundaryActionInput(
    caller: CallerInterface,
    surface: QuerySurface,
    tenantIsScoped: bool,
    targetPresent: bool,
    structuredFilterInjected: bool,
    rawSqlSandboxed: bool,
  ): bool =
    (surface == RawSql implies (
      not(tenantIsScoped) and not(targetPresent) and not(structuredFilterInjected)
    ))
      and (surface == StructuredJson implies not(rawSqlSandboxed))
      and (caller == Api and surface == StructuredJson and tenantIsScoped implies targetPresent)

  // The old free caller pick made these three generated traces possible even
  // though each Rust adapter owns one physical boundary. This predicate is a
  // compact negative proof for the replacement action domain.
  pure def actionBelongsToAdapter(
    adapter: CallerInterface,
    caller: CallerInterface,
    surface: QuerySurface,
    attack: QueryAttack,
    tenantIsScoped: bool,
    targetPresent: bool,
    structuredFilterInjected: bool,
    rawSqlSandboxed: bool,
  ): bool =
    adapter == caller
      and attackBelongsToSurface(surface, attack)
      and validBoundaryActionInput(
        caller,
        surface,
        tenantIsScoped,
        targetPresent,
        structuredFilterInjected,
        rawSqlSandboxed,
      )

  pure def attackAttemptsMutation(attack: QueryAttack): bool =
    attack == MutatingPrefixAttack
      or attack == MultiStatementInjection
      or attack == WithMutationAttack

  pure def crossTenantAttempted(attack: QueryAttack): bool =
    attack == UnionOtherTenantAttack
      or attack == CrossTenantPredicateAttack

  pure def rawSqlAvailable(caller: CallerInterface, surface: QuerySurface): bool =
    surface != RawSql or caller == Cli

  // The CLI's typed tenant-query parser requires the target before it reads
  // the JSON document. API and Lambda receive the body first, so structural
  // validation can win over an empty target at those boundaries.
  pure def cliTargetParserPrecedesShape(
    caller: CallerInterface,
    surface: QuerySurface,
    tenantIsScoped: bool,
    targetPresent: bool,
  ): bool =
    caller == Cli
      and surface == StructuredJson
      and tenantIsScoped
      and not(targetPresent)

  pure def apiTargetSchemaPrecedesShape(
    caller: CallerInterface,
    surface: QuerySurface,
    tenantIsScoped: bool,
    targetPresent: bool,
  ): bool =
    caller == Api
      and surface == StructuredJson
      and tenantIsScoped
      and not(targetPresent)

  pure def structuredTenantPredicateConflicts(
    surface: QuerySurface,
    attack: QueryAttack,
    tenantIsScoped: bool,
    targetPresent: bool,
    structuredFilterInjected: bool,
  ): bool =
    surface == StructuredJson
      and tenantIsScoped
      and (
        crossTenantAttempted(attack)
          or (not(targetPresent) and structuredFilterInjected)
      )

  pure def structuredConflictPrecedesMissingTarget(
    caller: CallerInterface,
    surface: QuerySurface,
    attack: QueryAttack,
    tenantIsScoped: bool,
    targetPresent: bool,
    structuredFilterInjected: bool,
  ): bool =
    surface == StructuredJson
      and tenantIsScoped
      and (caller == Lambda or targetPresent)
      and structuredTenantPredicateConflicts(
        surface,
        attack,
        tenantIsScoped,
        targetPresent,
        structuredFilterInjected,
      )

  pure def rawSqlExecutionSafe(surface: QuerySurface, rawSqlSandboxed: bool): bool =
    surface != RawSql or rawSqlSandboxed

  pure def rawSqlExecutionError(surface: QuerySurface, attack: QueryAttack): bool =
    // At the RawSql boundary this attack is a contract token for an
    // executable parser-invalid SQL/path shape. A DuckDB-tolerated expression
    // such as json_extract(item, '$.profile..email') is not evidence for this
    // branch; an adapter may classify it only after a real InvalidQuery.
    surface == RawSql and attack == MalformedDocumentPathAttack

  pure def returnedOtherTenantRows(
    attack: QueryAttack,
    tenantIsScoped: bool,
    targetPresent: bool,
    effectiveGuard: bool,
    queryAccepted: bool,
    result: SecurityResult,
  ): bool =
    tenantIsScoped
      and targetPresent
      and crossTenantAttempted(attack)
      and queryAccepted
      and result == AcceptedSafe
      and not(effectiveGuard)

  pure def observedDurableEffect(attack: QueryAttack, result: SecurityResult): bool =
    result == AcceptedSafe and attackAttemptsMutation(attack)

  pure def attackUsesMalformedPath(surface: QuerySurface, attack: QueryAttack): bool =
    surface == StructuredJson and attack == MalformedDocumentPathAttack

  pure def runtimeWithinBudget(runtimeMs: int, installedTimer: bool): bool =
    runtimeMs <= 10000 or installedTimer

  pure def queryCompletesWithinBudget(
    caller: CallerInterface,
    surface: QuerySurface,
    installedTimer: bool,
    runtimeMs: int,
  ): bool =
    (surface == RawSql and not(rawSqlAvailable(caller, surface)))
      or runtimeWithinBudget(runtimeMs, installedTimer)

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
    caller: CallerInterface,
    surface: QuerySurface,
    attack: QueryAttack,
    tenantIsScoped: bool,
    targetPresent: bool,
    structuredTenantFilterInjected: bool,
    rawSqlSandboxed: bool,
    installedTimer: bool,
    runtimeMs: int,
  ): bool =
    rawSqlAvailable(caller, surface)
      and rawSqlExecutionSafe(surface, rawSqlSandboxed)
      and not(attackAttemptsMutation(attack))
      and not(attackUsesMalformedPath(surface, attack))
      and not(rawSqlExecutionError(surface, attack))
      and not(cliTargetParserPrecedesShape(caller, surface, tenantIsScoped, targetPresent))
      and queryCompletesWithinBudget(caller, surface, installedTimer, runtimeMs)
      and (
        not(tenantIsScoped)
          or (
            targetPresent
              and tenantGuardEffective(surface, structuredTenantFilterInjected, rawSqlSandboxed)
              and not(crossTenantAttempted(attack))
          )
      )

  pure def securityResult(
    caller: CallerInterface,
    surface: QuerySurface,
    attack: QueryAttack,
    tenantIsScoped: bool,
    targetPresent: bool,
    structuredTenantFilterInjected: bool,
    rawSqlSandboxed: bool,
    installedTimer: bool,
    runtimeMs: int,
  ): SecurityResult =
    if (apiTargetSchemaPrecedesShape(caller, surface, tenantIsScoped, targetPresent)) {
      RejectedMissingTenantGuard
    } else if (cliTargetParserPrecedesShape(caller, surface, tenantIsScoped, targetPresent)) {
      RejectedMissingTenantGuard
    } else if (attackUsesMalformedPath(surface, attack)) {
      RejectedMalformedPath
    } else if (surface == RawSql and not(rawSqlAvailable(caller, surface))) {
      RejectedRawSqlUnavailable
    } else if (not(rawSqlExecutionSafe(surface, rawSqlSandboxed))) {
      RejectedMissingTenantGuard
    } else if (rawSqlExecutionError(surface, attack)) {
      RejectedMalformedPath
    } else if (attackAttemptsMutation(attack)) {
      RejectedMutation
    } else if (
      structuredConflictPrecedesMissingTarget(
        caller,
        surface,
        attack,
        tenantIsScoped,
        targetPresent,
        structuredTenantFilterInjected,
      )
    ) {
      RejectedCrossTenant
    } else if (
      tenantIsScoped
        and (not(targetPresent) or not(tenantGuardEffective(surface, structuredTenantFilterInjected, rawSqlSandboxed)))
    ) {
      RejectedMissingTenantGuard
    } else if (tenantIsScoped and crossTenantAttempted(attack)) {
      RejectedCrossTenant
    } else if (not(queryCompletesWithinBudget(caller, surface, installedTimer, runtimeMs))) {
      RejectedTimeout
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
    lastCaller' = Api,
    lastSurface' = StructuredJson,
    lastAttack' = BenignTenantQuery,
    structuredTenantFilterInjected' = false,
    rawSqlSandboxed' = false,
    lastRuntimeMs' = 0,
    lastChecked' = false,
  }

  action GenerateApiSecurityQuery = {
    val caller = Api
    nondet surface = querySurfaces.oneOf()
    nondet attack = attackChoices(surface).oneOf()
    nondet tenant_is_scoped = scopedChoices(surface).oneOf()
    nondet target_present = targetChoices(caller, surface, tenant_is_scoped).oneOf()
    nondet structured_tenant_filter_injected = structuredFilterChoices(surface, tenant_is_scoped).oneOf()
    nondet raw_sql_sandboxed = rawSandboxChoices(surface).oneOf()
    nondet installed_timer = bools.oneOf()
    nondet runtime_ms = runtimesMs.oneOf()
    val security_result = securityResult(
      caller,
      surface,
      attack,
      tenant_is_scoped,
      target_present,
      structured_tenant_filter_injected,
      raw_sql_sandboxed,
      installed_timer,
      runtime_ms
    )
    val query_accepted = queryAcceptedBySecurityPolicy(
      caller,
      surface,
      attack,
      tenant_is_scoped,
      target_present,
      structured_tenant_filter_injected,
      raw_sql_sandboxed,
      installed_timer,
      runtime_ms
    )
    val effective_guard = tenantGuardEffective(
      surface,
      structured_tenant_filter_injected,
      raw_sql_sandboxed,
    )
    all {
      accepted' = query_accepted,
      modifiesData' = observedDurableEffect(attack, security_result),
      seesOtherTenant' = returnedOtherTenantRows(
        attack,
        tenant_is_scoped,
        target_present,
        effective_guard,
        query_accepted,
        security_result,
      ),
      completesWithinBudget' = queryCompletesWithinBudget(caller, surface, installed_timer, runtime_ms),
      tenantScoped' = tenant_is_scoped,
      targetTenantPresent' = target_present,
      effectiveTenantGuard' = effective_guard,
      timerInstalled' = installed_timer,
      lastCaller' = caller,
      lastSurface' = surface,
      lastAttack' = attack,
      structuredTenantFilterInjected' = structured_tenant_filter_injected,
      rawSqlSandboxed' = raw_sql_sandboxed,
      lastRuntimeMs' = runtime_ms,
      lastChecked' = true,
      lastResult' = security_result,
    }
  }

  action GenerateCliSecurityQuery = {
    val caller = Cli
    nondet surface = querySurfaces.oneOf()
    nondet attack = attackChoices(surface).oneOf()
    nondet tenant_is_scoped = scopedChoices(surface).oneOf()
    nondet target_present = targetChoices(caller, surface, tenant_is_scoped).oneOf()
    nondet structured_tenant_filter_injected = structuredFilterChoices(surface, tenant_is_scoped).oneOf()
    nondet raw_sql_sandboxed = rawSandboxChoices(surface).oneOf()
    nondet installed_timer = bools.oneOf()
    nondet runtime_ms = runtimesMs.oneOf()
    val cli_security_result = securityResult(
      caller,
      surface,
      attack,
      tenant_is_scoped,
      target_present,
      structured_tenant_filter_injected,
      raw_sql_sandboxed,
      installed_timer,
      runtime_ms
    )
    val query_accepted = queryAcceptedBySecurityPolicy(
      caller,
      surface,
      attack,
      tenant_is_scoped,
      target_present,
      structured_tenant_filter_injected,
      raw_sql_sandboxed,
      installed_timer,
      runtime_ms
    )
    val effective_guard = tenantGuardEffective(
      surface,
      structured_tenant_filter_injected,
      raw_sql_sandboxed,
    )
    all {
      accepted' = query_accepted,
      modifiesData' = observedDurableEffect(attack, cli_security_result),
      seesOtherTenant' = returnedOtherTenantRows(
        attack,
        tenant_is_scoped,
        target_present,
        effective_guard,
        query_accepted,
        cli_security_result,
      ),
      completesWithinBudget' = queryCompletesWithinBudget(caller, surface, installed_timer, runtime_ms),
      tenantScoped' = tenant_is_scoped,
      targetTenantPresent' = target_present,
      effectiveTenantGuard' = effective_guard,
      timerInstalled' = installed_timer,
      lastCaller' = caller,
      lastSurface' = surface,
      lastAttack' = attack,
      structuredTenantFilterInjected' = structured_tenant_filter_injected,
      rawSqlSandboxed' = raw_sql_sandboxed,
      lastRuntimeMs' = runtime_ms,
      lastChecked' = true,
      lastResult' = cli_security_result,
    }
  }

  action GenerateLambdaSecurityQuery = {
    val caller = Lambda
    nondet surface = querySurfaces.oneOf()
    nondet attack = attackChoices(surface).oneOf()
    nondet tenant_is_scoped = scopedChoices(surface).oneOf()
    nondet target_present = targetChoices(caller, surface, tenant_is_scoped).oneOf()
    nondet structured_tenant_filter_injected = structuredFilterChoices(surface, tenant_is_scoped).oneOf()
    nondet raw_sql_sandboxed = rawSandboxChoices(surface).oneOf()
    nondet installed_timer = bools.oneOf()
    nondet runtime_ms = runtimesMs.oneOf()
    val lambda_security_result = securityResult(
      caller,
      surface,
      attack,
      tenant_is_scoped,
      target_present,
      structured_tenant_filter_injected,
      raw_sql_sandboxed,
      installed_timer,
      runtime_ms
    )
    val query_accepted = queryAcceptedBySecurityPolicy(
      caller,
      surface,
      attack,
      tenant_is_scoped,
      target_present,
      structured_tenant_filter_injected,
      raw_sql_sandboxed,
      installed_timer,
      runtime_ms
    )
    val effective_guard = tenantGuardEffective(
      surface,
      structured_tenant_filter_injected,
      raw_sql_sandboxed,
    )
    all {
      accepted' = query_accepted,
      modifiesData' = observedDurableEffect(attack, lambda_security_result),
      seesOtherTenant' = returnedOtherTenantRows(
        attack,
        tenant_is_scoped,
        target_present,
        effective_guard,
        query_accepted,
        lambda_security_result,
      ),
      completesWithinBudget' = queryCompletesWithinBudget(caller, surface, installed_timer, runtime_ms),
      tenantScoped' = tenant_is_scoped,
      targetTenantPresent' = target_present,
      effectiveTenantGuard' = effective_guard,
      timerInstalled' = installed_timer,
      lastCaller' = caller,
      lastSurface' = surface,
      lastAttack' = attack,
      structuredTenantFilterInjected' = structured_tenant_filter_injected,
      rawSqlSandboxed' = raw_sql_sandboxed,
      lastRuntimeMs' = runtime_ms,
      lastChecked' = true,
      lastResult' = lambda_security_result,
    }
  }

  action CheckSqlInjectionRejected = all {
    accepted' = queryAcceptedBySecurityPolicy(Cli, RawSql, MutatingPrefixAttack, false, false, false, true, true, 1),
    modifiesData' = observedDurableEffect(MutatingPrefixAttack, RejectedMutation),
    seesOtherTenant' = returnedOtherTenantRows(
      MutatingPrefixAttack,
      false,
      false,
      tenantGuardEffective(RawSql, false, true),
      queryAcceptedBySecurityPolicy(Cli, RawSql, MutatingPrefixAttack, false, false, false, true, true, 1),
      securityResult(Cli, RawSql, MutatingPrefixAttack, false, false, false, true, true, 1),
    ),
    completesWithinBudget' = queryCompletesWithinBudget(Cli, RawSql, true, 1),
    tenantScoped' = false,
    targetTenantPresent' = false,
    effectiveTenantGuard' = tenantGuardEffective(RawSql, false, true),
    timerInstalled' = true,
    lastCaller' = Cli,
    lastSurface' = RawSql,
    lastAttack' = MutatingPrefixAttack,
    structuredTenantFilterInjected' = false,
    rawSqlSandboxed' = true,
    lastRuntimeMs' = 1,
    lastChecked' = true,
    lastResult' = securityResult(Cli, RawSql, MutatingPrefixAttack, false, false, false, true, true, 1),
  }

  action CheckCrossTenantRejected = all {
    accepted' = queryAcceptedBySecurityPolicy(Api, StructuredJson, UnionOtherTenantAttack, true, true, true, false, true, 1),
    modifiesData' = observedDurableEffect(UnionOtherTenantAttack, RejectedCrossTenant),
    seesOtherTenant' = returnedOtherTenantRows(
      UnionOtherTenantAttack,
      true,
      true,
      tenantGuardEffective(StructuredJson, true, false),
      queryAcceptedBySecurityPolicy(Api, StructuredJson, UnionOtherTenantAttack, true, true, true, false, true, 1),
      securityResult(Api, StructuredJson, UnionOtherTenantAttack, true, true, true, false, true, 1),
    ),
    completesWithinBudget' = queryCompletesWithinBudget(Api, StructuredJson, true, 1),
    tenantScoped' = true,
    targetTenantPresent' = true,
    effectiveTenantGuard' = tenantGuardEffective(StructuredJson, true, false),
    timerInstalled' = true,
    lastCaller' = Api,
    lastSurface' = StructuredJson,
    lastAttack' = UnionOtherTenantAttack,
    structuredTenantFilterInjected' = true,
    rawSqlSandboxed' = false,
    lastRuntimeMs' = 1,
    lastChecked' = true,
    lastResult' = securityResult(Api, StructuredJson, UnionOtherTenantAttack, true, true, true, false, true, 1),
  }

  action CheckTimeoutRejected = all {
    accepted' = queryAcceptedBySecurityPolicy(Lambda, StructuredJson, BenignTenantQuery, true, true, true, false, false, 10001),
    modifiesData' = observedDurableEffect(BenignTenantQuery, RejectedTimeout),
    seesOtherTenant' = returnedOtherTenantRows(
      BenignTenantQuery,
      true,
      true,
      tenantGuardEffective(StructuredJson, true, false),
      queryAcceptedBySecurityPolicy(Lambda, StructuredJson, BenignTenantQuery, true, true, true, false, false, 10001),
      securityResult(Lambda, StructuredJson, BenignTenantQuery, true, true, true, false, false, 10001),
    ),
    completesWithinBudget' = queryCompletesWithinBudget(Lambda, StructuredJson, false, 10001),
    tenantScoped' = true,
    targetTenantPresent' = true,
    effectiveTenantGuard' = tenantGuardEffective(StructuredJson, true, false),
    timerInstalled' = false,
    lastCaller' = Lambda,
    lastSurface' = StructuredJson,
    lastAttack' = BenignTenantQuery,
    structuredTenantFilterInjected' = true,
    rawSqlSandboxed' = false,
    lastRuntimeMs' = 10001,
    lastChecked' = true,
    lastResult' = securityResult(Lambda, StructuredJson, BenignTenantQuery, true, true, true, false, false, 10001),
  }

  action CheckTenantGuardMissing = all {
    accepted' = queryAcceptedBySecurityPolicy(Cli, StructuredJson, BenignTenantQuery, true, true, false, false, true, 1),
    modifiesData' = observedDurableEffect(BenignTenantQuery, RejectedMissingTenantGuard),
    seesOtherTenant' = returnedOtherTenantRows(
      BenignTenantQuery,
      true,
      true,
      tenantGuardEffective(StructuredJson, false, false),
      queryAcceptedBySecurityPolicy(Cli, StructuredJson, BenignTenantQuery, true, true, false, false, true, 1),
      securityResult(Cli, StructuredJson, BenignTenantQuery, true, true, false, false, true, 1),
    ),
    completesWithinBudget' = queryCompletesWithinBudget(Cli, StructuredJson, true, 1),
    tenantScoped' = true,
    targetTenantPresent' = true,
    effectiveTenantGuard' = tenantGuardEffective(StructuredJson, false, false),
    timerInstalled' = true,
    lastCaller' = Cli,
    lastSurface' = StructuredJson,
    lastAttack' = BenignTenantQuery,
    structuredTenantFilterInjected' = false,
    rawSqlSandboxed' = false,
    lastRuntimeMs' = 1,
    lastChecked' = true,
    lastResult' = securityResult(Cli, StructuredJson, BenignTenantQuery, true, true, false, false, true, 1),
  }

  action DirectAcceptedSecureQuery = all {
    accepted' = queryAcceptedBySecurityPolicy(Lambda, StructuredJson, BenignTenantQuery, true, true, true, false, true, 1),
    modifiesData' = observedDurableEffect(BenignTenantQuery, AcceptedSafe),
    seesOtherTenant' = returnedOtherTenantRows(
      BenignTenantQuery,
      true,
      true,
      tenantGuardEffective(StructuredJson, true, false),
      queryAcceptedBySecurityPolicy(Lambda, StructuredJson, BenignTenantQuery, true, true, true, false, true, 1),
      securityResult(Lambda, StructuredJson, BenignTenantQuery, true, true, true, false, true, 1),
    ),
    completesWithinBudget' = queryCompletesWithinBudget(Lambda, StructuredJson, true, 1),
    tenantScoped' = true,
    targetTenantPresent' = true,
    effectiveTenantGuard' = tenantGuardEffective(StructuredJson, true, false),
    timerInstalled' = true,
    lastCaller' = Lambda,
    lastSurface' = StructuredJson,
    lastAttack' = BenignTenantQuery,
    structuredTenantFilterInjected' = true,
    rawSqlSandboxed' = false,
    lastRuntimeMs' = 1,
    lastChecked' = true,
    lastResult' = securityResult(Lambda, StructuredJson, BenignTenantQuery, true, true, true, false, true, 1),
  }

  // Mutation-only unsafe reducer: it records cross-tenant intent as returned
  // rows even though the canonical guard rejects the request. This action is
  // deliberately excluded from step and is enabled only by M-A1-QSEC-002.
  action UnsafeCrossTenantExecution = all {
    accepted' = queryAcceptedBySecurityPolicy(Api, StructuredJson, UnionOtherTenantAttack, true, true, true, false, true, 1),
    modifiesData' = observedDurableEffect(UnionOtherTenantAttack, RejectedCrossTenant),
    seesOtherTenant' = true and crossTenantAttempted(UnionOtherTenantAttack),
    completesWithinBudget' = queryCompletesWithinBudget(Api, StructuredJson, true, 1),
    tenantScoped' = true,
    targetTenantPresent' = true,
    effectiveTenantGuard' = tenantGuardEffective(StructuredJson, true, false),
    timerInstalled' = true,
    lastCaller' = Api,
    lastSurface' = StructuredJson,
    lastAttack' = UnionOtherTenantAttack,
    structuredTenantFilterInjected' = true,
    rawSqlSandboxed' = false,
    lastRuntimeMs' = 1,
    lastChecked' = true,
    lastResult' = securityResult(Api, StructuredJson, UnionOtherTenantAttack, true, true, true, false, true, 1),
  }

  // The broad formal step explores all three physical actions. Each adapter
  // bridge selects its own alias (apiStep, cliStep, or lambdaStep) so a crate
  // cannot receive another boundary's caller or parser shape.
  action apiStep = GenerateApiSecurityQuery
  action cliStep = GenerateCliSecurityQuery
  action lambdaStep = GenerateLambdaSecurityQuery

  action step = any {
    GenerateApiSecurityQuery,
    GenerateCliSecurityQuery,
    GenerateLambdaSecurityQuery,
    CheckSqlInjectionRejected,
    CheckCrossTenantRejected,
    CheckTimeoutRejected,
    CheckTenantGuardMissing,
    DirectAcceptedSecureQuery,
  }

  /* Properties */

  val queriesNeverModifyData =
    not(lastChecked) or not(modifiesData)

  // Rejected attempts must also return no cross-tenant rows; rejection is
  // classified by lastResult, not by treating visibility as an attack flag.
  val tenantScopedQueriesNeverSeeOtherTenants =
    tenantScoped implies not(seesOtherTenant)

  val acceptedQueriesCompleteWithinBudget =
    accepted implies completesWithinBudget

  val tenantScopedQueriesRequireTargetTenant =
    accepted and tenantScoped implies targetTenantPresent

  val tenantScopedQueriesRequireEffectiveGuard =
    accepted and tenantScoped implies effectiveTenantGuard

  val acceptanceMatchesCanonicalSecurityReducer =
    not(lastChecked) or accepted == queryAcceptedBySecurityPolicy(
      lastCaller,
      lastSurface,
      lastAttack,
      tenantScoped,
      targetTenantPresent,
      structuredTenantFilterInjected,
      rawSqlSandboxed,
      timerInstalled,
      lastRuntimeMs,
    )
  val resultMatchesCanonicalSecurityReducer =
    not(lastChecked) or lastResult == securityResult(
      lastCaller,
      lastSurface,
      lastAttack,
      tenantScoped,
      targetTenantPresent,
      structuredTenantFilterInjected,
      rawSqlSandboxed,
      timerInstalled,
      lastRuntimeMs,
    )
  val observedRowsMatchCanonicalVisibility =
    not(lastChecked) or seesOtherTenant == returnedOtherTenantRows(
      lastAttack,
      tenantScoped,
      targetTenantPresent,
      effectiveTenantGuard,
      accepted,
      lastResult,
    )

  val canonicalActionDomainMatchesBoundary =
    not(lastChecked) or validBoundaryActionInput(
      lastCaller,
      lastSurface,
      tenantScoped,
      targetTenantPresent,
      structuredTenantFilterInjected,
      rawSqlSandboxed,
    )

  val canonicalAttackDomainMatchesSurface =
    not(lastChecked) or attackBelongsToSurface(lastSurface, lastAttack)

  val inv = and {
    queriesNeverModifyData,
    tenantScopedQueriesNeverSeeOtherTenants,
    acceptedQueriesCompleteWithinBudget,
    tenantScopedQueriesRequireTargetTenant,
    tenantScopedQueriesRequireEffectiveGuard,
    acceptanceMatchesCanonicalSecurityReducer,
    resultMatchesCanonicalSecurityReducer,
    observedRowsMatchCanonicalVisibility,
    canonicalActionDomainMatchesBoundary,
    canonicalAttackDomainMatchesSurface,
  }

  /* Example Runs */

  run sqlInjectionAttackTest =
    all {
      assert(attackAttemptsMutation(MutatingPrefixAttack)),
      assert(attackAttemptsMutation(MultiStatementInjection)),
      assert(attackAttemptsMutation(WithMutationAttack)),
      assert(not(observedDurableEffect(MutatingPrefixAttack, RejectedMutation))),
      assert(observedDurableEffect(MutatingPrefixAttack, AcceptedSafe)),
      assert(queryAcceptedBySecurityPolicy(Cli, RawSql, MutatingPrefixAttack, false, false, false, true, true, 1) == false),
      assert(securityResult(Cli, RawSql, MultiStatementInjection, false, false, false, true, true, 1) == RejectedMutation),
    }

  run tenantIsolationTest =
    all {
      assert(crossTenantAttempted(UnionOtherTenantAttack)),
      assert(crossTenantAttempted(CrossTenantPredicateAttack)),
      assert(queryAcceptedBySecurityPolicy(Api, StructuredJson, BenignTenantQuery, true, true, true, false, true, 1)),
      assert(queryAcceptedBySecurityPolicy(Api, StructuredJson, UnionOtherTenantAttack, true, true, true, false, true, 1) == false),
      assert(returnedOtherTenantRows(UnionOtherTenantAttack, true, true, true, false, RejectedCrossTenant) == false),
      assert(securityResult(Cli, RawSql, BenignTenantQuery, true, true, false, false, true, 1) == RejectedMissingTenantGuard),
    }

  run queryVisibilityTest =
    all {
      assert(returnedOtherTenantRows(CrossTenantPredicateAttack, true, true, true, false, RejectedCrossTenant) == false),
      assert(returnedOtherTenantRows(CrossTenantPredicateAttack, true, false, false, false, RejectedMissingTenantGuard) == false),
      assert(returnedOtherTenantRows(CrossTenantPredicateAttack, true, true, true, true, AcceptedSafe) == false),
    }

  run queryTimeoutTest =
    all {
      assert(runtimeWithinBudget(10000, false)),
      assert(runtimeWithinBudget(10001, false) == false),
      assert(runtimeWithinBudget(10001, true)),
      assert(queryAcceptedBySecurityPolicy(Lambda, StructuredJson, BenignTenantQuery, true, true, true, false, false, 10001) == false),
      assert(securityResult(Lambda, StructuredJson, BenignTenantQuery, true, true, true, false, false, 10001) == RejectedTimeout),
    }

  run preparationPrecedesTimeoutTest =
    all {
      // Preparation failures are observable before execution starts. A
      // runtime over the deadline cannot relabel a tenant-predicate conflict,
      // malformed shape, mutation, or missing guard as a timeout.
      assert(securityResult(Api, StructuredJson, CrossTenantPredicateAttack, true, true, true, false, false, 10001) == RejectedCrossTenant),
      assert(securityResult(Cli, StructuredJson, CrossTenantPredicateAttack, true, true, true, false, false, 10001) == RejectedCrossTenant),
      assert(securityResult(Lambda, StructuredJson, CrossTenantPredicateAttack, true, true, true, false, false, 10001) == RejectedCrossTenant),
      assert(securityResult(Api, StructuredJson, MalformedDocumentPathAttack, true, true, true, false, false, 10001) == RejectedMalformedPath),
      assert(securityResult(Cli, RawSql, MutatingPrefixAttack, false, false, false, true, false, 10001) == RejectedMutation),
      assert(securityResult(Cli, StructuredJson, BenignTenantQuery, true, true, false, false, false, 10001) == RejectedMissingTenantGuard),
      assert(securityResult(Lambda, StructuredJson, BenignTenantQuery, true, true, true, false, false, 10001) == RejectedTimeout),
    }

  run callerInterfacePrecedenceTest =
    all {
      // CLI parses the required target before JSON. API's min-length target
      // schema rejects the request before the body reaches the engine, while
      // Lambda's plain String payload reaches the structured boundary.
      assert(securityResult(Cli, StructuredJson, MalformedDocumentPathAttack, true, false, false, false, true, 1) == RejectedMissingTenantGuard),
      assert(securityResult(Api, StructuredJson, MalformedDocumentPathAttack, true, false, false, false, true, 1) == RejectedMissingTenantGuard),
      assert(securityResult(Lambda, StructuredJson, MalformedDocumentPathAttack, true, false, false, false, true, 1) == RejectedMalformedPath),
      assert(securityResult(Cli, StructuredJson, MalformedDocumentPathAttack, true, true, false, false, true, 1) == RejectedMalformedPath),
      assert(securityResult(Api, StructuredJson, MalformedDocumentPathAttack, true, true, false, false, true, 1) == RejectedMalformedPath),
      assert(securityResult(Lambda, StructuredJson, MalformedDocumentPathAttack, true, true, false, false, true, 1) == RejectedMalformedPath),
      // A valid body with no target is a missing-target rejection. Lambda's
      // empty target plus an injected tenant predicate is a real engine
      // conflict; API never reaches that engine path because its schema wins.
      assert(securityResult(Api, StructuredJson, BenignTenantQuery, true, false, true, false, true, 1) == RejectedMissingTenantGuard),
      assert(securityResult(Cli, StructuredJson, BenignTenantQuery, true, false, true, false, true, 1) == RejectedMissingTenantGuard),
      assert(securityResult(Lambda, StructuredJson, BenignTenantQuery, true, false, true, false, true, 1) == RejectedCrossTenant),
      assert(securityResult(Lambda, StructuredJson, BenignTenantQuery, true, false, false, false, true, 1) == RejectedMissingTenantGuard),
      assert(securityResult(Api, StructuredJson, CrossTenantPredicateAttack, true, false, true, false, true, 1) == RejectedMissingTenantGuard),
      assert(securityResult(Lambda, StructuredJson, CrossTenantPredicateAttack, true, false, true, false, true, 1) == RejectedCrossTenant),
      assert(securityResult(Cli, StructuredJson, CrossTenantPredicateAttack, true, false, true, false, true, 1) == RejectedMissingTenantGuard),
      assert(securityResult(Api, StructuredJson, CrossTenantPredicateAttack, true, true, true, false, true, 1) == RejectedCrossTenant),
      assert(securityResult(Cli, StructuredJson, CrossTenantPredicateAttack, true, true, true, false, true, 1) == RejectedCrossTenant),
      assert(securityResult(Lambda, StructuredJson, CrossTenantPredicateAttack, true, true, true, false, true, 1) == RejectedCrossTenant),
      // CLI's typed parser wins for a missing target; mutation-shaped
      // StructuredQuery inputs are excluded from every typed action domain.
      // RawSql mutation cases remain covered by structuredAttackDomainTest.
      // RawSql is unavailable at the API and tenant-facing Lambda boundaries;
      // only the local CLI retains this surface. Lambda raw events therefore
      // fail at schema validation before sandboxing or timeout policy applies.
      assert(rawSqlAvailable(Api, RawSql) == false),
      assert(rawSqlAvailable(Cli, RawSql)),
      assert(rawSqlAvailable(Lambda, RawSql) == false),
      assert(securityResult(Api, RawSql, BenignTenantQuery, false, false, false, true, true, 1) == RejectedRawSqlUnavailable),
      assert(securityResult(Lambda, RawSql, BenignTenantQuery, false, false, false, true, true, 1) == RejectedRawSqlUnavailable),
      assert(queryAcceptedBySecurityPolicy(Cli, RawSql, BenignTenantQuery, false, false, false, true, true, 1)),
      assert(queryAcceptedBySecurityPolicy(Lambda, RawSql, BenignTenantQuery, false, false, false, true, true, 1) == false),
      assert(queryAcceptedBySecurityPolicy(Cli, RawSql, BenignTenantQuery, false, false, false, false, true, 1) == false),
      assert(securityResult(Cli, RawSql, MutatingPrefixAttack, false, false, false, true, true, 1) == RejectedMutation),
      assert(queryAcceptedBySecurityPolicy(Cli, RawSql, MalformedDocumentPathAttack, false, false, false, true, true, 1) == false),
      assert(securityResult(Cli, RawSql, MalformedDocumentPathAttack, false, false, false, true, true, 1) == RejectedMalformedPath),
      assert(securityResult(Lambda, RawSql, MalformedDocumentPathAttack, false, false, false, true, true, 1) == RejectedRawSqlUnavailable),
      assert(rawSqlExecutionError(RawSql, MalformedDocumentPathAttack)),
      assert(securityResult(Lambda, RawSql, BenignTenantQuery, false, false, false, false, true, 1) == RejectedRawSqlUnavailable),
      assert(queryCompletesWithinBudget(Api, RawSql, false, 10001)),
      assert(queryCompletesWithinBudget(Cli, RawSql, false, 10001) == false),
      assert(queryCompletesWithinBudget(Lambda, RawSql, true, 10001)),
    }

  run structuredSchemaReachabilityTest =
    all {
      // API's min_length target schema removes every scoped/missing-target
      // StructuredJson action; CLI and Lambda retain those physical inputs.
      assert(targetChoices(Api, StructuredJson, true) == Set(true)),
      assert(targetChoices(Cli, StructuredJson, true) == bools),
      assert(targetChoices(Lambda, StructuredJson, true) == bools),
      assert(not(validBoundaryActionInput(Api, StructuredJson, true, false, true, false))),
      assert(validBoundaryActionInput(Cli, StructuredJson, true, false, true, false)),
      assert(validBoundaryActionInput(Lambda, StructuredJson, true, false, true, false)),
      assert(apiTargetSchemaPrecedesShape(Api, StructuredJson, true, false)),
      assert(not(apiTargetSchemaPrecedesShape(Lambda, StructuredJson, true, false))),
      // Lambda's filter=true/missing-target trace is conflict-before-missing;
      // removing that filter restores the ordinary missing-target result.
      assert(securityResult(Lambda, StructuredJson, BenignTenantQuery, true, false, true, false, true, 1) == RejectedCrossTenant),
      assert(securityResult(Lambda, StructuredJson, BenignTenantQuery, true, false, false, false, true, 1) == RejectedMissingTenantGuard),
      // CLI keeps typed parser precedence, including a cross-tenant input.
      assert(securityResult(Cli, StructuredJson, CrossTenantPredicateAttack, true, false, true, false, true, 1) == RejectedMissingTenantGuard),
      assert(securityResult(Cli, StructuredJson, CrossTenantPredicateAttack, true, true, false, false, true, 1) == RejectedCrossTenant),
    }

  run structuredAttackDomainTest =
    all {
      // The old free attack domain admitted schema-invalid mutation-shaped
      // StructuredQuery inputs. Typed structured SELECTs retain malformed,
      // conflict, missing-target, and secure cases, but no mutation attack.
      assert(not(attackBelongsToSurface(StructuredJson, MutatingPrefixAttack))),
      assert(not(attackBelongsToSurface(StructuredJson, MultiStatementInjection))),
      assert(not(attackBelongsToSurface(StructuredJson, WithMutationAttack))),
      assert(attackBelongsToSurface(StructuredJson, MalformedDocumentPathAttack)),
      assert(attackBelongsToSurface(StructuredJson, CrossTenantPredicateAttack)),
      // RawSql remains the executable mutation boundary for the local CLI;
      // API and tenant-facing Lambda RawSql are rejected by availability
      // before execution.
      assert(attackBelongsToSurface(RawSql, MutatingPrefixAttack)),
      assert(attackBelongsToSurface(RawSql, MultiStatementInjection)),
      assert(attackBelongsToSurface(RawSql, WithMutationAttack)),
      assert(securityResult(Cli, RawSql, MutatingPrefixAttack, false, false, false, true, true, 1) == RejectedMutation),
      assert(securityResult(Lambda, RawSql, MultiStatementInjection, false, false, false, true, true, 1) == RejectedRawSqlUnavailable),
      assert(securityResult(Api, RawSql, MutatingPrefixAttack, false, false, false, true, true, 1) == RejectedRawSqlUnavailable),
    }

  run adapterActionDomainTest =
    all {
      // Exact foreign-caller traces that the old GenerateSecurityQuery action
      // admitted to the wrong crate-local adapter.
      assert(not(actionBelongsToAdapter(
        Api,
        Lambda,
        RawSql,
        MutatingPrefixAttack,
        true,
        true,
        false,
        true,
      ))),
      assert(not(actionBelongsToAdapter(
        Cli,
        Lambda,
        RawSql,
        BenignTenantQuery,
        true,
        false,
        false,
        true,
      ))),
      assert(not(actionBelongsToAdapter(
        Lambda,
        Cli,
        StructuredJson,
        MutatingPrefixAttack,
        true,
        true,
        true,
        false,
      ))),
      // Each replacement action retains its physical input domain without
      // accepting a foreign caller or a scoped raw request. API and
      // tenant-facing Lambda raw inputs are rejected by availability before
      // execution; only the local CLI executes RawSql.
      assert(actionBelongsToAdapter(
        Api,
        Api,
        RawSql,
        BenignTenantQuery,
        false,
        false,
        false,
        true,
      )),
      assert(not(actionBelongsToAdapter(
        Api,
        Api,
        StructuredJson,
        UnionOtherTenantAttack,
        true,
        false,
        true,
        false,
      ))),
      assert(actionBelongsToAdapter(
        Api,
        Api,
        StructuredJson,
        UnionOtherTenantAttack,
        true,
        true,
        true,
        false,
      )),
      assert(actionBelongsToAdapter(
        Cli,
        Cli,
        StructuredJson,
        BenignTenantQuery,
        true,
        true,
        true,
        false,
      )),
      assert(actionBelongsToAdapter(
        Cli,
        Cli,
        RawSql,
        MutatingPrefixAttack,
        false,
        false,
        false,
        true,
      )),
      assert(actionBelongsToAdapter(
        Lambda,
        Lambda,
        RawSql,
        MultiStatementInjection,
        false,
        false,
        false,
        true,
      )),
      assert(not(actionBelongsToAdapter(
        Lambda,
        Lambda,
        StructuredJson,
        MutatingPrefixAttack,
        true,
        false,
        true,
        false,
      ))),
    }

  /* Witnesses */

  val reset_state_witness =
    not(lastChecked)
      and lastResult == NotChecked
      and not(accepted)
      and not(modifiesData)
      and not(seesOtherTenant)
      and lastCaller == Api

  val api_action_domain_witness =
    lastChecked
      and lastCaller == Api
      and canonicalActionDomainMatchesBoundary

  val cli_action_domain_witness =
    lastChecked
      and lastCaller == Cli
      and canonicalActionDomainMatchesBoundary

  val lambda_action_domain_witness =
    lastChecked
      and lastCaller == Lambda
      and canonicalActionDomainMatchesBoundary

  val structured_attack_domain_witness =
    lastChecked
      and lastSurface == StructuredJson
      and not(attackAttemptsMutation(lastAttack))

  val sql_injection_rejected_witness =
    lastResult == RejectedMutation

  val raw_sql_mutation_rejected_witness =
    lastChecked
      and lastCaller == Cli
      and lastSurface == RawSql
      and attackAttemptsMutation(lastAttack)
      and lastResult == RejectedMutation
      and not(modifiesData)

  val rejected_mutation_no_effect_witness =
    lastChecked
      and lastResult == RejectedMutation
      and not(modifiesData)

  val lambda_missing_target_conflict_witness =
    lastChecked
      and lastCaller == Lambda
      and lastSurface == StructuredJson
      and lastAttack == BenignTenantQuery
      and tenantScoped
      and not(targetTenantPresent)
      and structuredTenantFilterInjected
      and lastResult == RejectedCrossTenant
      and not(seesOtherTenant)

  val lambda_missing_target_rejection_witness =
    lastChecked
      and lastCaller == Lambda
      and lastSurface == StructuredJson
      and lastAttack == BenignTenantQuery
      and tenantScoped
      and not(targetTenantPresent)
      and not(structuredTenantFilterInjected)
      and lastResult == RejectedMissingTenantGuard
      and not(modifiesData)

  val cross_tenant_attack_attempt_witness =
    lastChecked and crossTenantAttempted(lastAttack)

  val cross_tenant_rejected_witness =
    lastResult == RejectedCrossTenant

  val safe_cross_tenant_rejection_witness =
    lastResult == RejectedCrossTenant
      and crossTenantAttempted(lastAttack)
      and not(seesOtherTenant)

  val timeout_rejected_witness =
    lastResult == RejectedTimeout

  val tenant_guard_missing_witness =
    lastResult == RejectedMissingTenantGuard

  val accepted_secure_query_witness =
    lastResult == AcceptedSafe
      and accepted
      and tenantScoped
      and not(seesOtherTenant)

  val unsafe_cross_tenant_visibility_witness =
    lastChecked
      and tenantScoped
      and seesOtherTenant
      and crossTenantAttempted(lastAttack)

  val cli_missing_target_malformed_witness =
    lastChecked
      and lastCaller == Cli
      and lastSurface == StructuredJson
      and lastAttack == MalformedDocumentPathAttack
      and not(targetTenantPresent)
      and lastResult == RejectedMissingTenantGuard

  val cli_missing_target_conflict_witness =
    lastChecked
      and lastCaller == Cli
      and lastSurface == StructuredJson
      and lastAttack == CrossTenantPredicateAttack
      and tenantScoped
      and not(targetTenantPresent)
      and lastResult == RejectedMissingTenantGuard

  val api_target_schema_witness =
    lastChecked
      and lastCaller == Api
      and lastSurface == StructuredJson
      and tenantScoped
      and targetTenantPresent
      and canonicalActionDomainMatchesBoundary

  val lambda_malformed_missing_target_witness =
    lastChecked
      and lastCaller == Lambda
      and lastSurface == StructuredJson
      and lastAttack == MalformedDocumentPathAttack
      and not(targetTenantPresent)
      and lastResult == RejectedMalformedPath

  val target_present_malformed_witness =
    lastChecked
      and lastSurface == StructuredJson
      and lastAttack == MalformedDocumentPathAttack
      and targetTenantPresent
      and lastResult == RejectedMalformedPath

  val valid_missing_target_witness =
    lastChecked
      and lastSurface == StructuredJson
      and lastAttack == BenignTenantQuery
      and not(targetTenantPresent)
      and lastResult == RejectedMissingTenantGuard

  val valid_conflict_witness =
    lastChecked
      and lastSurface == StructuredJson
      and crossTenantAttempted(lastAttack)
      and lastResult == RejectedCrossTenant

  val raw_sql_unavailable_witness =
    lastChecked
      and (lastCaller == Api or lastCaller == Lambda)
      and lastSurface == RawSql
      and lastResult == RejectedRawSqlUnavailable

  val raw_sql_sandbox_missing_witness =
    lastChecked
      and lastCaller == Cli
      and lastSurface == RawSql
      and not(rawSqlSandboxed)
      and lastResult == RejectedMissingTenantGuard

  // This witness is only a formal input contract. U must supply an actually
  // invalid raw SQL/path and observe InvalidQuery before mapping the result;
  // the fixed action signature carries the attack token but no SQL text.
  val raw_sql_malformed_path_witness =
    lastChecked
      and lastCaller == Cli
      and lastSurface == RawSql
      and lastAttack == MalformedDocumentPathAttack
      and rawSqlSandboxed
      and rawSqlExecutionError(lastSurface, lastAttack)
      and lastResult == RejectedMalformedPath
}
