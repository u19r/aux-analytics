module rate_limit_mbt {
  // A trace keeps one persisted operation's lifecycle and cumulative execution
  // metrics. Limiter inputs are per-batch values; they are deliberately not
  // derived from the cumulative progress document.
  type RateLimitCase =
    | WithinBudget
    | OverRowsBudget
    | OverSourceRequestsBudget
    | RetryBackoff
    | OutsideMaintenanceWindow
    | LowerBudgetTakesEffect
    | CancelBeforeBatch
    | PauseBeforeBatch
    | ResumeAfterPause
    | AcceptCancellation
    | Observe

  type RateLimitReport = {
    allowed: bool,
    waitMs: int,
    retryDelayMs: int,
    batchMayStart: bool,
    policyRowsLimit: int,
  }

  type RateOperationStatus = Running | Paused | Cancelling | Cancelled
  type RateCancellationState = NotRequested | Requested | Accepted
  type RateOperation = {
    operationIdentity: int,
    actorAuthorized: bool,
    phaseExecuting: bool,
    status: RateOperationStatus,
    cancellationState: RateCancellationState,
    completedBatches: int,
    rowsScanned: int,
    retryCount: int,
    revision: int,
    auditLength: int,
  }

  pure def initialOperation: RateOperation = {
    operationIdentity: 1,
    actorAuthorized: true,
    phaseExecuting: true,
    status: Running,
    cancellationState: NotRequested,
    completedBatches: 0,
    rowsScanned: 0,
    retryCount: 0,
    revision: 2,
    auditLength: 3,
  }

  pure def defaultReport: RateLimitReport = {
    allowed: true,
    waitMs: 0,
    retryDelayMs: 0,
    batchMayStart: true,
    policyRowsLimit: 100,
  }

  pure def waitForRows(rows: int, limit: int): int =
    if (rows <= limit) 0 else rows * 1000 / limit

  pure def runningDecision(
    operation: RateOperation,
    batchRows: int,
    policyRowsLimit: int,
  ): RateLimitReport = {
    allowed: waitForRows(batchRows, policyRowsLimit) == 0,
    waitMs: waitForRows(batchRows, policyRowsLimit),
    retryDelayMs: 0,
    batchMayStart: operation.status == Running and waitForRows(batchRows, policyRowsLimit) == 0,
    policyRowsLimit: policyRowsLimit,
  }

  pure def controlDecision(operation: RateOperation): RateLimitReport = {
    allowed: true,
    waitMs: 0,
    retryDelayMs: 0,
    batchMayStart: false,
    policyRowsLimit: 100,
  }

  pure def observedDecision(operation: RateOperation): RateLimitReport =
    runningDecision(operation, 25, 100)

  pure def reportFor(operation: RateOperation, rateLimitCase: RateLimitCase): RateLimitReport =
    if (rateLimitCase == WithinBudget) runningDecision(operation, 25, 100)
    else if (rateLimitCase == OverRowsBudget) {
      allowed: false,
      waitMs: 2500,
      retryDelayMs: 0,
      batchMayStart: false,
      policyRowsLimit: 100,
    }
    else if (rateLimitCase == OverSourceRequestsBudget) {
      allowed: false,
      waitMs: 2000,
      retryDelayMs: 0,
      batchMayStart: false,
      policyRowsLimit: 100,
    }
    else if (rateLimitCase == RetryBackoff) {
      allowed: operation.status == Running,
      waitMs: 0,
      retryDelayMs: 1000 * (operation.retryCount + 1),
      batchMayStart: operation.status == Running,
      policyRowsLimit: 100,
    }
    else if (rateLimitCase == OutsideMaintenanceWindow) {
      allowed: false,
      waitMs: 1800000,
      retryDelayMs: 0,
      batchMayStart: false,
      policyRowsLimit: 100,
    }
    else if (rateLimitCase == LowerBudgetTakesEffect) runningDecision(operation, 75, 50)
    else if (rateLimitCase == CancelBeforeBatch) controlDecision(operation)
    else if (rateLimitCase == PauseBeforeBatch) controlDecision(operation)
    else if (rateLimitCase == ResumeAfterPause) {
      allowed: operation.status == Paused,
      waitMs: 0,
      retryDelayMs: 0,
      batchMayStart: operation.status == Paused,
      policyRowsLimit: 100,
    }
    else if (rateLimitCase == AcceptCancellation) controlDecision(operation)
    else observedDecision(operation)

  pure def withUsage(operation: RateOperation): RateOperation = {
    operationIdentity: operation.operationIdentity,
    actorAuthorized: operation.actorAuthorized,
    phaseExecuting: operation.phaseExecuting,
    status: operation.status,
    cancellationState: operation.cancellationState,
    completedBatches: operation.completedBatches + 1,
    rowsScanned: operation.rowsScanned + 25,
    retryCount: operation.retryCount,
    revision: operation.revision + 1,
    auditLength: operation.auditLength + 1,
  }

  pure def withRetry(operation: RateOperation): RateOperation = {
    operationIdentity: operation.operationIdentity,
    actorAuthorized: operation.actorAuthorized,
    phaseExecuting: operation.phaseExecuting,
    status: operation.status,
    cancellationState: operation.cancellationState,
    completedBatches: operation.completedBatches,
    rowsScanned: operation.rowsScanned,
    retryCount: operation.retryCount + 1,
    revision: operation.revision + 1,
    auditLength: operation.auditLength + 1,
  }

  pure def withStatus(
    operation: RateOperation,
    status: RateOperationStatus,
    cancellationState: RateCancellationState,
  ): RateOperation = {
    operationIdentity: operation.operationIdentity,
    actorAuthorized: operation.actorAuthorized,
    phaseExecuting: operation.phaseExecuting,
    status: status,
    cancellationState: cancellationState,
    completedBatches: operation.completedBatches,
    rowsScanned: operation.rowsScanned,
    retryCount: operation.retryCount,
    revision: operation.revision + 1,
    auditLength: operation.auditLength + 1,
  }

  pure def withCancelledStatus(operation: RateOperation): RateOperation = {
    operationIdentity: operation.operationIdentity,
    actorAuthorized: operation.actorAuthorized,
    phaseExecuting: false,
    status: Cancelled,
    cancellationState: Accepted,
    completedBatches: operation.completedBatches,
    rowsScanned: operation.rowsScanned,
    retryCount: operation.retryCount,
    revision: operation.revision + 1,
    auditLength: operation.auditLength + 1,
  }

  pure def nextOperation(operation: RateOperation, rateLimitCase: RateLimitCase): RateOperation =
    if (rateLimitCase == WithinBudget and operation.status == Running) {
      withUsage(operation)
    }
    else if (rateLimitCase == RetryBackoff and operation.status == Running) {
      withRetry(operation)
    }
    else if (rateLimitCase == PauseBeforeBatch and operation.status == Running) {
      withStatus(operation, Paused, operation.cancellationState)
    }
    else if (rateLimitCase == ResumeAfterPause and operation.status == Paused) {
      withStatus(operation, Running, operation.cancellationState)
    }
    else if (rateLimitCase == CancelBeforeBatch and
        (operation.status == Running or operation.status == Paused)) {
      withStatus(operation, Cancelling, Requested)
    }
    else if (rateLimitCase == AcceptCancellation and operation.status == Cancelling) {
      withCancelledStatus(operation)
    }
    else operation

  pure def scenarioChoices(operation: RateOperation): Set[RateLimitCase] =
    if (operation.status == Running) Set(
      WithinBudget,
      OverRowsBudget,
      OverSourceRequestsBudget,
      RetryBackoff,
      OutsideMaintenanceWindow,
      LowerBudgetTakesEffect,
      CancelBeforeBatch,
      PauseBeforeBatch,
      Observe,
    )
    else if (operation.status == Paused) Set(CancelBeforeBatch, ResumeAfterPause, Observe)
    else if (operation.status == Cancelling) Set(AcceptCancellation, Observe)
    else Set(Observe)

  var previousOperation: RateOperation
  var operation: RateOperation
  var lastCase: RateLimitCase
  var lastReport: RateLimitReport

  action init = all {
    previousOperation' = initialOperation,
    operation' = initialOperation,
    lastCase' = Observe,
    lastReport' = defaultReport,
  }

  action RateLimitScenario = {
    nondet rateLimitCase = scenarioChoices(operation).oneOf()
    all {
      previousOperation' = operation,
      operation' = nextOperation(operation, rateLimitCase),
      lastCase' = rateLimitCase,
      lastReport' = reportFor(operation, rateLimitCase),
    }
  }

  action step = RateLimitScenario

  val durableIdentityIsStable = operation.operationIdentity == 1
  val actorIsAuthorized = operation.actorAuthorized
  val lifecycleMatchesStatus =
    if (operation.status == Cancelled) not(operation.phaseExecuting) else operation.phaseExecuting
  val usageIsCumulative = operation.completedBatches >= 0 and operation.rowsScanned >= 0
  val retryStateIsCumulative = operation.retryCount >= 0
  val policyLimitIsPositive = lastReport.policyRowsLimit > 0
  val revisionAndAuditAreDurable = operation.revision >= 2 and operation.auditLength >= 3
  val budgetWaitBlocksBatch =
    if (lastReport.waitMs > 0) not(lastReport.batchMayStart) else true
  val reportValuesNonnegative =
    lastReport.waitMs >= 0 and lastReport.retryDelayMs >= 0
  val startRequiresPermission =
    if (lastReport.batchMayStart) lastReport.allowed else true
  val deniedCannotStart =
    if (not(lastReport.allowed)) not(lastReport.batchMayStart) else true
  val retryDelayOnlyOnRetry =
    if (lastCase != RetryBackoff) lastReport.retryDelayMs == 0 else true
  val retryBackoffIsPositive =
    if (lastCase == RetryBackoff) lastReport.retryDelayMs > 0 else true
  val cancellationStopsBatch =
    if (lastCase == CancelBeforeBatch or lastCase == AcceptCancellation)
      not(lastReport.batchMayStart)
    else true
  val pauseStopsBatch =
    if (lastCase == PauseBeforeBatch) not(lastReport.batchMayStart) else true
  val resumeRestoresBatch =
    if (lastCase == ResumeAfterPause and previousOperation.status == Paused)
      operation.status == Running and lastReport.batchMayStart
    else true
  val policyEvidenceMatchesCase =
    if (lastCase == LowerBudgetTakesEffect) lastReport.policyRowsLimit == 50
    else lastReport.policyRowsLimit == 100

  // Every action compares the current persisted progress with the state just
  // before that action. A failed/denied decision or lifecycle control cannot
  // silently reset cumulative execution counters.
  val allowedBatchAction =
    lastCase == WithinBudget and previousOperation.status == Running and lastReport.allowed
  val retryAction =
    lastCase == RetryBackoff and previousOperation.status == Running
  val cumulativeProgressRetained =
    if (allowedBatchAction)
      operation.completedBatches == previousOperation.completedBatches + 1 and
        operation.rowsScanned == previousOperation.rowsScanned + 25 and
        operation.retryCount == previousOperation.retryCount
    else if (retryAction)
      operation.completedBatches == previousOperation.completedBatches and
        operation.rowsScanned == previousOperation.rowsScanned and
        operation.retryCount == previousOperation.retryCount + 1
    else
      operation.completedBatches == previousOperation.completedBatches and
        operation.rowsScanned == previousOperation.rowsScanned and
        operation.retryCount == previousOperation.retryCount

  val lifecycleTransition =
    (lastCase == PauseBeforeBatch and previousOperation.status == Running) or
      (lastCase == ResumeAfterPause and previousOperation.status == Paused) or
      (lastCase == CancelBeforeBatch and
        (previousOperation.status == Running or previousOperation.status == Paused)) or
      (lastCase == AcceptCancellation and previousOperation.status == Cancelling)
  val durableRevisionAndAuditDelta =
    if (allowedBatchAction or retryAction or lifecycleTransition)
      operation.revision == previousOperation.revision + 1 and
        operation.auditLength == previousOperation.auditLength + 1
    else
      operation.revision == previousOperation.revision and
        operation.auditLength == previousOperation.auditLength

  // Lower-budget checks use a separate immutable persisted request. The
  // operation under trace retains identity and progress while the report
  // exposes the alternate request's stored policy.
  val loweredBudgetIsIndependent =
    if (lastCase == LowerBudgetTakesEffect)
      lastReport.policyRowsLimit == 50 and
        operation.completedBatches == previousOperation.completedBatches and
        operation.rowsScanned == previousOperation.rowsScanned and
        operation.retryCount == previousOperation.retryCount and
        operation.revision == previousOperation.revision and
        operation.auditLength == previousOperation.auditLength
    else true

  val within_budget_witness =
    lastCase == WithinBudget and lastReport.allowed and
      operation.rowsScanned > previousOperation.rowsScanned
  val rows_budget_witness =
    lastCase == OverRowsBudget and not(lastReport.allowed) and lastReport.waitMs == 2500
  val source_requests_budget_witness =
    lastCase == OverSourceRequestsBudget and not(lastReport.allowed) and
      lastReport.waitMs == 2000
  val retry_witness =
    lastCase == RetryBackoff and lastReport.retryDelayMs > 0 and
      operation.retryCount > previousOperation.retryCount
  val retry_progression_witness =
    retry_witness and lastReport.retryDelayMs == 1000 * operation.retryCount
  val maintenance_window_witness =
    lastCase == OutsideMaintenanceWindow and lastReport.waitMs == 1800000 and
      not(lastReport.batchMayStart)
  val pause_witness =
    lastCase == PauseBeforeBatch and operation.status == Paused and
      not(lastReport.batchMayStart)
  val resume_witness =
    lastCase == ResumeAfterPause and previousOperation.status == Paused and
      operation.status == Running and lastReport.batchMayStart
  val cancellation_witness =
    lastCase == CancelBeforeBatch and operation.status == Cancelling and
      not(lastReport.batchMayStart)
  val cancellation_acceptance_witness =
    lastCase == AcceptCancellation and operation.status == Cancelled and
      operation.cancellationState == Accepted and not(lastReport.batchMayStart)
  val cumulative_usage_witness =
    lastCase == WithinBudget and
      operation.completedBatches > previousOperation.completedBatches and
      operation.rowsScanned > previousOperation.rowsScanned
  val lowered_limit_witness =
    lastCase == LowerBudgetTakesEffect and loweredBudgetIsIndependent and
      lastReport.waitMs == 1500

  val inv = all {
    durableIdentityIsStable,
    actorIsAuthorized,
    lifecycleMatchesStatus,
    usageIsCumulative,
    retryStateIsCumulative,
    policyLimitIsPositive,
    revisionAndAuditAreDurable,
    budgetWaitBlocksBatch,
    reportValuesNonnegative,
    startRequiresPermission,
    deniedCannotStart,
    retryDelayOnlyOnRetry,
    retryBackoffIsPositive,
    cancellationStopsBatch,
    pauseStopsBatch,
    resumeRestoresBatch,
    policyEvidenceMatchesCase,
    cumulativeProgressRetained,
    durableRevisionAndAuditDelta,
    loweredBudgetIsIndependent,
  }

  run withinBudget = assert(reportFor(initialOperation, WithinBudget).allowed)
  run overRowsBudget = assert(reportFor(initialOperation, OverRowsBudget).waitMs == 2500)
  run overSourceRequestsBudget = assert(
    reportFor(initialOperation, OverSourceRequestsBudget).waitMs == 2000 and
      not(reportFor(initialOperation, OverSourceRequestsBudget).batchMayStart)
  )
  run retryBackoff = assert(reportFor(initialOperation, RetryBackoff).retryDelayMs == 1000)
  run maintenanceWindowBlocks = assert(
    not(reportFor(initialOperation, OutsideMaintenanceWindow).batchMayStart)
  )
  run lowerBudgetUsesIndependentPolicy = assert(
    reportFor(initialOperation, LowerBudgetTakesEffect).policyRowsLimit == 50 and
      reportFor(initialOperation, LowerBudgetTakesEffect).waitMs == 1500 and
      nextOperation(initialOperation, LowerBudgetTakesEffect).rowsScanned == initialOperation.rowsScanned
  )
  run cancelBlocks = assert(not(reportFor(initialOperation, CancelBeforeBatch).batchMayStart))
  run pauseBlocks = assert(
    reportFor(initialOperation, PauseBeforeBatch).allowed and
      not(reportFor(initialOperation, PauseBeforeBatch).batchMayStart)
  )
}
