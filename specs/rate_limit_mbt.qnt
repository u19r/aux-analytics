module rate_limit_mbt {
  type RateLimitCase =
    | WithinBudget
    | OverRowsBudget
    | RetryBackoff
    | OutsideMaintenanceWindow
    | LowerBudgetTakesEffect
    | CancelBeforeBatch
    | PauseBeforeBatch

  type RateLimitReport = {
    allowed: bool,
    waitMs: int,
    retryDelayMs: int,
    batchMayStart: bool,
  }

  pure def defaultReport: RateLimitReport = {
    allowed: true,
    waitMs: 0,
    retryDelayMs: 0,
    batchMayStart: true,
  }

  pure def reportFor(rateLimitCase: RateLimitCase): RateLimitReport =
    match rateLimitCase {
      | WithinBudget => defaultReport
      | OverRowsBudget => {
          allowed: false,
          waitMs: 2500,
          retryDelayMs: 0,
          batchMayStart: false,
        }
      | RetryBackoff => {
          allowed: true,
          waitMs: 0,
          retryDelayMs: 750,
          batchMayStart: true,
        }
      | OutsideMaintenanceWindow => {
          allowed: false,
          waitMs: 1800000,
          retryDelayMs: 0,
          batchMayStart: false,
        }
      | LowerBudgetTakesEffect => {
          allowed: false,
          waitMs: 10000,
          retryDelayMs: 0,
          batchMayStart: false,
        }
      | CancelBeforeBatch => {
          allowed: true,
          waitMs: 0,
          retryDelayMs: 0,
          batchMayStart: false,
        }
      | PauseBeforeBatch => {
          allowed: true,
          waitMs: 0,
          retryDelayMs: 0,
          batchMayStart: false,
        }
    }

  var lastCase: RateLimitCase
  var lastReport: RateLimitReport

  action init = all {
    lastCase' = WithinBudget,
    lastReport' = defaultReport,
  }

  action RateLimitScenario = {
    nondet rateLimitCase = Set(
      WithinBudget,
      OverRowsBudget,
      RetryBackoff,
      OutsideMaintenanceWindow,
      LowerBudgetTakesEffect,
      CancelBeforeBatch,
      PauseBeforeBatch,
    ).oneOf()
    all {
      lastCase' = rateLimitCase,
      lastReport' = reportFor(rateLimitCase),
    }
  }

  action step = RateLimitScenario

  val budgetWaitBlocksBatch =
    if (lastReport.waitMs > 0) not(lastReport.batchMayStart) else true
  val retryBackoffIsPositive =
    if (lastCase == RetryBackoff) lastReport.retryDelayMs > 0 else true
  val cancellationStopsBatch =
    if (lastCase == CancelBeforeBatch) not(lastReport.batchMayStart) else true
  val pauseStopsBatch =
    if (lastCase == PauseBeforeBatch) not(lastReport.batchMayStart) else true
  val loweredBudgetApplies =
    if (lastCase == LowerBudgetTakesEffect) lastReport.waitMs == 10000 else true

  val inv = all {
    budgetWaitBlocksBatch,
    retryBackoffIsPositive,
    cancellationStopsBatch,
    pauseStopsBatch,
    loweredBudgetApplies,
  }

  run withinBudget = assert(reportFor(WithinBudget).allowed)
  run overRowsBudget = assert(reportFor(OverRowsBudget).waitMs == 2500)
  run retryBackoff = assert(reportFor(RetryBackoff).retryDelayMs == 750)
  run maintenanceWindowBlocks = assert(not(reportFor(OutsideMaintenanceWindow).batchMayStart))
  run lowerBudgetBlocks = assert(reportFor(LowerBudgetTakesEffect).waitMs == 10000)
  run cancelBlocks = assert(not(reportFor(CancelBeforeBatch).batchMayStart))
}
