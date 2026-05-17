module table_fix_mbt {
  type FixCase =
    | DryRunRawBackup
    | ApplyCurrentSource
    | EmptySelectionRejected
    | StaleTruthRejected

  type FixResult =
    | NotRun
    | DryRunComplete
    | Applied
    | RejectedEmptySelection
    | RejectedStaleTruth

  type FixReport = {
    result: FixResult,
    selectedRows: int,
    repairedRows: int,
    destinationMutated: bool,
  }

  pure def defaultReport: FixReport = {
    result: NotRun,
    selectedRows: 0,
    repairedRows: 0,
    destinationMutated: false,
  }

  pure def reportFor(fixCase: FixCase): FixReport =
    match fixCase {
      | DryRunRawBackup => {
          result: DryRunComplete,
          selectedRows: 1,
          repairedRows: 1,
          destinationMutated: false,
        }
      | ApplyCurrentSource => {
          result: Applied,
          selectedRows: 1,
          repairedRows: 1,
          destinationMutated: true,
        }
      | EmptySelectionRejected => {
          result: RejectedEmptySelection,
          selectedRows: 0,
          repairedRows: 0,
          destinationMutated: false,
        }
      | StaleTruthRejected => {
          result: RejectedStaleTruth,
          selectedRows: 1,
          repairedRows: 0,
          destinationMutated: false,
        }
    }

  var lastReport: FixReport

  action init = lastReport' = defaultReport

  action FixScenario = {
    nondet fixCase = Set(DryRunRawBackup, ApplyCurrentSource, EmptySelectionRejected, StaleTruthRejected).oneOf()
    all {
      lastReport' = reportFor(fixCase),
    }
  }

  action step = FixScenario

  val dryRunDoesNotMutate = if (lastReport.result == DryRunComplete) not(lastReport.destinationMutated) else true
  val repairedRowsBounded = lastReport.repairedRows <= lastReport.selectedRows
  val rejectedCasesDoNotMutate =
    if (lastReport.result == RejectedEmptySelection or lastReport.result == RejectedStaleTruth)
      not(lastReport.destinationMutated)
    else true

  val inv = all {
    dryRunDoesNotMutate,
    repairedRowsBounded,
    rejectedCasesDoNotMutate,
  }

  run dryRunCase = assert(reportFor(DryRunRawBackup).destinationMutated == false)
  run applyCase = assert(reportFor(ApplyCurrentSource).destinationMutated)
  run staleRejectCase = assert(reportFor(StaleTruthRejected).result == RejectedStaleTruth)
}
