module trim_mbt {
  type TrimCase =
    | DryRunRowKeys
    | ApplyWithoutConfirmation
    | ApplyRowKeysWithConfirmation
    | ApplyDeletesTooManyRows
    | EmptySelectionRejected
    | UnsafePredicateRejected
    | RowPredicateApply
    | FullTableDropApply
    | HorizonTrimApply
    | SnapshotExpiryUnsupported
    | RawBackupExpirationUnsupported

  type TrimResult =
    | NotRun
    | DryRunComplete
    | Applied
    | RejectedMissingConfirmation
    | RejectedApplyNotSupported
    | RejectedDeleteCountExceeded
    | RejectedEmptySelection
    | RejectedUnsafePredicate

  type TrimReport = {
    result: TrimResult,
    candidateRows: int,
    rowsDeleted: int,
    destinationMutated: bool,
  }

  pure def defaultReport: TrimReport = {
    result: NotRun,
    candidateRows: 0,
    rowsDeleted: 0,
    destinationMutated: false,
  }

  pure def reportFor(trimCase: TrimCase): TrimReport =
    match trimCase {
      | DryRunRowKeys => {
          result: DryRunComplete,
          candidateRows: 2,
          rowsDeleted: 0,
          destinationMutated: false,
        }
      | ApplyWithoutConfirmation => {
          result: RejectedMissingConfirmation,
          candidateRows: 0,
          rowsDeleted: 0,
          destinationMutated: false,
        }
      | ApplyRowKeysWithConfirmation => {
          result: Applied,
          candidateRows: 1,
          rowsDeleted: 1,
          destinationMutated: true,
        }
      | ApplyDeletesTooManyRows => {
          result: RejectedDeleteCountExceeded,
          candidateRows: 1,
          rowsDeleted: 2,
          destinationMutated: false,
        }
      | EmptySelectionRejected => {
          result: RejectedEmptySelection,
          candidateRows: 0,
          rowsDeleted: 0,
          destinationMutated: false,
        }
      | UnsafePredicateRejected => {
          result: RejectedUnsafePredicate,
          candidateRows: 0,
          rowsDeleted: 0,
          destinationMutated: false,
        }
      | RowPredicateApply => {
          result: Applied,
          candidateRows: 1,
          rowsDeleted: 1,
          destinationMutated: true,
        }
      | FullTableDropApply => {
          result: Applied,
          candidateRows: 2,
          rowsDeleted: 2,
          destinationMutated: true,
        }
      | HorizonTrimApply => {
          result: Applied,
          candidateRows: 1,
          rowsDeleted: 1,
          destinationMutated: true,
        }
      | SnapshotExpiryUnsupported => {
          result: RejectedApplyNotSupported,
          candidateRows: 0,
          rowsDeleted: 0,
          destinationMutated: false,
        }
      | RawBackupExpirationUnsupported => {
          result: RejectedApplyNotSupported,
          candidateRows: 0,
          rowsDeleted: 0,
          destinationMutated: false,
        }
    }

  var lastReport: TrimReport

  action init = lastReport' = defaultReport

  action TrimScenario = {
    nondet trimCase = Set(
      DryRunRowKeys,
      ApplyWithoutConfirmation,
      ApplyRowKeysWithConfirmation,
      ApplyDeletesTooManyRows,
      EmptySelectionRejected,
      UnsafePredicateRejected,
      RowPredicateApply,
      FullTableDropApply,
      HorizonTrimApply,
      SnapshotExpiryUnsupported,
      RawBackupExpirationUnsupported,
    ).oneOf()
    all { lastReport' = reportFor(trimCase) }
  }

  action step = TrimScenario

  val dryRunDoesNotMutate =
    if (lastReport.result == DryRunComplete) not(lastReport.destinationMutated) else true
  val rejectedCasesDoNotMutate =
    if (
      lastReport.result == RejectedMissingConfirmation
        or lastReport.result == RejectedApplyNotSupported
        or lastReport.result == RejectedDeleteCountExceeded
        or lastReport.result == RejectedEmptySelection
        or lastReport.result == RejectedUnsafePredicate
    ) not(lastReport.destinationMutated) else true
  val deletedRowsBounded = lastReport.rowsDeleted <= lastReport.candidateRows
  val appliedMutates = if (lastReport.result == Applied) lastReport.destinationMutated else true

  val inv = all {
    dryRunDoesNotMutate,
    rejectedCasesDoNotMutate,
    if (lastReport.result == RejectedDeleteCountExceeded) true else deletedRowsBounded,
    appliedMutates,
  }

  run dryRunCase = assert(reportFor(DryRunRowKeys).candidateRows == 2)
  run missingConfirmationCase = assert(reportFor(ApplyWithoutConfirmation).result == RejectedMissingConfirmation)
  run applyCase = assert(reportFor(ApplyRowKeysWithConfirmation).result == Applied)
  run rowPredicateApplyCase = assert(reportFor(RowPredicateApply).result == Applied)
  run fullDropApplyCase = assert(reportFor(FullTableDropApply).rowsDeleted == 2)
  run horizonTrimApplyCase = assert(reportFor(HorizonTrimApply).rowsDeleted == 1)
  run lifecycleUnsupportedCase = assert(reportFor(SnapshotExpiryUnsupported).result == RejectedApplyNotSupported)
}
