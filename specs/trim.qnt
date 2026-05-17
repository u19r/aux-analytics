module trim {
  type TrimPhase =
    | Planned
    | CountingCandidates
    | DryRunComplete
    | ApplyingBoundedDelete
    | Verifying
    | Completed
    | Rejected

  type TrimAction =
    | DryRunRowKeys
    | ApplyWithoutConfirmation
    | ApplyWithConfirmation
    | UnsafePredicate
    | RowPredicateApply
    | FullTableDropApply
    | HorizonTrimApply
    | SnapshotExpiryApply
    | RawBackupExpirationApply

  type TrimState = {
    phase: TrimPhase,
    dryRun: bool,
    confirmationProvided: bool,
    candidateRows: int,
    rowsDeleted: int,
    destinationMutated: bool,
    unsafePredicateRejected: bool,
    applySupported: bool,
  }

  pure def runAction(trimAction: TrimAction): TrimState =
    match trimAction {
      | DryRunRowKeys => {
          phase: DryRunComplete,
          dryRun: true,
          confirmationProvided: false,
          candidateRows: 2,
          rowsDeleted: 0,
          destinationMutated: false,
          unsafePredicateRejected: false,
          applySupported: false,
        }
      | ApplyWithoutConfirmation => {
          phase: Rejected,
          dryRun: false,
          confirmationProvided: false,
          candidateRows: 0,
          rowsDeleted: 0,
          destinationMutated: false,
          unsafePredicateRejected: false,
          applySupported: false,
        }
      | ApplyWithConfirmation => {
          phase: Completed,
          dryRun: false,
          confirmationProvided: true,
          candidateRows: 1,
          rowsDeleted: 1,
          destinationMutated: true,
          unsafePredicateRejected: false,
          applySupported: true,
        }
      | UnsafePredicate => {
          phase: Rejected,
          dryRun: true,
          confirmationProvided: false,
          candidateRows: 0,
          rowsDeleted: 0,
          destinationMutated: false,
          unsafePredicateRejected: true,
          applySupported: false,
        }
      | RowPredicateApply => {
          phase: Completed,
          dryRun: false,
          confirmationProvided: true,
          candidateRows: 1,
          rowsDeleted: 1,
          destinationMutated: true,
          unsafePredicateRejected: false,
          applySupported: true,
        }
      | FullTableDropApply => {
          phase: Completed,
          dryRun: false,
          confirmationProvided: true,
          candidateRows: 2,
          rowsDeleted: 2,
          destinationMutated: true,
          unsafePredicateRejected: false,
          applySupported: true,
        }
      | HorizonTrimApply => {
          phase: Completed,
          dryRun: false,
          confirmationProvided: true,
          candidateRows: 1,
          rowsDeleted: 1,
          destinationMutated: true,
          unsafePredicateRejected: false,
          applySupported: true,
        }
      | SnapshotExpiryApply => {
          phase: Rejected,
          dryRun: false,
          confirmationProvided: true,
          candidateRows: 0,
          rowsDeleted: 0,
          destinationMutated: false,
          unsafePredicateRejected: false,
          applySupported: false,
        }
      | RawBackupExpirationApply => {
          phase: Rejected,
          dryRun: false,
          confirmationProvided: true,
          candidateRows: 0,
          rowsDeleted: 0,
          destinationMutated: false,
          unsafePredicateRejected: false,
          applySupported: false,
        }
    }

  var state: TrimState

  action init = state' = runAction(DryRunRowKeys)

  action TrimScenario = {
    nondet trimAction = Set(
      DryRunRowKeys,
      ApplyWithoutConfirmation,
      ApplyWithConfirmation,
      UnsafePredicate,
      RowPredicateApply,
      FullTableDropApply,
      HorizonTrimApply,
      SnapshotExpiryApply,
      RawBackupExpirationApply,
    ).oneOf()
    all { state' = runAction(trimAction) }
  }

  action step = TrimScenario

  val dryRunDoesNotMutate = if (state.dryRun) not(state.destinationMutated) else true
  val deletionRequiresConfirmation = if (state.rowsDeleted > 0) state.confirmationProvided else true
  val deletionRequiresSupportedApply = if (state.rowsDeleted > 0) state.applySupported else true
  val deletedRowsBounded = state.rowsDeleted <= state.candidateRows
  val rejectedCasesDoNotMutate = if (state.phase == Rejected) not(state.destinationMutated) else true
  val unsafePredicateNeverCounts = if (state.unsafePredicateRejected) state.candidateRows == 0 else true

  val inv = all {
    dryRunDoesNotMutate,
    deletionRequiresConfirmation,
    deletionRequiresSupportedApply,
    deletedRowsBounded,
    rejectedCasesDoNotMutate,
    unsafePredicateNeverCounts,
  }

  run dryRunWitness = assert(not(runAction(DryRunRowKeys).destinationMutated))
  run missingConfirmationWitness = assert(runAction(ApplyWithoutConfirmation).phase == Rejected)
  run boundedApplyWitness = assert(runAction(ApplyWithConfirmation).rowsDeleted <= runAction(ApplyWithConfirmation).candidateRows)
  run rowPredicateApplyWitness = assert(runAction(RowPredicateApply).rowsDeleted == 1)
  run fullDropWitness = assert(runAction(FullTableDropApply).rowsDeleted == runAction(FullTableDropApply).candidateRows)
  run horizonTrimWitness = assert(runAction(HorizonTrimApply).rowsDeleted == 1)
  run lifecycleUnsupportedWitness = assert(runAction(SnapshotExpiryApply).phase == Rejected and runAction(RawBackupExpirationApply).phase == Rejected)
}
