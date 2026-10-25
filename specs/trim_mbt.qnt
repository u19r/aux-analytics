module trim_mbt {
  type TrimCase =
    | DryRunRowKeys
    | ApplyWithoutConfirmation
    | ApplyRowKeysWithConfirmation
    | ApplyDeletesTooManyRows
    | ApplyStaleCandidates
    | EmptySelectionRejected
    | UnsafePredicateRejected
    | RowPredicateApply
    | FullTableDropApply
    | HorizonTrimApply
    | SnapshotExpiryUnsupported
    | RawBackupExpirationUnsupported
    | SchemaUnsupported

  type TrimReport = {
    candidateRows: int,
    rowsDeleted: int,
    destinationMutated: bool,
  }

  // Rejections carry no report payload. Counts only enter the model through
  // a persisted production report observed by the driver.
  type TrimOutcome =
    | NotRun
    | DryRun(TrimReport)
    | Applied(TrimReport)
    | RejectedMissingConfirmation
    | RejectedUnsupportedTarget
    | RejectedDeleteCountExceeded
    | RejectedStaleCandidate
    | RejectedSchemaUnsupported
    | RejectedEmptySelection
    | RejectedUnsafePredicate

  type TrimState = {
    lastOutcome: TrimOutcome,
    operationPersisted: bool,
    reportPersisted: bool,
    destinationMutated: bool,
  }

  pure def stateFor(trimCase: TrimCase): TrimState =
    match trimCase {
      | DryRunRowKeys => {
          lastOutcome: DryRun({
            candidateRows: 2,
            rowsDeleted: 0,
            destinationMutated: false,
          }),
          operationPersisted: true,
          reportPersisted: true,
          destinationMutated: false,
        }
      | ApplyWithoutConfirmation => {
          lastOutcome: RejectedMissingConfirmation,
          operationPersisted: false,
          reportPersisted: false,
          destinationMutated: false,
        }
      | ApplyRowKeysWithConfirmation => {
          lastOutcome: Applied({
            candidateRows: 1,
            rowsDeleted: 1,
            destinationMutated: true,
          }),
          operationPersisted: true,
          reportPersisted: true,
          destinationMutated: true,
        }
      | ApplyDeletesTooManyRows => {
          lastOutcome: RejectedDeleteCountExceeded,
          operationPersisted: true,
          reportPersisted: false,
          destinationMutated: false,
        }
      | ApplyStaleCandidates => {
          lastOutcome: RejectedStaleCandidate,
          operationPersisted: true,
          reportPersisted: false,
          destinationMutated: false,
        }
      | EmptySelectionRejected => {
          lastOutcome: RejectedEmptySelection,
          operationPersisted: false,
          reportPersisted: false,
          destinationMutated: false,
        }
      | UnsafePredicateRejected => {
          lastOutcome: RejectedUnsafePredicate,
          operationPersisted: false,
          reportPersisted: false,
          destinationMutated: false,
        }
      | RowPredicateApply => {
          lastOutcome: Applied({
            candidateRows: 1,
            rowsDeleted: 1,
            destinationMutated: true,
          }),
          operationPersisted: true,
          reportPersisted: true,
          destinationMutated: true,
        }
      | FullTableDropApply => {
          lastOutcome: Applied({
            candidateRows: 2,
            rowsDeleted: 2,
            destinationMutated: true,
          }),
          operationPersisted: true,
          reportPersisted: true,
          destinationMutated: true,
        }
      | HorizonTrimApply => {
          lastOutcome: Applied({
            candidateRows: 1,
            rowsDeleted: 1,
            destinationMutated: true,
          }),
          operationPersisted: true,
          reportPersisted: true,
          destinationMutated: true,
        }
      | SnapshotExpiryUnsupported => {
          lastOutcome: RejectedUnsupportedTarget,
          operationPersisted: true,
          reportPersisted: false,
          destinationMutated: false,
        }
      | RawBackupExpirationUnsupported => {
          lastOutcome: RejectedUnsupportedTarget,
          operationPersisted: true,
          reportPersisted: false,
          destinationMutated: false,
        }
      | SchemaUnsupported => {
          lastOutcome: RejectedSchemaUnsupported,
          operationPersisted: true,
          reportPersisted: false,
          destinationMutated: false,
        }
    }

  var lastOutcome: TrimOutcome
  var operationPersisted: bool
  var reportPersisted: bool
  var destinationMutated: bool

  action init = all {
    lastOutcome' = NotRun,
    operationPersisted' = false,
    reportPersisted' = false,
    destinationMutated' = false,
  }

  action TrimScenario = {
    nondet trimCase = Set(
      DryRunRowKeys,
      ApplyWithoutConfirmation,
      ApplyRowKeysWithConfirmation,
      ApplyDeletesTooManyRows,
      ApplyStaleCandidates,
      EmptySelectionRejected,
      UnsafePredicateRejected,
      RowPredicateApply,
      FullTableDropApply,
      HorizonTrimApply,
      SnapshotExpiryUnsupported,
      RawBackupExpirationUnsupported,
      SchemaUnsupported,
    ).oneOf()
    all {
      lastOutcome' = stateFor(trimCase).lastOutcome,
      operationPersisted' = stateFor(trimCase).operationPersisted,
      reportPersisted' = stateFor(trimCase).reportPersisted,
      destinationMutated' = stateFor(trimCase).destinationMutated,
    }
  }

  action step = TrimScenario

  val currentState: TrimState = {
    lastOutcome: lastOutcome,
    operationPersisted: operationPersisted,
    reportPersisted: reportPersisted,
    destinationMutated: destinationMutated,
  }

  pure def reportStateIsBounded(state: TrimState): bool =
    match (state.lastOutcome) {
      | DryRun(report) =>
          report.candidateRows >= 0
            and report.rowsDeleted == 0
            and not(report.destinationMutated)
      | Applied(report) =>
          report.candidateRows >= 0
            and report.rowsDeleted > 0
            and report.rowsDeleted <= report.candidateRows
            and report.destinationMutated
      | NotRun => true
      | RejectedMissingConfirmation => true
      | RejectedUnsupportedTarget => true
      | RejectedDeleteCountExceeded => true
      | RejectedStaleCandidate => true
      | RejectedSchemaUnsupported => true
      | RejectedEmptySelection => true
      | RejectedUnsafePredicate => true
    }

  pure def rejectedCasesAreEffectFree(state: TrimState): bool =
    match (state.lastOutcome) {
      | RejectedMissingConfirmation =>
          not(state.destinationMutated) and not(state.reportPersisted)
      | RejectedUnsupportedTarget =>
          not(state.destinationMutated) and not(state.reportPersisted)
      | RejectedDeleteCountExceeded =>
          not(state.destinationMutated) and not(state.reportPersisted)
      | RejectedStaleCandidate =>
          not(state.destinationMutated) and not(state.reportPersisted)
      | RejectedSchemaUnsupported =>
          not(state.destinationMutated) and not(state.reportPersisted)
      | RejectedEmptySelection =>
          not(state.destinationMutated) and not(state.reportPersisted)
      | RejectedUnsafePredicate =>
          not(state.destinationMutated) and not(state.reportPersisted)
      | NotRun => true
      | DryRun(_) => true
      | Applied(_) => true
    }

  pure def validationRejectsBeforePersistence(state: TrimState): bool =
    match (state.lastOutcome) {
      | RejectedMissingConfirmation => not(state.operationPersisted)
      | RejectedEmptySelection => not(state.operationPersisted)
      | RejectedUnsafePredicate => not(state.operationPersisted)
      | NotRun => true
      | DryRun(_) => true
      | Applied(_) => true
      | RejectedUnsupportedTarget => true
      | RejectedDeleteCountExceeded => true
      | RejectedStaleCandidate => true
      | RejectedSchemaUnsupported => true
    }

  pure def lifecycleStateMatchesOutcome(state: TrimState): bool =
    match (state.lastOutcome) {
      | NotRun => not(state.operationPersisted) and not(state.reportPersisted)
      | DryRun(_) => state.operationPersisted and state.reportPersisted
      | Applied(_) => state.operationPersisted and state.reportPersisted
      | RejectedMissingConfirmation =>
          not(state.operationPersisted) and not(state.reportPersisted)
      | RejectedUnsupportedTarget =>
          state.operationPersisted and not(state.reportPersisted)
      | RejectedDeleteCountExceeded =>
          state.operationPersisted and not(state.reportPersisted)
      | RejectedStaleCandidate =>
          state.operationPersisted and not(state.reportPersisted)
      | RejectedSchemaUnsupported =>
          state.operationPersisted and not(state.reportPersisted)
      | RejectedEmptySelection =>
          not(state.operationPersisted) and not(state.reportPersisted)
      | RejectedUnsafePredicate =>
          not(state.operationPersisted) and not(state.reportPersisted)
    }

  val inv = all {
    reportStateIsBounded(currentState),
    rejectedCasesAreEffectFree(currentState),
    validationRejectsBeforePersistence(currentState),
    lifecycleStateMatchesOutcome(currentState),
  }

  pure def dryRunWitness(state: TrimState): bool = match (state.lastOutcome) {
    | DryRun(report) =>
        report.candidateRows > 0
          and report.rowsDeleted == 0
          and not(report.destinationMutated)
          and state.reportPersisted
    | NotRun => false
    | Applied(_) => false
    | RejectedMissingConfirmation => false
    | RejectedUnsupportedTarget => false
    | RejectedDeleteCountExceeded => false
    | RejectedStaleCandidate => false
    | RejectedSchemaUnsupported => false
    | RejectedEmptySelection => false
    | RejectedUnsafePredicate => false
  }

  pure def appliedWitness(state: TrimState): bool = match (state.lastOutcome) {
    | Applied(report) =>
        report.rowsDeleted > 0
          and report.rowsDeleted <= report.candidateRows
          and report.destinationMutated
          and state.reportPersisted
    | NotRun => false
    | DryRun(_) => false
    | RejectedMissingConfirmation => false
    | RejectedUnsupportedTarget => false
    | RejectedDeleteCountExceeded => false
    | RejectedStaleCandidate => false
    | RejectedSchemaUnsupported => false
    | RejectedEmptySelection => false
    | RejectedUnsafePredicate => false
  }

  pure def missingConfirmationWitness(state: TrimState): bool =
    state.lastOutcome == RejectedMissingConfirmation
      and not(state.operationPersisted)
      and not(state.reportPersisted)
      and not(state.destinationMutated)

  pure def staleCandidateWitness(state: TrimState): bool =
    state.lastOutcome == RejectedStaleCandidate
      and state.operationPersisted
      and not(state.reportPersisted)
      and not(state.destinationMutated)

  pure def emptySelectionWitness(state: TrimState): bool =
    state.lastOutcome == RejectedEmptySelection
      and not(state.operationPersisted)
      and not(state.reportPersisted)
      and not(state.destinationMutated)

  pure def unsafePredicateWitness(state: TrimState): bool =
    state.lastOutcome == RejectedUnsafePredicate
      and not(state.operationPersisted)
      and not(state.reportPersisted)
      and not(state.destinationMutated)

  pure def lifecycleUnsupportedWitness(state: TrimState): bool =
    state.lastOutcome == RejectedUnsupportedTarget
      and state.operationPersisted
      and not(state.reportPersisted)
      and not(state.destinationMutated)

  pure def schemaUnsupportedWitness(state: TrimState): bool =
    state.lastOutcome == RejectedSchemaUnsupported
      and state.operationPersisted
      and not(state.reportPersisted)
      and not(state.destinationMutated)

  pure def boundedRejectWitness(state: TrimState): bool =
    state.lastOutcome == RejectedDeleteCountExceeded
      and state.operationPersisted
      and not(state.reportPersisted)
      and not(state.destinationMutated)

  val dry_run_witness = dryRunWitness(currentState)
  val applied_witness = appliedWitness(currentState)
  val missing_confirmation_witness = missingConfirmationWitness(currentState)
  val stale_candidate_witness = staleCandidateWitness(currentState)
  val empty_selection_witness = emptySelectionWitness(currentState)
  val unsafe_predicate_witness = unsafePredicateWitness(currentState)
  val lifecycle_unsupported_witness = lifecycleUnsupportedWitness(currentState)
  val schema_unsupported_witness = schemaUnsupportedWitness(currentState)
  val bounded_reject_witness = boundedRejectWitness(currentState)

  run dryRunCase = assert(
    stateFor(DryRunRowKeys).lastOutcome == DryRun({
      candidateRows: 2,
      rowsDeleted: 0,
      destinationMutated: false,
    })
  )
  run missingConfirmationCase = assert(
    stateFor(ApplyWithoutConfirmation).lastOutcome == RejectedMissingConfirmation
  )
  run applyCase = assert(
    stateFor(ApplyRowKeysWithConfirmation).lastOutcome == Applied({
      candidateRows: 1,
      rowsDeleted: 1,
      destinationMutated: true,
    })
  )
  run boundedRejectCase = assert(
    stateFor(ApplyDeletesTooManyRows).lastOutcome == RejectedDeleteCountExceeded
  )
  run staleCase = assert(
    stateFor(ApplyStaleCandidates).lastOutcome == RejectedStaleCandidate
  )
  run emptySelectionCase = assert(
    stateFor(EmptySelectionRejected).lastOutcome == RejectedEmptySelection
  )
  run unsafePredicateCase = assert(
    stateFor(UnsafePredicateRejected).lastOutcome == RejectedUnsafePredicate
  )
  run rowPredicateApplyCase = assert(
    stateFor(RowPredicateApply).lastOutcome == Applied({
      candidateRows: 1,
      rowsDeleted: 1,
      destinationMutated: true,
    })
  )
  run fullDropApplyCase = assert(
    stateFor(FullTableDropApply).lastOutcome == Applied({
      candidateRows: 2,
      rowsDeleted: 2,
      destinationMutated: true,
    })
  )
  run horizonTrimApplyCase = assert(
    stateFor(HorizonTrimApply).lastOutcome == Applied({
      candidateRows: 1,
      rowsDeleted: 1,
      destinationMutated: true,
    })
  )
  run lifecycleUnsupportedCase = assert(
    stateFor(SnapshotExpiryUnsupported).lastOutcome == RejectedUnsupportedTarget
  )
  run schemaUnsupportedCase = assert(
    stateFor(SchemaUnsupported).lastOutcome == RejectedSchemaUnsupported
  )
}
