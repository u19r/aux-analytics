module table_fix_mbt {
  type FixCase =
    | DryRunRawBackup
    | ApplyCurrentSource
    | ApplyTwoCurrentSource
    | EmptySelectionRejected
    | MissingConfirmationRejected
    | OversizedSelectionRejected
    | PostFixValidationRejected
    | StaleTruthRejected
    | VerificationFailedRejected

  type FixReport = {
    selectedRows: int,
    repairedRows: int,
    destinationMutated: bool,
  }

  // A rejected request has no report payload. This keeps counts sourced from
  // the persisted production report instead of manufacturing values for an
  // error path.
  type FixOutcome =
    | NotRun
    | DryRun(FixReport)
    | Applied(FixReport)
    | RejectedEmptySelection
    | RejectedMissingConfirmation
    | RejectedRepairSetTooLarge
    | RejectedPostFixValidation
    | RejectedStaleTruth
    | RejectedDestinationVerification

  type FixState = {
    lastOutcome: FixOutcome,
    operationPersisted: bool,
    reportPersisted: bool,
    operationSucceeded: bool,
    destinationMutated: bool,
  }

  pure def stateFor(fixCase: FixCase): FixState =
    match fixCase {
      | DryRunRawBackup => {
          lastOutcome: DryRun({
            selectedRows: 1,
            repairedRows: 0,
            destinationMutated: false,
          }),
          operationPersisted: true,
          reportPersisted: true,
          operationSucceeded: true,
          destinationMutated: false,
        }
      | ApplyCurrentSource => {
          lastOutcome: Applied({
            selectedRows: 1,
            repairedRows: 1,
            destinationMutated: true,
          }),
          operationPersisted: true,
          reportPersisted: true,
          operationSucceeded: true,
          destinationMutated: true,
        }
      | ApplyTwoCurrentSource => {
          lastOutcome: Applied({
            selectedRows: 2,
            repairedRows: 2,
            destinationMutated: true,
          }),
          operationPersisted: true,
          reportPersisted: true,
          operationSucceeded: true,
          destinationMutated: true,
        }
      | EmptySelectionRejected => {
          lastOutcome: RejectedEmptySelection,
          operationPersisted: false,
          reportPersisted: false,
          operationSucceeded: false,
          destinationMutated: false,
        }
      | MissingConfirmationRejected => {
          lastOutcome: RejectedMissingConfirmation,
          operationPersisted: false,
          reportPersisted: false,
          operationSucceeded: false,
          destinationMutated: false,
        }
      | OversizedSelectionRejected => {
          lastOutcome: RejectedRepairSetTooLarge,
          operationPersisted: true,
          reportPersisted: false,
          operationSucceeded: false,
          destinationMutated: false,
        }
      | PostFixValidationRejected => {
          lastOutcome: RejectedPostFixValidation,
          operationPersisted: true,
          reportPersisted: false,
          operationSucceeded: false,
          destinationMutated: false,
        }
      | StaleTruthRejected => {
          lastOutcome: RejectedStaleTruth,
          operationPersisted: true,
          reportPersisted: false,
          operationSucceeded: false,
          destinationMutated: false,
        }
      | VerificationFailedRejected => {
          lastOutcome: RejectedDestinationVerification,
          operationPersisted: true,
          reportPersisted: false,
          operationSucceeded: false,
          // The production destination effect is committed before its
          // post-write verifier reports failure. The operation is paused and
          // has no success/report evidence, but the attempted row effect is
          // intentionally observable for retry reconciliation.
          destinationMutated: true,
        }
    }

  var lastOutcome: FixOutcome
  var operationPersisted: bool
  var reportPersisted: bool
  var operationSucceeded: bool
  var destinationMutated: bool

  action init = all {
    lastOutcome' = NotRun,
    operationPersisted' = false,
    reportPersisted' = false,
    operationSucceeded' = false,
    destinationMutated' = false,
  }

  action FixScenario = {
    nondet fixCase = Set(
      DryRunRawBackup,
      ApplyCurrentSource,
      ApplyTwoCurrentSource,
      EmptySelectionRejected,
      MissingConfirmationRejected,
      OversizedSelectionRejected,
      PostFixValidationRejected,
      StaleTruthRejected,
      VerificationFailedRejected,
    ).oneOf()
    all {
      lastOutcome' = stateFor(fixCase).lastOutcome,
      operationPersisted' = stateFor(fixCase).operationPersisted,
      reportPersisted' = stateFor(fixCase).reportPersisted,
      operationSucceeded' = stateFor(fixCase).operationSucceeded,
      destinationMutated' = stateFor(fixCase).destinationMutated,
    }
  }

  action step = FixScenario

  val currentState: FixState = {
    lastOutcome: lastOutcome,
    operationPersisted: operationPersisted,
    reportPersisted: reportPersisted,
    operationSucceeded: operationSucceeded,
    destinationMutated: destinationMutated,
  }

  pure def reportStateIsBounded(state: FixState): bool =
    match (state.lastOutcome) {
      | DryRun(report) =>
          report.selectedRows >= 0
            and report.repairedRows == 0
            and not(report.destinationMutated)
      | Applied(report) =>
          report.selectedRows >= 0
            and report.repairedRows > 0
            and report.repairedRows == report.selectedRows
            and report.destinationMutated
      | NotRun
          => true
      | RejectedEmptySelection => true
      | RejectedMissingConfirmation => true
      | RejectedRepairSetTooLarge => true
      | RejectedPostFixValidation => true
      | RejectedStaleTruth => true
      | RejectedDestinationVerification => true
    }

  pure def rejectedCasesHaveNoSuccessEvidence(state: FixState): bool =
    match (state.lastOutcome) {
      | RejectedEmptySelection =>
          not(state.destinationMutated) and not(state.reportPersisted)
      | RejectedMissingConfirmation =>
          not(state.destinationMutated) and not(state.reportPersisted)
      | RejectedRepairSetTooLarge =>
          not(state.destinationMutated) and not(state.reportPersisted)
      | RejectedPostFixValidation =>
          not(state.destinationMutated) and not(state.reportPersisted)
      | RejectedStaleTruth =>
          not(state.destinationMutated) and not(state.reportPersisted)
      | RejectedDestinationVerification =>
          state.destinationMutated and not(state.reportPersisted)
      | NotRun => true
      | DryRun(_) => true
      | Applied(_) => true
    }

  pure def validationRejectsBeforePersistence(state: FixState): bool =
    match (state.lastOutcome) {
      | RejectedEmptySelection => not(state.operationPersisted)
      | RejectedMissingConfirmation => not(state.operationPersisted)
      | NotRun => true
      | DryRun(_) => true
      | Applied(_) => true
      | RejectedRepairSetTooLarge => true
      | RejectedPostFixValidation => true
      | RejectedStaleTruth => true
      | RejectedDestinationVerification => true
    }

  pure def lifecycleStateMatchesOutcome(state: FixState): bool =
    match (state.lastOutcome) {
      | NotRun => not(state.operationPersisted) and not(state.reportPersisted)
      | DryRun(_) => state.operationPersisted and state.reportPersisted
      | Applied(_) => state.operationPersisted and state.reportPersisted
      | RejectedEmptySelection =>
          not(state.operationPersisted) and not(state.reportPersisted)
      | RejectedMissingConfirmation =>
          not(state.operationPersisted) and not(state.reportPersisted)
      | RejectedRepairSetTooLarge =>
          state.operationPersisted and not(state.reportPersisted)
      | RejectedPostFixValidation =>
          state.operationPersisted and not(state.reportPersisted)
      | RejectedStaleTruth =>
          state.operationPersisted and not(state.reportPersisted)
      | RejectedDestinationVerification =>
          state.operationPersisted and not(state.reportPersisted)
    }

  val successOnlyHasPersistedReport =
    operationSucceeded == reportPersisted

  val inv = all {
    reportStateIsBounded(currentState),
    rejectedCasesHaveNoSuccessEvidence(currentState),
    validationRejectsBeforePersistence(currentState),
    lifecycleStateMatchesOutcome(currentState),
    successOnlyHasPersistedReport,
  }

  run dryRunCase = assert(
    stateFor(DryRunRawBackup).lastOutcome == DryRun({
      selectedRows: 1,
      repairedRows: 0,
      destinationMutated: false,
    })
  )
  run applyCase = assert(stateFor(ApplyCurrentSource).lastOutcome == Applied({
    selectedRows: 1,
    repairedRows: 1,
    destinationMutated: true,
  }))
  run twoRowApplyCase = assert(stateFor(ApplyTwoCurrentSource).lastOutcome == Applied({
    selectedRows: 2,
    repairedRows: 2,
    destinationMutated: true,
  }))
  run staleRejectCase = assert(
    stateFor(StaleTruthRejected).lastOutcome == RejectedStaleTruth
  )
  run emptySelectionCase = assert(
    stateFor(EmptySelectionRejected).lastOutcome == RejectedEmptySelection
  )
  run missingConfirmationCase = assert(
    stateFor(MissingConfirmationRejected).lastOutcome == RejectedMissingConfirmation
  )
  run oversizedSelectionCase = assert(
    stateFor(OversizedSelectionRejected).lastOutcome == RejectedRepairSetTooLarge
  )
  run postFixValidationCase = assert(
    stateFor(PostFixValidationRejected).lastOutcome == RejectedPostFixValidation
  )
  run verificationFailureCase = assert(
    stateFor(VerificationFailedRejected).lastOutcome == RejectedDestinationVerification
  )

  pure def dryRunWitness(state: FixState): bool = match (state.lastOutcome) {
    | DryRun(report) => not(report.destinationMutated) and state.reportPersisted
    | NotRun => false
    | Applied(_) => false
    | RejectedEmptySelection => false
    | RejectedMissingConfirmation => false
    | RejectedRepairSetTooLarge => false
    | RejectedPostFixValidation => false
    | RejectedStaleTruth => false
    | RejectedDestinationVerification => false
  }
  pure def appliedWitness(state: FixState): bool = match (state.lastOutcome) {
    | Applied(report) => report.destinationMutated and state.destinationMutated
    | NotRun => false
    | DryRun(_) => false
    | RejectedEmptySelection => false
    | RejectedMissingConfirmation => false
    | RejectedRepairSetTooLarge => false
    | RejectedPostFixValidation => false
    | RejectedStaleTruth => false
    | RejectedDestinationVerification => false
  }
  pure def completeAppliedWitness(state: FixState): bool = match (state.lastOutcome) {
    | Applied(report) =>
        report.selectedRows == 2
          and report.repairedRows == 2
          and report.destinationMutated
          and state.destinationMutated
          and state.operationPersisted
          and state.reportPersisted
          and state.operationSucceeded
    | NotRun => false
    | DryRun(_) => false
    | RejectedEmptySelection => false
    | RejectedMissingConfirmation => false
    | RejectedRepairSetTooLarge => false
    | RejectedPostFixValidation => false
    | RejectedStaleTruth => false
    | RejectedDestinationVerification => false
  }
  val dry_run_witness = dryRunWitness(currentState)
  val applied_witness = appliedWitness(currentState)
  val complete_apply_witness = completeAppliedWitness(currentState)
  val stale_rejection_witness =
    lastOutcome == RejectedStaleTruth
      and not(destinationMutated)
      and operationPersisted
      and not(reportPersisted)
  val empty_selection_witness =
    lastOutcome == RejectedEmptySelection
      and not(operationPersisted)
      and not(reportPersisted)
      and not(destinationMutated)
  val missing_confirmation_witness =
    lastOutcome == RejectedMissingConfirmation
      and not(operationPersisted)
      and not(reportPersisted)
      and not(destinationMutated)
  val oversized_selection_witness =
    lastOutcome == RejectedRepairSetTooLarge
      and operationPersisted
      and not(operationSucceeded)
      and not(reportPersisted)
      and not(destinationMutated)
  val validation_failure_witness =
    lastOutcome == RejectedPostFixValidation
      and operationPersisted
      and not(operationSucceeded)
      and not(reportPersisted)
      and not(destinationMutated)
  val verification_failure_witness =
    lastOutcome == RejectedDestinationVerification
      and operationPersisted
      and not(operationSucceeded)
      and not(reportPersisted)
      and destinationMutated
}
