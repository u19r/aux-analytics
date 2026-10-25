module privacy_fix_mbt {
  type FixCase =
    | DryRunRawBackup
    | ApplyRawBackupRejected
    | DeleteWithoutConfirmation
    | DeleteWithConfirmationRejected

  type FixResult =
    | NotRun
    | DryRunComplete
    | DestinationFenceUnavailable
    | RejectedMissingConfirmation

  type FixReport = {
    result: FixResult,
    scannedObjects: int,
    taintedObjects: int,
    rewrittenObjects: int,
    destinationMutated: bool,
    cleanReplayVerified: bool,
    operationPersisted: bool,
    reportPersisted: bool,
    cleanTargetMutated: bool,
    quarantineMutated: bool,
    sourceDeleted: bool,
    progressPersisted: bool,
  }

  pure def defaultReport: FixReport = {
    result: NotRun,
    scannedObjects: 0,
    taintedObjects: 0,
    rewrittenObjects: 0,
    destinationMutated: false,
    cleanReplayVerified: false,
    operationPersisted: false,
    reportPersisted: false,
    cleanTargetMutated: false,
    quarantineMutated: false,
    sourceDeleted: false,
    progressPersisted: false,
  }

  pure def fenceRejectedReport: FixReport = {
    result: DestinationFenceUnavailable,
    scannedObjects: 0,
    taintedObjects: 0,
    rewrittenObjects: 0,
    destinationMutated: false,
    cleanReplayVerified: false,
    operationPersisted: false,
    reportPersisted: false,
    cleanTargetMutated: false,
    quarantineMutated: false,
    sourceDeleted: false,
    progressPersisted: false,
  }

  pure def reportFor(fixCase: FixCase): FixReport =
    match fixCase {
      | DryRunRawBackup => {
          result: DryRunComplete,
          scannedObjects: 1,
          taintedObjects: 1,
          rewrittenObjects: 0,
          destinationMutated: false,
          cleanReplayVerified: false,
          operationPersisted: true,
          reportPersisted: true,
          cleanTargetMutated: false,
          quarantineMutated: false,
          sourceDeleted: false,
          progressPersisted: false,
        }
      | ApplyRawBackupRejected => fenceRejectedReport
      | DeleteWithoutConfirmation => {
          result: RejectedMissingConfirmation,
          scannedObjects: 0,
          taintedObjects: 0,
          rewrittenObjects: 0,
          destinationMutated: false,
          cleanReplayVerified: false,
          operationPersisted: false,
          reportPersisted: false,
          cleanTargetMutated: false,
          quarantineMutated: false,
          sourceDeleted: false,
          progressPersisted: false,
        }
      | DeleteWithConfirmationRejected => fenceRejectedReport
    }

  var lastReport: FixReport

  action init = lastReport' = defaultReport

  action FixScenario = {
    nondet fixCase = Set(
      DryRunRawBackup,
      ApplyRawBackupRejected,
      DeleteWithoutConfirmation,
      DeleteWithConfirmationRejected,
    ).oneOf()
    all { lastReport' = reportFor(fixCase) }
  }

  action step = FixScenario

  val reportCountsAreBounded = all {
    lastReport.scannedObjects >= 0,
    lastReport.taintedObjects >= 0,
    lastReport.rewrittenObjects >= 0,
    lastReport.taintedObjects <= lastReport.scannedObjects,
    lastReport.rewrittenObjects <= lastReport.scannedObjects,
  }
  val dryRunIsReadOnly =
    if (lastReport.result == DryRunComplete)
      lastReport.rewrittenObjects == 0
        and not(lastReport.destinationMutated)
        and not(lastReport.cleanReplayVerified)
        and lastReport.operationPersisted
        and lastReport.reportPersisted
        and not(lastReport.cleanTargetMutated)
        and not(lastReport.quarantineMutated)
        and not(lastReport.sourceDeleted)
        and not(lastReport.progressPersisted)
    else true
  val fenceRejectionIsNoOp =
    if (lastReport.result == DestinationFenceUnavailable)
      lastReport.scannedObjects == 0
        and lastReport.taintedObjects == 0
        and lastReport.rewrittenObjects == 0
        and not(lastReport.destinationMutated)
        and not(lastReport.cleanReplayVerified)
        and not(lastReport.operationPersisted)
        and not(lastReport.reportPersisted)
        and not(lastReport.cleanTargetMutated)
        and not(lastReport.quarantineMutated)
        and not(lastReport.sourceDeleted)
        and not(lastReport.progressPersisted)
    else true
  val confirmationRejectionIsNoOp =
    if (lastReport.result == RejectedMissingConfirmation)
      lastReport.scannedObjects == 0
        and lastReport.taintedObjects == 0
        and lastReport.rewrittenObjects == 0
        and not(lastReport.destinationMutated)
        and not(lastReport.operationPersisted)
        and not(lastReport.reportPersisted)
        and not(lastReport.cleanTargetMutated)
        and not(lastReport.quarantineMutated)
        and not(lastReport.sourceDeleted)
        and not(lastReport.progressPersisted)
    else true
  val destinationMutationRequiresVerifiedReplay =
    if (lastReport.destinationMutated)
      lastReport.rewrittenObjects > 0 and lastReport.cleanReplayVerified
    else true
  val inv = all {
    reportCountsAreBounded,
    dryRunIsReadOnly,
    fenceRejectionIsNoOp,
    confirmationRejectionIsNoOp,
    destinationMutationRequiresVerifiedReplay,
  }

  run dryRunCase = all {
    assert(reportFor(DryRunRawBackup).result == DryRunComplete),
    assert(reportFor(DryRunRawBackup).operationPersisted),
    assert(reportFor(DryRunRawBackup).reportPersisted),
    assert(not(reportFor(DryRunRawBackup).destinationMutated)),
  }
  run applyFenceRejectionCase = all {
    assert(reportFor(ApplyRawBackupRejected).result == DestinationFenceUnavailable),
    assert(not(reportFor(ApplyRawBackupRejected).operationPersisted)),
    assert(not(reportFor(ApplyRawBackupRejected).cleanTargetMutated)),
  }
  run rejectedCase = assert(reportFor(DeleteWithoutConfirmation).result == RejectedMissingConfirmation)
  run destructiveFenceRejectionCase = all {
    assert(reportFor(DeleteWithConfirmationRejected).result == DestinationFenceUnavailable),
    assert(not(reportFor(DeleteWithConfirmationRejected).sourceDeleted)),
    assert(not(reportFor(DeleteWithConfirmationRejected).progressPersisted)),
  }
  run hostileTrace = all {
    assert(reportFor(DryRunRawBackup).scannedObjects == 1),
    assert(reportFor(ApplyRawBackupRejected).scannedObjects == 0),
    assert(reportFor(DeleteWithoutConfirmation).result == RejectedMissingConfirmation),
    assert(reportFor(DeleteWithConfirmationRejected).result == DestinationFenceUnavailable),
    assert(not(reportFor(DeleteWithConfirmationRejected).destinationMutated)),
  }

  val dry_run_witness =
    lastReport.result == DryRunComplete
      and lastReport.operationPersisted
      and lastReport.reportPersisted
      and not(lastReport.destinationMutated)
  val apply_fence_rejection_witness =
    lastReport.result == DestinationFenceUnavailable
      and not(lastReport.operationPersisted)
      and not(lastReport.destinationMutated)
      and not(lastReport.cleanTargetMutated)
  val confirmation_rejection_witness =
    lastReport.result == RejectedMissingConfirmation
      and not(lastReport.destinationMutated)
      and not(lastReport.operationPersisted)
}
