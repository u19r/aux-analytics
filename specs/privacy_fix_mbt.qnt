module privacy_fix_mbt {
  type FixCase =
    | DryRunRawBackup
    | RewriteRawBackup
    | DeleteWithoutConfirmation

  type FixResult =
    | NotRun
    | DryRunComplete
    | CleanRewriteComplete
    | RejectedMissingConfirmation

  type FixReport = {
    result: FixResult,
    scannedObjects: int,
    taintedObjects: int,
    rewrittenObjects: int,
    destinationMutated: bool,
    cleanReplayVerified: bool,
  }

  pure def defaultReport: FixReport = {
    result: NotRun,
    scannedObjects: 0,
    taintedObjects: 0,
    rewrittenObjects: 0,
    destinationMutated: false,
    cleanReplayVerified: false,
  }

  pure def reportFor(fixCase: FixCase): FixReport =
    match fixCase {
      | DryRunRawBackup => {
          result: DryRunComplete,
          scannedObjects: 1,
          taintedObjects: 1,
          rewrittenObjects: 0,
          destinationMutated: false,
          cleanReplayVerified: true,
        }
      | RewriteRawBackup => {
          result: CleanRewriteComplete,
          scannedObjects: 1,
          taintedObjects: 1,
          rewrittenObjects: 1,
          destinationMutated: true,
          cleanReplayVerified: true,
        }
      | DeleteWithoutConfirmation => {
          result: RejectedMissingConfirmation,
          scannedObjects: 0,
          taintedObjects: 0,
          rewrittenObjects: 0,
          destinationMutated: false,
          cleanReplayVerified: false,
        }
    }

  var lastReport: FixReport

  action init = lastReport' = defaultReport

  action FixScenario = {
    nondet fixCase = Set(DryRunRawBackup, RewriteRawBackup, DeleteWithoutConfirmation).oneOf()
    all { lastReport' = reportFor(fixCase) }
  }

  action step = FixScenario

  val dryRunDoesNotMutate =
    if (lastReport.result == DryRunComplete) not(lastReport.destinationMutated) else true
  val rewriteVerifiesCleanReplay =
    if (lastReport.result == CleanRewriteComplete) lastReport.cleanReplayVerified else true
  val rejectedDoesNotMutate =
    if (lastReport.result == RejectedMissingConfirmation) not(lastReport.destinationMutated) else true

  val inv = all {
    dryRunDoesNotMutate,
    rewriteVerifiesCleanReplay,
    rejectedDoesNotMutate,
  }

  run dryRunCase = assert(reportFor(DryRunRawBackup).destinationMutated == false)
  run rewriteCase = assert(reportFor(RewriteRawBackup).rewrittenObjects == 1)
  run rejectedCase = assert(reportFor(DeleteWithoutConfirmation).result == RejectedMissingConfirmation)
}
