module privacy_fix {
  type FixPhase = Scan | Rewrite | Verify | Cleanup | Completed | Rejected
  type FixAction = DryRun | RewriteCleanPrefix | DeleteWithoutConfirmation | DeleteWithConfirmation

  type FixState = {
    phase: FixPhase,
    dryRun: bool,
    destinationMutated: bool,
    taintedDeleted: bool,
    cleanVerified: bool,
    destructiveConfirmed: bool,
  }

  pure def runAction(fixAction: FixAction): FixState =
    match fixAction {
      | DryRun => {
          phase: Completed,
          dryRun: true,
          destinationMutated: false,
          taintedDeleted: false,
          cleanVerified: false,
          destructiveConfirmed: false,
        }
      | RewriteCleanPrefix => {
          phase: Completed,
          dryRun: false,
          destinationMutated: true,
          taintedDeleted: false,
          cleanVerified: true,
          destructiveConfirmed: false,
        }
      | DeleteWithoutConfirmation => {
          phase: Rejected,
          dryRun: false,
          destinationMutated: false,
          taintedDeleted: false,
          cleanVerified: false,
          destructiveConfirmed: false,
        }
      | DeleteWithConfirmation => {
          phase: Completed,
          dryRun: false,
          destinationMutated: true,
          taintedDeleted: true,
          cleanVerified: true,
          destructiveConfirmed: true,
        }
    }

  var state: FixState

  action init = state' = runAction(DryRun)

  action RemediationScenario = {
    nondet fixAction = Set(DryRun, RewriteCleanPrefix, DeleteWithoutConfirmation, DeleteWithConfirmation).oneOf()
    all { state' = runAction(fixAction) }
  }

  action step = RemediationScenario

  val dryRunDoesNotMutate = if (state.dryRun) not(state.destinationMutated) else true
  val deletionRequiresConfirmation = if (state.taintedDeleted) state.destructiveConfirmed else true
  val cleanupRequiresCleanVerification = if (state.taintedDeleted) state.cleanVerified else true

  val inv = all {
    dryRunDoesNotMutate,
    deletionRequiresConfirmation,
    cleanupRequiresCleanVerification,
  }

  run dryRunWitness = assert(not(runAction(DryRun).destinationMutated))
  run rewriteWitness = assert(runAction(RewriteCleanPrefix).cleanVerified)
  run deleteRejectedWitness = assert(runAction(DeleteWithoutConfirmation).phase == Rejected)
}
