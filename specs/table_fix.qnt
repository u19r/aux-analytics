module table_fix {
  type FixState =
    | Planned
    | SelectingRows
    | ReadingTruth
    | ComputingRepairSet
    | DryRunComplete
    | ApplyingRepair
    | Validating
    | Succeeded
    | Rejected

  var state: FixState
  var dryRun: bool
  var selectedRows: int
  var truthRows: int
  var repairedRows: int
  var destinationMutated: bool
  var rowIdentityKnown: bool
  var staleTruthRejected: bool

  action init = all {
    state' = Planned,
    dryRun' = false,
    selectedRows' = 0,
    truthRows' = 0,
    repairedRows' = 0,
    destinationMutated' = false,
    rowIdentityKnown' = true,
    staleTruthRejected' = false,
  }

  action SelectRows = {
    nondet requestedDryRun = Set(true, false).oneOf()
    all {
      state == Planned,
      state' = SelectingRows,
      dryRun' = requestedDryRun,
      selectedRows' = selectedRows + 1,
      truthRows' = truthRows,
      repairedRows' = repairedRows,
      destinationMutated' = destinationMutated,
      rowIdentityKnown' = rowIdentityKnown,
      staleTruthRejected' = staleTruthRejected,
    }
  }

  action ReadTruth = all {
    state == SelectingRows or state == ReadingTruth,
    selectedRows > truthRows,
    state' = ReadingTruth,
    dryRun' = dryRun,
    selectedRows' = selectedRows,
    truthRows' = truthRows + 1,
    repairedRows' = repairedRows,
    destinationMutated' = destinationMutated,
    rowIdentityKnown' = rowIdentityKnown,
    staleTruthRejected' = staleTruthRejected,
  }

  action ComputeRepairSet = all {
    state == ReadingTruth,
    truthRows == selectedRows,
    rowIdentityKnown,
    state' = ComputingRepairSet,
    dryRun' = dryRun,
    selectedRows' = selectedRows,
    truthRows' = truthRows,
    repairedRows' = repairedRows,
    destinationMutated' = destinationMutated,
    rowIdentityKnown' = rowIdentityKnown,
    staleTruthRejected' = staleTruthRejected,
  }

  action CompleteDryRun = all {
    state == ComputingRepairSet,
    dryRun,
    state' = DryRunComplete,
    dryRun' = dryRun,
    selectedRows' = selectedRows,
    truthRows' = truthRows,
    repairedRows' = selectedRows,
    destinationMutated' = false,
    rowIdentityKnown' = rowIdentityKnown,
    staleTruthRejected' = staleTruthRejected,
  }

  action ApplyRepair = all {
    state == ComputingRepairSet,
    not(dryRun),
    state' = ApplyingRepair,
    dryRun' = dryRun,
    selectedRows' = selectedRows,
    truthRows' = truthRows,
    repairedRows' = selectedRows,
    destinationMutated' = true,
    rowIdentityKnown' = rowIdentityKnown,
    staleTruthRejected' = staleTruthRejected,
  }

  action ValidateRepair = all {
    state == ApplyingRepair,
    state' = Validating,
    dryRun' = dryRun,
    selectedRows' = selectedRows,
    truthRows' = truthRows,
    repairedRows' = repairedRows,
    destinationMutated' = destinationMutated,
    rowIdentityKnown' = rowIdentityKnown,
    staleTruthRejected' = staleTruthRejected,
  }

  action Succeed = all {
    state == Validating,
    state' = Succeeded,
    dryRun' = dryRun,
    selectedRows' = selectedRows,
    truthRows' = truthRows,
    repairedRows' = repairedRows,
    destinationMutated' = destinationMutated,
    rowIdentityKnown' = rowIdentityKnown,
    staleTruthRejected' = staleTruthRejected,
  }

  action RejectStaleTruth = all {
    state == ReadingTruth or state == ComputingRepairSet,
    state' = Rejected,
    dryRun' = dryRun,
    selectedRows' = selectedRows,
    truthRows' = truthRows,
    repairedRows' = repairedRows,
    destinationMutated' = destinationMutated,
    rowIdentityKnown' = rowIdentityKnown,
    staleTruthRejected' = true,
  }

  action step = any {
    SelectRows,
    ReadTruth,
    ComputeRepairSet,
    CompleteDryRun,
    ApplyRepair,
    ValidateRepair,
    Succeed,
    RejectStaleTruth,
  }

  val dryRunDoesNotMutate = if (dryRun) not(destinationMutated) else true
  val repairUsesRowIdentity = if (repairedRows > 0) rowIdentityKnown else true
  val repairedRowsBounded = repairedRows <= selectedRows
  val truthRowsBounded = truthRows <= selectedRows
  val staleTruthNeverSucceeds = if (staleTruthRejected) state == Rejected else true

  val inv = all {
    dryRunDoesNotMutate,
    repairUsesRowIdentity,
    repairedRowsBounded,
    truthRowsBounded,
    staleTruthNeverSucceeds,
  }

  run dryRunPreservesDestination = assert(if (state == DryRunComplete) not(destinationMutated) else true)
  run appliedRepairUsesIdentity = assert(if (state == Succeeded) rowIdentityKnown and destinationMutated else true)

  val dry_run_witness = state == DryRunComplete and not(destinationMutated)
  val applied_repair_witness = state == Succeeded and destinationMutated
  val stale_rejection_witness = state == Rejected and staleTruthRejected
}
