module ingest_hashed_range {
  type Processor = ProcessorA | ProcessorB | ProcessorC | ProcessorD | ProcessorE | QueryOnly
  type Generation = GenA1 | GenA2 | GenB1 | GenC1 | GenD1 | GenE1
  type Slot = Slot0 | Slot1 | Slot2 | Slot3
  type Version = V0 | V1 | V2 | V3

  type Scenario =
    | NotChecked
    | SingleProcessorAllSlots
    | TwoProcessorRebalance
    | FailedProcessorTakeover
    | GracefulLeaveRelease
    | ZeroToFiveJoin
    | LostLeaseNoProgress
    | ProgressThenTrim
    | BootstrapRetry
    | QueryOnlySkipped

  var lastScenario: Scenario
  var activeProcessors: Set[Processor]
  var ownedSlots: Set[Slot]
  var progressVersion: Version
  var trimmedThrough: Version
  var validLeaseHeld: bool
  var ingestSucceeded: bool
  var progressSaveSucceeded: bool
  var queryOnlyOwnedSlot: bool
  var bootstrapCommittedBy: Set[Generation]

  pure val allSlots: Set[Slot] = Set(Slot0, Slot1, Slot2, Slot3)
  pure val fiveProcessors: Set[Processor] = Set(ProcessorA, ProcessorB, ProcessorC, ProcessorD, ProcessorE)

  pure def versionRank(version: Version): int =
    match version {
      | V0 => 0
      | V1 => 1
      | V2 => 2
      | V3 => 3
    }

  pure def monotonic(before: Version, after: Version): bool =
    versionRank(after) >= versionRank(before)

  pure def canAdvanceProgress(leaseHeld: bool, ingestOk: bool, saveOk: bool): bool =
    leaseHeld and ingestOk and saveOk

  pure def trimAllowed(progress: Version, trimTo: Version, saveOk: bool): bool =
    saveOk and versionRank(trimTo) <= versionRank(progress)

  pure def isActiveForAssignment(lastSeenMs: int, nowMs: int, leaseDurationMs: int): bool =
    lastSeenMs >= nowMs - leaseDurationMs

  pure def leaseDurationValid(leaseDurationMs: int, heartbeatIntervalMs: int): bool =
    leaseDurationMs >= heartbeatIntervalMs * 2

  action init = all {
    lastScenario' = NotChecked,
    activeProcessors' = Set(),
    ownedSlots' = Set(),
    progressVersion' = V0,
    trimmedThrough' = V0,
    validLeaseHeld' = false,
    ingestSucceeded' = false,
    progressSaveSucceeded' = false,
    queryOnlyOwnedSlot' = false,
    bootstrapCommittedBy' = Set(),
  }

  action CheckSingleProcessorAllSlots = all {
    lastScenario' = SingleProcessorAllSlots,
    activeProcessors' = Set(ProcessorA),
    ownedSlots' = allSlots,
    progressVersion' = V1,
    trimmedThrough' = V0,
    validLeaseHeld' = true,
    ingestSucceeded' = true,
    progressSaveSucceeded' = true,
    queryOnlyOwnedSlot' = false,
    bootstrapCommittedBy' = Set(GenA1),
  }

  action CheckTwoProcessorRebalance = all {
    lastScenario' = TwoProcessorRebalance,
    activeProcessors' = Set(ProcessorA, ProcessorB),
    ownedSlots' = Set(Slot0, Slot2),
    progressVersion' = V1,
    trimmedThrough' = V0,
    validLeaseHeld' = true,
    ingestSucceeded' = true,
    progressSaveSucceeded' = true,
    queryOnlyOwnedSlot' = false,
    bootstrapCommittedBy' = Set(GenA1),
  }

  action CheckFailedProcessorTakeover = all {
    lastScenario' = FailedProcessorTakeover,
    activeProcessors' = Set(ProcessorB),
    ownedSlots' = Set(Slot1, Slot3),
    progressVersion' = V2,
    trimmedThrough' = V1,
    validLeaseHeld' = true,
    ingestSucceeded' = true,
    progressSaveSucceeded' = true,
    queryOnlyOwnedSlot' = false,
    bootstrapCommittedBy' = Set(GenB1),
  }

  action CheckGracefulLeaveRelease = all {
    lastScenario' = GracefulLeaveRelease,
    activeProcessors' = Set(ProcessorB),
    ownedSlots' = Set(Slot0, Slot1, Slot2, Slot3),
    progressVersion' = V1,
    trimmedThrough' = V0,
    validLeaseHeld' = true,
    ingestSucceeded' = true,
    progressSaveSucceeded' = true,
    queryOnlyOwnedSlot' = false,
    bootstrapCommittedBy' = Set(GenB1),
  }

  action CheckZeroToFiveJoin = all {
    lastScenario' = ZeroToFiveJoin,
    activeProcessors' = fiveProcessors,
    ownedSlots' = Set(Slot0),
    progressVersion' = V1,
    trimmedThrough' = V0,
    validLeaseHeld' = true,
    ingestSucceeded' = true,
    progressSaveSucceeded' = true,
    queryOnlyOwnedSlot' = false,
    bootstrapCommittedBy' = Set(GenC1),
  }

  action CheckLostLeaseNoProgress = all {
    lastScenario' = LostLeaseNoProgress,
    activeProcessors' = Set(ProcessorA, ProcessorB),
    ownedSlots' = Set(Slot0),
    progressVersion' = V0,
    trimmedThrough' = V0,
    validLeaseHeld' = false,
    ingestSucceeded' = true,
    progressSaveSucceeded' = false,
    queryOnlyOwnedSlot' = false,
    bootstrapCommittedBy' = Set(GenA1),
  }

  action CheckProgressThenTrim = all {
    lastScenario' = ProgressThenTrim,
    activeProcessors' = Set(ProcessorA),
    ownedSlots' = allSlots,
    progressVersion' = V3,
    trimmedThrough' = V2,
    validLeaseHeld' = true,
    ingestSucceeded' = true,
    progressSaveSucceeded' = true,
    queryOnlyOwnedSlot' = false,
    bootstrapCommittedBy' = Set(GenA1),
  }

  action CheckBootstrapRetry = all {
    lastScenario' = BootstrapRetry,
    activeProcessors' = Set(ProcessorA, ProcessorB),
    ownedSlots' = Set(Slot0, Slot1),
    progressVersion' = V0,
    trimmedThrough' = V0,
    validLeaseHeld' = true,
    ingestSucceeded' = false,
    progressSaveSucceeded' = false,
    queryOnlyOwnedSlot' = false,
    bootstrapCommittedBy' = Set(GenB1),
  }

  action CheckQueryOnlySkipped = all {
    lastScenario' = QueryOnlySkipped,
    activeProcessors' = Set(ProcessorA),
    ownedSlots' = Set(Slot0),
    progressVersion' = V0,
    trimmedThrough' = V0,
    validLeaseHeld' = false,
    ingestSucceeded' = false,
    progressSaveSucceeded' = false,
    queryOnlyOwnedSlot' = false,
    bootstrapCommittedBy' = Set(),
  }

  action step = any {
    CheckSingleProcessorAllSlots,
    CheckTwoProcessorRebalance,
    CheckFailedProcessorTakeover,
    CheckGracefulLeaveRelease,
    CheckZeroToFiveJoin,
    CheckLostLeaseNoProgress,
    CheckProgressThenTrim,
    CheckBootstrapRetry,
    CheckQueryOnlySkipped,
  }

  val progressRequiresLease =
    progressVersion == V0 or validLeaseHeld

  val failedIngestDoesNotAdvance =
    ingestSucceeded or progressVersion == V0

  val failedSaveDoesNotTrim =
    progressSaveSucceeded or trimmedThrough == V0

  val trimNeverAheadOfProgress =
    versionRank(trimmedThrough) <= versionRank(progressVersion)

  val queryOnlyNeverOwns =
    queryOnlyOwnedSlot == false

  val singleBootstrapCommitter =
    bootstrapCommittedBy.size() <= 1

  val inv = all {
    progressRequiresLease,
    failedIngestDoesNotAdvance,
    failedSaveDoesNotTrim,
    trimNeverAheadOfProgress,
    queryOnlyNeverOwns,
    singleBootstrapCommitter,
  }

  run assignmentExamples = all {
    assert(isActiveForAssignment(95, 100, 10)),
    assert(isActiveForAssignment(89, 100, 10) == false),
    assert(leaseDurationValid(10000, 5000)),
    assert(leaseDurationValid(9000, 5000) == false),
  }

  run progressAndTrimExamples = all {
    assert(canAdvanceProgress(true, true, true)),
    assert(canAdvanceProgress(false, true, true) == false),
    assert(canAdvanceProgress(true, false, true) == false),
    assert(canAdvanceProgress(true, true, false) == false),
    assert(trimAllowed(V2, V1, true)),
    assert(trimAllowed(V1, V2, true) == false),
    assert(trimAllowed(V2, V1, false) == false),
    assert(monotonic(V1, V2)),
    assert(monotonic(V2, V1) == false),
  }

  val single_processor_all_slots_witness =
    lastScenario == SingleProcessorAllSlots and ownedSlots == allSlots

  val two_processor_rebalance_witness =
    lastScenario == TwoProcessorRebalance and activeProcessors.size() == 2 and ownedSlots.size() == 2

  val failed_processor_takeover_witness =
    lastScenario == FailedProcessorTakeover and activeProcessors == Set(ProcessorB) and validLeaseHeld

  val graceful_leave_release_witness =
    lastScenario == GracefulLeaveRelease and activeProcessors == Set(ProcessorB)

  val zero_to_five_join_witness =
    lastScenario == ZeroToFiveJoin and activeProcessors == fiveProcessors

  val lost_lease_no_progress_witness =
    lastScenario == LostLeaseNoProgress and validLeaseHeld == false and progressVersion == V0

  val progress_then_trim_witness =
    lastScenario == ProgressThenTrim and trimmedThrough == V2 and progressVersion == V3

  val bootstrap_retry_witness =
    lastScenario == BootstrapRetry and bootstrapCommittedBy == Set(GenB1)

  val query_only_skipped_witness =
    lastScenario == QueryOnlySkipped and queryOnlyOwnedSlot == false
}
