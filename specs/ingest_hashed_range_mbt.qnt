module ingest_hashed_range_mbt {
  type ProcessorMode = QueryOnly | IngestCapable
  type AssignmentResult = AssignmentNotChecked | OwnsAllSlots | OwnsSomeSlots | SkipsOwnership
  type ProgressResult = ProgressNotChecked | AdvancesProgress | BlocksProgress
  type TrimResult = TrimNotChecked | AllowsTrim | BlocksTrim
  type MarkerScanResult = MarkerScanNotChecked | PollsChangedTable | SkipsUnchangedTable
  type LeasePlanResult = LeasePlanNotChecked | AcquiresMissingLease | RenewsHeldLease | ReleasesUnownedLease

  type ProcessorCase = {
    mode: ProcessorMode,
    activeProcessors: int,
    leaseHeld: bool,
    ingestSucceeded: bool,
    progressSaved: bool,
    progressVersion: int,
    trimVersion: int,
    progressPresent: bool,
    markerVersion: int,
  }

  var lastCase: ProcessorCase
  var lastAssignment: AssignmentResult
  var lastProgress: ProgressResult
  var lastTrim: TrimResult
  var lastMarkerScan: MarkerScanResult
  var lastLeasePlan: LeasePlanResult

  pure def expectedAssignment(testCase: ProcessorCase): AssignmentResult =
    if (testCase.mode == QueryOnly) {
      SkipsOwnership
    } else if (testCase.activeProcessors == 1 and testCase.leaseHeld) {
      OwnsAllSlots
    } else if (testCase.activeProcessors > 1 and testCase.leaseHeld) {
      OwnsSomeSlots
    } else {
      SkipsOwnership
    }

  pure def expectedProgress(testCase: ProcessorCase): ProgressResult =
    if (testCase.leaseHeld and testCase.ingestSucceeded and testCase.progressSaved) {
      AdvancesProgress
    } else {
      BlocksProgress
    }

  pure def expectedTrim(testCase: ProcessorCase): TrimResult =
    if (testCase.progressVersion >= testCase.trimVersion and testCase.progressSaved) {
      AllowsTrim
    } else {
      BlocksTrim
    }

  pure def expectedMarkerScan(testCase: ProcessorCase): MarkerScanResult =
    if (not(testCase.progressPresent) or testCase.markerVersion > testCase.progressVersion) {
      PollsChangedTable
    } else {
      SkipsUnchangedTable
    }

  action init = all {
    lastCase' = {
      mode: QueryOnly,
      activeProcessors: 0,
      leaseHeld: false,
      ingestSucceeded: false,
      progressSaved: false,
      progressVersion: 0,
      trimVersion: 0,
      progressPresent: false,
      markerVersion: 0,
    },
    lastAssignment' = AssignmentNotChecked,
    lastProgress' = ProgressNotChecked,
    lastTrim' = TrimNotChecked,
    lastMarkerScan' = MarkerScanNotChecked,
    lastLeasePlan' = LeasePlanNotChecked,
  }

  action CheckProcessorCase = {
    nondet mode = Set(QueryOnly, IngestCapable).oneOf()
    nondet activeProcessors = Set(0, 1, 2, 5).oneOf()
    nondet leaseHeld = Set(true, false).oneOf()
    nondet ingestSucceeded = Set(true, false).oneOf()
    nondet progressSaved = Set(true, false).oneOf()
    nondet progressVersion = Set(0, 1, 2).oneOf()
    nondet trimVersion = Set(0, 1, 2).oneOf()
    nondet progressPresent = Set(true, false).oneOf()
    nondet markerVersion = Set(0, 1, 2).oneOf()
    val testCase = {
      mode: mode,
      activeProcessors: activeProcessors,
      leaseHeld: leaseHeld,
      ingestSucceeded: ingestSucceeded,
      progressSaved: progressSaved,
      progressVersion: progressVersion,
      trimVersion: trimVersion,
      progressPresent: progressPresent,
      markerVersion: markerVersion,
    }
    all {
      lastCase' = testCase,
      lastAssignment' = expectedAssignment(testCase),
      lastProgress' = expectedProgress(testCase),
      lastTrim' = expectedTrim(testCase),
      lastMarkerScan' = expectedMarkerScan(testCase),
      lastLeasePlan' = LeasePlanNotChecked,
    }
  }

  action CheckQueryOnlySkipped = {
    val testCase = {
      mode: QueryOnly,
      activeProcessors: 5,
      leaseHeld: false,
      ingestSucceeded: false,
      progressSaved: false,
      progressVersion: 0,
      trimVersion: 0,
      progressPresent: false,
      markerVersion: 0,
    }
    all {
      lastCase' = testCase,
      lastAssignment' = expectedAssignment(testCase),
      lastProgress' = expectedProgress(testCase),
      lastTrim' = expectedTrim(testCase),
      lastMarkerScan' = expectedMarkerScan(testCase),
      lastLeasePlan' = LeasePlanNotChecked,
    }
  }

  action CheckSingleProcessorOwnsAll = {
    val testCase = {
      mode: IngestCapable,
      activeProcessors: 1,
      leaseHeld: true,
      ingestSucceeded: false,
      progressSaved: false,
      progressVersion: 0,
      trimVersion: 0,
      progressPresent: false,
      markerVersion: 1,
    }
    all {
      lastCase' = testCase,
      lastAssignment' = expectedAssignment(testCase),
      lastProgress' = expectedProgress(testCase),
      lastTrim' = expectedTrim(testCase),
      lastMarkerScan' = expectedMarkerScan(testCase),
      lastLeasePlan' = LeasePlanNotChecked,
    }
  }

  action CheckMultiProcessorOwnsSome = {
    val testCase = {
      mode: IngestCapable,
      activeProcessors: 2,
      leaseHeld: true,
      ingestSucceeded: false,
      progressSaved: false,
      progressVersion: 0,
      trimVersion: 0,
      progressPresent: true,
      markerVersion: 1,
    }
    all {
      lastCase' = testCase,
      lastAssignment' = expectedAssignment(testCase),
      lastProgress' = expectedProgress(testCase),
      lastTrim' = expectedTrim(testCase),
      lastMarkerScan' = expectedMarkerScan(testCase),
      lastLeasePlan' = LeasePlanNotChecked,
    }
  }

  action CheckFailedProcessorTakeover = {
    val testCase = {
      mode: IngestCapable,
      activeProcessors: 1,
      leaseHeld: true,
      ingestSucceeded: true,
      progressSaved: true,
      progressVersion: 2,
      trimVersion: 1,
      progressPresent: true,
      markerVersion: 2,
    }
    all {
      lastCase' = testCase,
      lastAssignment' = expectedAssignment(testCase),
      lastProgress' = expectedProgress(testCase),
      lastTrim' = expectedTrim(testCase),
      lastMarkerScan' = expectedMarkerScan(testCase),
      lastLeasePlan' = LeasePlanNotChecked,
    }
  }

  action CheckProgressAdvance = {
    val testCase = {
      mode: IngestCapable,
      activeProcessors: 1,
      leaseHeld: true,
      ingestSucceeded: true,
      progressSaved: true,
      progressVersion: 2,
      trimVersion: 1,
      progressPresent: true,
      markerVersion: 2,
    }
    all {
      lastCase' = testCase,
      lastAssignment' = expectedAssignment(testCase),
      lastProgress' = expectedProgress(testCase),
      lastTrim' = expectedTrim(testCase),
      lastMarkerScan' = expectedMarkerScan(testCase),
      lastLeasePlan' = LeasePlanNotChecked,
    }
  }

  action CheckProgressBlockedWithoutLease = {
    val testCase = {
      mode: IngestCapable,
      activeProcessors: 1,
      leaseHeld: false,
      ingestSucceeded: true,
      progressSaved: true,
      progressVersion: 2,
      trimVersion: 1,
      progressPresent: true,
      markerVersion: 2,
    }
    all {
      lastCase' = testCase,
      lastAssignment' = expectedAssignment(testCase),
      lastProgress' = expectedProgress(testCase),
      lastTrim' = expectedTrim(testCase),
      lastMarkerScan' = expectedMarkerScan(testCase),
      lastLeasePlan' = LeasePlanNotChecked,
    }
  }

  action CheckTrimBlockedAheadOfProgress = {
    val testCase = {
      mode: IngestCapable,
      activeProcessors: 1,
      leaseHeld: true,
      ingestSucceeded: true,
      progressSaved: true,
      progressVersion: 1,
      trimVersion: 2,
      progressPresent: true,
      markerVersion: 1,
    }
    all {
      lastCase' = testCase,
      lastAssignment' = expectedAssignment(testCase),
      lastProgress' = expectedProgress(testCase),
      lastTrim' = expectedTrim(testCase),
      lastMarkerScan' = expectedMarkerScan(testCase),
      lastLeasePlan' = LeasePlanNotChecked,
    }
  }

  action CheckMarkerWithoutProgressPolls = {
    val testCase = {
      mode: IngestCapable,
      activeProcessors: 1,
      leaseHeld: true,
      ingestSucceeded: false,
      progressSaved: false,
      progressVersion: 0,
      trimVersion: 0,
      progressPresent: false,
      markerVersion: 1,
    }
    all {
      lastCase' = testCase,
      lastAssignment' = expectedAssignment(testCase),
      lastProgress' = expectedProgress(testCase),
      lastTrim' = expectedTrim(testCase),
      lastMarkerScan' = expectedMarkerScan(testCase),
      lastLeasePlan' = LeasePlanNotChecked,
    }
  }

  action CheckNewerMarkerPolls = {
    val testCase = {
      mode: IngestCapable,
      activeProcessors: 1,
      leaseHeld: true,
      ingestSucceeded: false,
      progressSaved: false,
      progressVersion: 1,
      trimVersion: 0,
      progressPresent: true,
      markerVersion: 2,
    }
    all {
      lastCase' = testCase,
      lastAssignment' = expectedAssignment(testCase),
      lastProgress' = expectedProgress(testCase),
      lastTrim' = expectedTrim(testCase),
      lastMarkerScan' = expectedMarkerScan(testCase),
      lastLeasePlan' = LeasePlanNotChecked,
    }
  }

  action CheckOldMarkerSkips = {
    val testCase = {
      mode: IngestCapable,
      activeProcessors: 1,
      leaseHeld: true,
      ingestSucceeded: false,
      progressSaved: false,
      progressVersion: 2,
      trimVersion: 0,
      progressPresent: true,
      markerVersion: 1,
    }
    all {
      lastCase' = testCase,
      lastAssignment' = expectedAssignment(testCase),
      lastProgress' = expectedProgress(testCase),
      lastTrim' = expectedTrim(testCase),
      lastMarkerScan' = expectedMarkerScan(testCase),
      lastLeasePlan' = LeasePlanNotChecked,
    }
  }

  action CheckLeaseAcquire = all {
    lastLeasePlan' = AcquiresMissingLease,
    lastCase' = lastCase,
    lastAssignment' = lastAssignment,
    lastProgress' = lastProgress,
    lastTrim' = lastTrim,
    lastMarkerScan' = lastMarkerScan,
  }

  action CheckLeaseRenew = all {
    lastLeasePlan' = RenewsHeldLease,
    lastCase' = lastCase,
    lastAssignment' = lastAssignment,
    lastProgress' = lastProgress,
    lastTrim' = lastTrim,
    lastMarkerScan' = lastMarkerScan,
  }

  action CheckLeaseRelease = all {
    lastLeasePlan' = ReleasesUnownedLease,
    lastCase' = lastCase,
    lastAssignment' = lastAssignment,
    lastProgress' = lastProgress,
    lastTrim' = lastTrim,
    lastMarkerScan' = lastMarkerScan,
  }

  action step = any {
    CheckQueryOnlySkipped,
    CheckSingleProcessorOwnsAll,
    CheckMultiProcessorOwnsSome,
    CheckFailedProcessorTakeover,
    CheckProgressAdvance,
    CheckProgressBlockedWithoutLease,
    CheckTrimBlockedAheadOfProgress,
    CheckMarkerWithoutProgressPolls,
    CheckNewerMarkerPolls,
    CheckOldMarkerSkips,
    CheckLeaseAcquire,
    CheckLeaseRenew,
    CheckLeaseRelease,
  }

  val queryOnlySkipped =
    lastAssignment == AssignmentNotChecked or lastCase.mode != QueryOnly or lastAssignment == SkipsOwnership

  val trimBounded =
    lastTrim == TrimNotChecked or lastTrim != AllowsTrim or lastCase.progressVersion >= lastCase.trimVersion

  val progressAdvancedOnlyAfterSuccess =
    lastProgress == ProgressNotChecked or lastProgress != AdvancesProgress or (
      lastCase.leaseHeld and lastCase.ingestSucceeded and lastCase.progressSaved
    )

  val changedMarkerPolls =
    lastMarkerScan == MarkerScanNotChecked or lastMarkerScan != SkipsUnchangedTable or (
      lastCase.progressPresent and lastCase.progressVersion >= lastCase.markerVersion
    )

  val leasePlanUsesModeledActions =
    lastLeasePlan == LeasePlanNotChecked or lastLeasePlan == AcquiresMissingLease or
    lastLeasePlan == RenewsHeldLease or lastLeasePlan == ReleasesUnownedLease

  val inv = all {
    queryOnlySkipped,
    trimBounded,
    progressAdvancedOnlyAfterSuccess,
    changedMarkerPolls,
    leasePlanUsesModeledActions,
  }
}
