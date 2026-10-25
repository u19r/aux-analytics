// Compact planning MBT input/result contract. The same resolver is used for
// inferred and explicitly requested methods, so an unsupported request cannot
// be silently downgraded to a stream-only plan.
module backfill_planning_mbt {
  type SourceKind = DynamoDb | AuxStorage | RawBackup | LocalFixture
  type RequestedMethod =
    | NoRequestedMethod
    | RequestedDynamoDbPitrExport
    | RequestedDynamoDbParallelScan
    | RequestedAuxStorageEnumeration
    | RequestedRawBackupReplay
    | RequestedLocalFixtureSnapshot
  type PlanResult =
    | NotChecked
    | PlannedDynamoDbPitrExport
    | PlannedDynamoDbParallelScan
    | PlannedAuxStorageEnumeration
    | PlannedRawBackupReplay
    | PlannedLocalFixtureSnapshot
    | RejectedMissingIdentity
    | RejectedUnsafeStreamOnly
    | RejectedHistoricalEnumerationUnsupported

  type PlanCase = {
    sourceKind: SourceKind,
    requestedMethod: RequestedMethod,
    hasRowIdentity: bool,
    supportsPitrExport: bool,
    supportsParallelScan: bool,
    supportsEnumeration: bool,
    supportsRawReplay: bool,
  }

  var lastCase: PlanCase
  var lastResult: PlanResult

  pure def sourceSupportsMethod(sourceKind: SourceKind, requestedMethod: RequestedMethod): bool =
    if (requestedMethod == RequestedDynamoDbPitrExport or requestedMethod == RequestedDynamoDbParallelScan) sourceKind == DynamoDb
    else if (requestedMethod == RequestedAuxStorageEnumeration) sourceKind == AuxStorage
    else if (requestedMethod == RequestedRawBackupReplay) sourceKind == RawBackup
    else if (requestedMethod == RequestedLocalFixtureSnapshot) sourceKind == LocalFixture
    else true

  pure def requestedIsEnabled(planCase: PlanCase): bool =
    if (planCase.requestedMethod == RequestedDynamoDbPitrExport) planCase.sourceKind == DynamoDb and planCase.supportsPitrExport
    else if (planCase.requestedMethod == RequestedDynamoDbParallelScan) planCase.sourceKind == DynamoDb and planCase.supportsParallelScan
    else if (planCase.requestedMethod == RequestedAuxStorageEnumeration) planCase.sourceKind == AuxStorage and planCase.supportsEnumeration
    else if (planCase.requestedMethod == RequestedRawBackupReplay) planCase.sourceKind == RawBackup and planCase.supportsRawReplay
    else if (planCase.requestedMethod == RequestedLocalFixtureSnapshot) planCase.sourceKind == LocalFixture and planCase.supportsEnumeration
    else true

  pure def rejectedFor(sourceKind: SourceKind): PlanResult =
    if (sourceKind == DynamoDb) RejectedUnsafeStreamOnly else RejectedHistoricalEnumerationUnsupported

  pure def inferredResult(planCase: PlanCase): PlanResult =
    if (planCase.sourceKind == DynamoDb and planCase.supportsPitrExport) PlannedDynamoDbPitrExport
    else if (planCase.sourceKind == DynamoDb and planCase.supportsParallelScan) PlannedDynamoDbParallelScan
    else if (planCase.sourceKind == AuxStorage and planCase.supportsEnumeration) PlannedAuxStorageEnumeration
    else if (planCase.sourceKind == RawBackup and planCase.supportsRawReplay) PlannedRawBackupReplay
    else if (planCase.sourceKind == LocalFixture and planCase.supportsEnumeration) PlannedLocalFixtureSnapshot
    else rejectedFor(planCase.sourceKind)

  pure def requestedResult(planCase: PlanCase): PlanResult =
    if (planCase.requestedMethod == RequestedDynamoDbPitrExport) PlannedDynamoDbPitrExport
    else if (planCase.requestedMethod == RequestedDynamoDbParallelScan) PlannedDynamoDbParallelScan
    else if (planCase.requestedMethod == RequestedAuxStorageEnumeration) PlannedAuxStorageEnumeration
    else if (planCase.requestedMethod == RequestedRawBackupReplay) PlannedRawBackupReplay
    else PlannedLocalFixtureSnapshot

  pure def expectedResult(planCase: PlanCase): PlanResult =
    if (not(planCase.hasRowIdentity)) RejectedMissingIdentity
    else if (planCase.requestedMethod == NoRequestedMethod) inferredResult(planCase)
    else if (sourceSupportsMethod(planCase.sourceKind, planCase.requestedMethod) and requestedIsEnabled(planCase)) requestedResult(planCase)
    else rejectedFor(planCase.sourceKind)

  action init = all {
    lastCase' = {
      sourceKind: DynamoDb,
      requestedMethod: NoRequestedMethod,
      hasRowIdentity: true,
      supportsPitrExport: true,
      supportsParallelScan: true,
      supportsEnumeration: false,
      supportsRawReplay: false,
    },
    lastResult' = NotChecked,
  }

  action CheckPlan = {
    nondet sourceKind = Set(DynamoDb, AuxStorage, RawBackup, LocalFixture).oneOf()
    nondet requestedMethod = Set(NoRequestedMethod, RequestedDynamoDbPitrExport, RequestedDynamoDbParallelScan, RequestedAuxStorageEnumeration, RequestedRawBackupReplay, RequestedLocalFixtureSnapshot).oneOf()
    nondet hasRowIdentity = Set(true, false).oneOf()
    nondet supportsPitrExport = Set(true, false).oneOf()
    nondet supportsParallelScan = Set(true, false).oneOf()
    nondet supportsEnumeration = Set(true, false).oneOf()
    nondet supportsRawReplay = Set(true, false).oneOf()
    val planCase = {
      sourceKind: sourceKind,
      requestedMethod: requestedMethod,
      hasRowIdentity: hasRowIdentity,
      supportsPitrExport: supportsPitrExport,
      supportsParallelScan: supportsParallelScan,
      supportsEnumeration: supportsEnumeration,
      supportsRawReplay: supportsRawReplay,
    }
    all {
      lastCase' = planCase,
      lastResult' = expectedResult(planCase),
    }
  }

  action step = CheckPlan

  val requestedMethodsMustMatchSource =
    if (lastResult == PlannedDynamoDbPitrExport or lastResult == PlannedDynamoDbParallelScan) lastCase.sourceKind == DynamoDb
    else if (lastResult == PlannedAuxStorageEnumeration) lastCase.sourceKind == AuxStorage
    else if (lastResult == PlannedRawBackupReplay) lastCase.sourceKind == RawBackup
    else if (lastResult == PlannedLocalFixtureSnapshot) lastCase.sourceKind == LocalFixture
    else true
  val missingIdentityRejected = not(not(lastCase.hasRowIdentity) and lastResult != RejectedMissingIdentity and lastResult != NotChecked)
  val inferredPlanHasCapability = if (lastCase.requestedMethod == NoRequestedMethod and lastResult != NotChecked and lastResult != RejectedMissingIdentity and lastResult != RejectedUnsafeStreamOnly and lastResult != RejectedHistoricalEnumerationUnsupported) requestedIsEnabled(lastCase) else true
  val inv = all {
    requestedMethodsMustMatchSource,
    missingIdentityRejected,
    inferredPlanHasCapability,
  }

  run planningMbtExamples = all {
    assert(expectedResult({
      sourceKind: DynamoDb,
      requestedMethod: NoRequestedMethod,
      hasRowIdentity: true,
      supportsPitrExport: true,
      supportsParallelScan: true,
      supportsEnumeration: false,
      supportsRawReplay: false,
    }) == PlannedDynamoDbPitrExport),
    assert(expectedResult({
      sourceKind: LocalFixture,
      requestedMethod: RequestedAuxStorageEnumeration,
      hasRowIdentity: true,
      supportsPitrExport: false,
      supportsParallelScan: false,
      supportsEnumeration: true,
      supportsRawReplay: false,
    }) == RejectedHistoricalEnumerationUnsupported),
    assert(expectedResult({
      sourceKind: DynamoDb,
      requestedMethod: RequestedDynamoDbPitrExport,
      hasRowIdentity: false,
      supportsPitrExport: true,
      supportsParallelScan: true,
      supportsEnumeration: false,
      supportsRawReplay: false,
    }) == RejectedMissingIdentity),
  }

  val planned_witness = lastResult == PlannedDynamoDbPitrExport or lastResult == PlannedDynamoDbParallelScan or lastResult == PlannedAuxStorageEnumeration or lastResult == PlannedRawBackupReplay or lastResult == PlannedLocalFixtureSnapshot
  val missing_identity_witness = lastResult == RejectedMissingIdentity
  val unsupported_request_witness = lastResult == RejectedHistoricalEnumerationUnsupported or lastResult == RejectedUnsafeStreamOnly
}
