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
    if (requestedMethod == RequestedDynamoDbPitrExport) sourceKind == DynamoDb
    else if (requestedMethod == RequestedDynamoDbParallelScan) sourceKind == DynamoDb
    else if (requestedMethod == RequestedAuxStorageEnumeration) sourceKind == AuxStorage
    else if (requestedMethod == RequestedRawBackupReplay) sourceKind == RawBackup
    else if (requestedMethod == RequestedLocalFixtureSnapshot) sourceKind == LocalFixture
    else true

  pure def requestedResult(planCase: PlanCase): PlanResult =
    if (not(planCase.hasRowIdentity)) RejectedMissingIdentity
    else if (planCase.requestedMethod == RequestedDynamoDbPitrExport and planCase.sourceKind == DynamoDb and planCase.supportsPitrExport) PlannedDynamoDbPitrExport
    else if (planCase.requestedMethod == RequestedDynamoDbParallelScan and planCase.sourceKind == DynamoDb and planCase.supportsParallelScan) PlannedDynamoDbParallelScan
    else if (planCase.requestedMethod == RequestedAuxStorageEnumeration and planCase.sourceKind == AuxStorage and planCase.supportsEnumeration) PlannedAuxStorageEnumeration
    else if (planCase.requestedMethod == RequestedRawBackupReplay and planCase.sourceKind == RawBackup and planCase.supportsRawReplay) PlannedRawBackupReplay
    else if (planCase.requestedMethod == RequestedLocalFixtureSnapshot and planCase.sourceKind == LocalFixture and planCase.supportsEnumeration) PlannedLocalFixtureSnapshot
    else if (planCase.sourceKind == DynamoDb) RejectedUnsafeStreamOnly
    else RejectedHistoricalEnumerationUnsupported

  pure def inferredResult(planCase: PlanCase): PlanResult =
    if (not(planCase.hasRowIdentity)) RejectedMissingIdentity
    else if (planCase.sourceKind == DynamoDb and planCase.supportsPitrExport) PlannedDynamoDbPitrExport
    else if (planCase.supportsEnumeration and planCase.sourceKind == AuxStorage) PlannedAuxStorageEnumeration
    else if (planCase.supportsEnumeration and planCase.sourceKind == LocalFixture) PlannedLocalFixtureSnapshot
    else if (planCase.sourceKind == DynamoDb and planCase.supportsParallelScan) PlannedDynamoDbParallelScan
    else if (planCase.supportsRawReplay and planCase.sourceKind == RawBackup) PlannedRawBackupReplay
    else RejectedHistoricalEnumerationUnsupported

  pure def expectedResult(planCase: PlanCase): PlanResult =
    if (planCase.requestedMethod == NoRequestedMethod) inferredResult(planCase)
    else requestedResult(planCase)

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
  val inv = all {
    requestedMethodsMustMatchSource,
    missingIdentityRejected,
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
  }
}
