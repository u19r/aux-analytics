// Backfill planning inspects the actual source and destination before
// reserving an operation. A requested method is either supported by that
// source or rejected; it is never silently replaced by stream-only replay.
module backfill_planning {
  type SourceKind = DynamoDb | AuxStorage | RawBackup | LocalFixture
  type SnapshotMethod =
    | DynamoDbPitrExport
    | DynamoDbParallelScan
    | AuxStorageEnumeration
    | RawBackupReplay
    | LocalFixtureSnapshot
    | StreamOnly
  type RequestedMethod =
    | NoRequestedMethod
    | RequestedDynamoDbPitrExport
    | RequestedDynamoDbParallelScan
    | RequestedAuxStorageEnumeration
    | RequestedRawBackupReplay
    | RequestedLocalFixtureSnapshot
  type PlanState =
    | Requested
    | SourceInspected
    | DestinationInspected
    | StrategyChosen
    | ChunksEstimated
    | OperationReserved
    | Rejected

  var state: PlanState
  var operationIdentity: int
  var sourceKind: SourceKind
  var metadataReachable: bool
  var destinationReady: bool
  var hasRowIdentity: bool
  var supportsPitrExport: bool
  var supportsParallelScan: bool
  var supportsEnumeration: bool
  var supportsRawReplay: bool
  var streamRetentionHours: int
  var chosenMethod: SnapshotMethod
  var requestedMethod: RequestedMethod
  var chunkCount: int
  var deletesCanBeProven: bool

  pure def chooseStrategy(
    kind: SourceKind,
    pitr: bool,
    scan: bool,
    enumeration: bool,
    rawReplay: bool,
  ): SnapshotMethod =
    if (kind == DynamoDb and pitr) DynamoDbPitrExport
    else if (kind == DynamoDb and scan) DynamoDbParallelScan
    else if (kind == AuxStorage and enumeration) AuxStorageEnumeration
    else if (kind == LocalFixture and enumeration) LocalFixtureSnapshot
    else if (kind == RawBackup and rawReplay) RawBackupReplay
    else StreamOnly

  pure def requestedToSnapshot(requested: RequestedMethod): SnapshotMethod =
    if (requested == RequestedDynamoDbPitrExport) DynamoDbPitrExport
    else if (requested == RequestedDynamoDbParallelScan) DynamoDbParallelScan
    else if (requested == RequestedAuxStorageEnumeration) AuxStorageEnumeration
    else if (requested == RequestedRawBackupReplay) RawBackupReplay
    else if (requested == RequestedLocalFixtureSnapshot) LocalFixtureSnapshot
    else StreamOnly

  pure def methodSupportedBySource(kind: SourceKind, method: SnapshotMethod): bool =
    if (method == DynamoDbPitrExport or method == DynamoDbParallelScan) kind == DynamoDb
    else if (method == AuxStorageEnumeration) kind == AuxStorage
    else if (method == RawBackupReplay) kind == RawBackup
    else if (method == LocalFixtureSnapshot) kind == LocalFixture
    else false

  pure def capabilitySupports(
    kind: SourceKind,
    method: SnapshotMethod,
    pitr: bool,
    scan: bool,
    enumeration: bool,
    rawReplay: bool,
  ): bool =
    if (method == DynamoDbPitrExport) kind == DynamoDb and pitr
    else if (method == DynamoDbParallelScan) kind == DynamoDb and scan
    else if (method == AuxStorageEnumeration) (kind == AuxStorage or kind == LocalFixture) and enumeration
    else if (method == RawBackupReplay) kind == RawBackup and rawReplay
    else if (method == LocalFixtureSnapshot) kind == LocalFixture and enumeration
    else false

  pure def requestedStrategy(
    requested: RequestedMethod,
    kind: SourceKind,
    pitr: bool,
    scan: bool,
    enumeration: bool,
    rawReplay: bool,
  ): SnapshotMethod = {
    pure val requestedSnapshot = requestedToSnapshot(requested)
    if (requested == NoRequestedMethod) chooseStrategy(kind, pitr, scan, enumeration, rawReplay)
    else if (methodSupportedBySource(kind, requestedSnapshot) and capabilitySupports(kind, requestedSnapshot, pitr, scan, enumeration, rawReplay)) requestedSnapshot
    else StreamOnly
  }

  pure def planIsSafe(identity: bool, metadata: bool, destination: bool, method: SnapshotMethod): bool =
    identity and metadata and destination and method != StreamOnly

  action init = all {
    state' = Requested,
    operationIdentity' = 1,
    sourceKind' = DynamoDb,
    metadataReachable' = false,
    destinationReady' = false,
    hasRowIdentity' = true,
    supportsPitrExport' = true,
    supportsParallelScan' = true,
    supportsEnumeration' = false,
    supportsRawReplay' = false,
    streamRetentionHours' = 24,
    chosenMethod' = StreamOnly,
    requestedMethod' = NoRequestedMethod,
    chunkCount' = 0,
    deletesCanBeProven' = false,
  }

  action InspectSource = {
    nondet kind = Set(DynamoDb, AuxStorage, RawBackup, LocalFixture).oneOf()
    nondet identity = Set(true, false).oneOf()
    nondet reachable = Set(true, false).oneOf()
    all {
      state == Requested,
      state' = SourceInspected,
      operationIdentity' = operationIdentity,
      sourceKind' = kind,
      metadataReachable' = reachable,
      destinationReady' = destinationReady,
      hasRowIdentity' = identity,
      supportsPitrExport' = kind == DynamoDb,
      supportsParallelScan' = kind == DynamoDb,
      supportsEnumeration' = kind == AuxStorage or kind == LocalFixture,
      supportsRawReplay' = kind == RawBackup,
      streamRetentionHours' = if (kind == DynamoDb) 24 else 0,
      chosenMethod' = chosenMethod,
      requestedMethod' = requestedMethod,
      chunkCount' = chunkCount,
      deletesCanBeProven' = kind == AuxStorage or kind == LocalFixture,
    }
  }

  action InspectDestination = all {
    state == SourceInspected,
    state' = DestinationInspected,
    operationIdentity' = operationIdentity,
    sourceKind' = sourceKind,
    metadataReachable' = metadataReachable,
    destinationReady' = true,
    hasRowIdentity' = hasRowIdentity,
    supportsPitrExport' = supportsPitrExport,
    supportsParallelScan' = supportsParallelScan,
    supportsEnumeration' = supportsEnumeration,
    supportsRawReplay' = supportsRawReplay,
    streamRetentionHours' = streamRetentionHours,
    chosenMethod' = chosenMethod,
    requestedMethod' = requestedMethod,
    chunkCount' = chunkCount,
    deletesCanBeProven' = deletesCanBeProven,
  }

  action ChooseStrategy = {
    val method = requestedStrategy(requestedMethod, sourceKind, supportsPitrExport, supportsParallelScan, supportsEnumeration, supportsRawReplay)
    all {
      state == DestinationInspected,
      state' = if (planIsSafe(hasRowIdentity, metadataReachable, destinationReady, method)) StrategyChosen else Rejected,
      operationIdentity' = operationIdentity,
      sourceKind' = sourceKind,
      metadataReachable' = metadataReachable,
      destinationReady' = destinationReady,
      hasRowIdentity' = hasRowIdentity,
      supportsPitrExport' = supportsPitrExport,
      supportsParallelScan' = supportsParallelScan,
      supportsEnumeration' = supportsEnumeration,
      supportsRawReplay' = supportsRawReplay,
      streamRetentionHours' = streamRetentionHours,
      chosenMethod' = method,
      requestedMethod' = requestedMethod,
      chunkCount' = chunkCount,
      deletesCanBeProven' = deletesCanBeProven,
    }
  }

  action EstimateChunks = all {
    state == StrategyChosen,
    chosenMethod != StreamOnly,
    state' = ChunksEstimated,
    operationIdentity' = operationIdentity,
    sourceKind' = sourceKind,
    metadataReachable' = metadataReachable,
    destinationReady' = destinationReady,
    hasRowIdentity' = hasRowIdentity,
    supportsPitrExport' = supportsPitrExport,
    supportsParallelScan' = supportsParallelScan,
    supportsEnumeration' = supportsEnumeration,
    supportsRawReplay' = supportsRawReplay,
    streamRetentionHours' = streamRetentionHours,
    chosenMethod' = chosenMethod,
    requestedMethod' = requestedMethod,
    chunkCount' = 1,
    deletesCanBeProven' = deletesCanBeProven,
  }

  action ReserveOperation = all {
    state == ChunksEstimated,
    planIsSafe(hasRowIdentity, metadataReachable, destinationReady, chosenMethod),
    chunkCount > 0,
    state' = OperationReserved,
    operationIdentity' = operationIdentity,
    sourceKind' = sourceKind,
    metadataReachable' = metadataReachable,
    destinationReady' = destinationReady,
    hasRowIdentity' = hasRowIdentity,
    supportsPitrExport' = supportsPitrExport,
    supportsParallelScan' = supportsParallelScan,
    supportsEnumeration' = supportsEnumeration,
    supportsRawReplay' = supportsRawReplay,
    streamRetentionHours' = streamRetentionHours,
    chosenMethod' = chosenMethod,
    requestedMethod' = requestedMethod,
    chunkCount' = chunkCount,
    deletesCanBeProven' = deletesCanBeProven,
  }

  action MissingRowIdentity = all {
    state == Requested,
    state' = Requested,
    operationIdentity' = operationIdentity,
    sourceKind' = sourceKind,
    metadataReachable' = metadataReachable,
    destinationReady' = destinationReady,
    hasRowIdentity' = false,
    supportsPitrExport' = supportsPitrExport,
    supportsParallelScan' = supportsParallelScan,
    supportsEnumeration' = supportsEnumeration,
    supportsRawReplay' = supportsRawReplay,
    streamRetentionHours' = streamRetentionHours,
    chosenMethod' = chosenMethod,
    requestedMethod' = requestedMethod,
    chunkCount' = chunkCount,
    deletesCanBeProven' = deletesCanBeProven,
  }

  action RequestMethod = {
    nondet requested = Set(RequestedDynamoDbPitrExport, RequestedDynamoDbParallelScan, RequestedAuxStorageEnumeration, RequestedRawBackupReplay, RequestedLocalFixtureSnapshot).oneOf()
    all {
      state == Requested,
      state' = state,
      operationIdentity' = operationIdentity,
      sourceKind' = sourceKind,
      metadataReachable' = metadataReachable,
      destinationReady' = destinationReady,
      hasRowIdentity' = hasRowIdentity,
      supportsPitrExport' = supportsPitrExport,
      supportsParallelScan' = supportsParallelScan,
      supportsEnumeration' = supportsEnumeration,
      supportsRawReplay' = supportsRawReplay,
      streamRetentionHours' = streamRetentionHours,
      chosenMethod' = chosenMethod,
      requestedMethod' = requested,
      chunkCount' = chunkCount,
      deletesCanBeProven' = deletesCanBeProven,
    }
  }

  action step = {
    nondet actionChoice = Set("inspect_source", "inspect_destination", "choose_strategy", "estimate_chunks", "reserve_operation", "missing_row_identity", "request_method").oneOf()
    if (actionChoice == "inspect_source") InspectSource
    else if (actionChoice == "inspect_destination") InspectDestination
    else if (actionChoice == "choose_strategy") ChooseStrategy
    else if (actionChoice == "estimate_chunks") EstimateChunks
    else if (actionChoice == "reserve_operation") ReserveOperation
    else if (actionChoice == "request_method") RequestMethod
    else MissingRowIdentity
  }

  val reservedPlansHaveChunks = if (state == OperationReserved) chunkCount > 0 else true
  val missingIdentityRejected = not(state == OperationReserved and hasRowIdentity == false)
  val noStreamOnlyReserved = not(state == OperationReserved and chosenMethod == StreamOnly)
  val metadataRequiredToReserve = if (state == OperationReserved) metadataReachable else true
  val destinationRequiredToReserve = if (state == OperationReserved) destinationReady else true
  val sourceMatchesChosenMethod = if (state == StrategyChosen or state == ChunksEstimated or state == OperationReserved) methodSupportedBySource(sourceKind, chosenMethod) else true
  val deterministicPlan = if (state == StrategyChosen or state == ChunksEstimated or state == OperationReserved) chosenMethod == requestedStrategy(requestedMethod, sourceKind, supportsPitrExport, supportsParallelScan, supportsEnumeration, supportsRawReplay) else true
  val requestedMethodMatchesSource = if (state == OperationReserved and requestedMethod != NoRequestedMethod) methodSupportedBySource(sourceKind, chosenMethod) else true
  val stableOperationIdentity = operationIdentity == 1

  val inv = all {
    reservedPlansHaveChunks,
    missingIdentityRejected,
    noStreamOnlyReserved,
    metadataRequiredToReserve,
    destinationRequiredToReserve,
    sourceMatchesChosenMethod,
    deterministicPlan,
    requestedMethodMatchesSource,
    stableOperationIdentity,
  }

  run strategyExamples = all {
    assert(chooseStrategy(DynamoDb, true, true, false, false) == DynamoDbPitrExport),
    assert(chooseStrategy(DynamoDb, false, true, false, false) == DynamoDbParallelScan),
    assert(chooseStrategy(AuxStorage, false, false, true, false) == AuxStorageEnumeration),
    assert(chooseStrategy(RawBackup, false, false, false, true) == RawBackupReplay),
    assert(requestedStrategy(RequestedLocalFixtureSnapshot, LocalFixture, false, false, true, false) == LocalFixtureSnapshot),
    assert(requestedStrategy(RequestedAuxStorageEnumeration, LocalFixture, false, false, true, false) == StreamOnly),
  }

  val source_inspected_witness = state == SourceInspected
  val destination_inspected_witness = state == DestinationInspected
  val strategy_chosen_witness = state == StrategyChosen
  val chunks_estimated_witness = state == ChunksEstimated
  val operation_reserved_witness = state == OperationReserved
  val unsafe_rejected_witness = state == Rejected
  val delete_proof_recorded_witness = deletesCanBeProven
  val missing_metadata_rejected_witness = state == Rejected and not(metadataReachable)
  val wrong_requested_method_witness = state == Rejected and requestedMethod != NoRequestedMethod
}
