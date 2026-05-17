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
  var sourceKind: SourceKind
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
    if (pitr) DynamoDbPitrExport
    else if (enumeration and kind == AuxStorage) AuxStorageEnumeration
    else if (enumeration and kind == LocalFixture) LocalFixtureSnapshot
    else if (scan) DynamoDbParallelScan
    else if (rawReplay) RawBackupReplay
    else StreamOnly

  pure def requestedToSnapshot(requested: RequestedMethod): SnapshotMethod =
    if (requested == RequestedDynamoDbPitrExport) DynamoDbPitrExport
    else if (requested == RequestedDynamoDbParallelScan) DynamoDbParallelScan
    else if (requested == RequestedAuxStorageEnumeration) AuxStorageEnumeration
    else if (requested == RequestedRawBackupReplay) RawBackupReplay
    else if (requested == RequestedLocalFixtureSnapshot) LocalFixtureSnapshot
    else StreamOnly

  pure def methodSupportedBySource(kind: SourceKind, method: SnapshotMethod): bool =
    if (method == DynamoDbPitrExport) kind == DynamoDb
    else if (method == DynamoDbParallelScan) kind == DynamoDb
    else if (method == AuxStorageEnumeration) kind == AuxStorage
    else if (method == RawBackupReplay) kind == RawBackup
    else if (method == LocalFixtureSnapshot) kind == LocalFixture
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
    else if (methodSupportedBySource(kind, requestedSnapshot)) requestedSnapshot
    else StreamOnly
  }

  pure def planIsSafe(identity: bool, method: SnapshotMethod): bool =
    identity and method != StreamOnly

  pure def deterministicStrategy(kind: SourceKind, pitr: bool, scan: bool, enumeration: bool, rawReplay: bool): SnapshotMethod =
    chooseStrategy(kind, pitr, scan, enumeration, rawReplay)

  action init = all {
    state' = Requested,
    sourceKind' = DynamoDb,
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
    all {
      state == Requested,
      state' = SourceInspected,
      sourceKind' = kind,
      hasRowIdentity' = hasRowIdentity,
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
    sourceKind' = sourceKind,
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
      state' = if (planIsSafe(hasRowIdentity, method)) StrategyChosen else Rejected,
      sourceKind' = sourceKind,
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
    state' = ChunksEstimated,
    sourceKind' = sourceKind,
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
    state' = OperationReserved,
    sourceKind' = sourceKind,
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
    sourceKind' = sourceKind,
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
      sourceKind' = sourceKind,
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
    nondet action_choice = Set("inspect_source", "inspect_destination", "choose_strategy", "estimate_chunks", "reserve_operation", "missing_row_identity", "request_method").oneOf()
    if (action_choice == "inspect_source") InspectSource
    else if (action_choice == "inspect_destination") InspectDestination
    else if (action_choice == "choose_strategy") ChooseStrategy
    else if (action_choice == "estimate_chunks") EstimateChunks
    else if (action_choice == "reserve_operation") ReserveOperation
    else if (action_choice == "request_method") RequestMethod
    else MissingRowIdentity
  }

  val reservedPlansHaveChunks = if (state == OperationReserved) chunkCount > 0 else true
  val missingIdentityRejected = not(state == OperationReserved and hasRowIdentity == false)
  val noStreamOnlyReserved = not(state == OperationReserved and chosenMethod == StreamOnly)
  val dynamodbDoesNotUseStreamOnlyBeyondRetention =
    not(state == OperationReserved and sourceKind == DynamoDb and streamRetentionHours == 24 and chosenMethod == StreamOnly)
  val deterministicPlan =
    chosenMethod == StreamOnly or chosenMethod == requestedStrategy(requestedMethod, sourceKind, supportsPitrExport, supportsParallelScan, supportsEnumeration, supportsRawReplay)
  val requestedMethodMatchesSource =
    if (state == OperationReserved and requestedMethod != NoRequestedMethod)
    methodSupportedBySource(sourceKind, chosenMethod)
    else true

  val inv = all {
    reservedPlansHaveChunks,
    missingIdentityRejected,
    noStreamOnlyReserved,
    dynamodbDoesNotUseStreamOnlyBeyondRetention,
    deterministicPlan,
    requestedMethodMatchesSource,
  }

  run strategyExamples = all {
    assert(chooseStrategy(DynamoDb, true, true, false, false) == DynamoDbPitrExport),
    assert(chooseStrategy(DynamoDb, false, true, false, false) == DynamoDbParallelScan),
    assert(chooseStrategy(AuxStorage, false, false, true, false) == AuxStorageEnumeration),
    assert(chooseStrategy(RawBackup, false, false, false, true) == RawBackupReplay),
    assert(requestedStrategy(RequestedAuxStorageEnumeration, LocalFixture, false, false, true, false) == StreamOnly),
    assert(requestedStrategy(RequestedLocalFixtureSnapshot, LocalFixture, false, false, true, false) == LocalFixtureSnapshot),
    assert(methodSupportedBySource(LocalFixture, AuxStorageEnumeration) == false),
    assert(planIsSafe(false, DynamoDbPitrExport) == false),
    assert(planIsSafe(true, StreamOnly) == false),
  }

  val source_inspected_witness = state == SourceInspected
  val destination_inspected_witness = state == DestinationInspected
  val strategy_chosen_witness = state == StrategyChosen
  val chunks_estimated_witness = state == ChunksEstimated
  val operation_reserved_witness = state == OperationReserved
  val unsafe_rejected_witness = state == Rejected
  val delete_proof_recorded_witness = deletesCanBeProven
}
