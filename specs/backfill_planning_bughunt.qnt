module backfill_planning_bughunt {
  type Source = DynamoDb | AuxStorage | RawBackup | LocalFixture | Unsupported
  type Strategy = Export | Scan | Enumerate | Replay | Fixture | Reject
  type Requested = None | WantExport | WantScan | WantEnumerate | WantReplay | WantFixture

  var source: Source
  var requested: Requested
  var hasIdentity: bool
  var pitr: bool
  var scan: bool
  var enumerate: bool
  var replay: bool
  var strategy: Strategy
  var reserved: bool
  var unsafeAccepted: bool
  var deleteProofRecorded: bool

  pure def choose(sourceKind: Source, canExport: bool, canScan: bool, canEnumerate: bool, canReplay: bool): Strategy =
    if (sourceKind == Unsupported) Reject
    else if (canExport) Export
    else if (canEnumerate and sourceKind == AuxStorage) Enumerate
    else if (canEnumerate and sourceKind == LocalFixture) Fixture
    else if (canScan) Scan
    else if (canReplay and sourceKind == RawBackup) Replay
    else Reject

  pure def canProveDeletes(sourceKind: Source): bool =
    sourceKind == AuxStorage or sourceKind == LocalFixture

  pure def requestedStrategy(requestedMethod: Requested, sourceKind: Source, canExport: bool, canScan: bool, canEnumerate: bool, canReplay: bool): Strategy =
    if (requestedMethod == None) choose(sourceKind, canExport, canScan, canEnumerate, canReplay)
    else if (requestedMethod == WantExport and sourceKind == DynamoDb and canExport) Export
    else if (requestedMethod == WantScan and sourceKind == DynamoDb and canScan) Scan
    else if (requestedMethod == WantEnumerate and sourceKind == AuxStorage and canEnumerate) Enumerate
    else if (requestedMethod == WantReplay and sourceKind == RawBackup and canReplay) Replay
    else if (requestedMethod == WantFixture and sourceKind == LocalFixture and canEnumerate) Fixture
    else Reject

  pure def strategyMatchesSource(strategyValue: Strategy, sourceKind: Source): bool =
    if (strategyValue == Export or strategyValue == Scan) sourceKind == DynamoDb
    else if (strategyValue == Enumerate) sourceKind == AuxStorage
    else if (strategyValue == Replay) sourceKind == RawBackup
    else if (strategyValue == Fixture) sourceKind == LocalFixture
    else true

  action init = all {
    source' = Unsupported,
    requested' = None,
    hasIdentity' = true,
    pitr' = false,
    scan' = false,
    enumerate' = false,
    replay' = false,
    strategy' = Reject,
    reserved' = false,
    unsafeAccepted' = false,
    deleteProofRecorded' = false,
  }

  action Discover = {
    nondet sourceKind = Set(DynamoDb, AuxStorage, RawBackup, LocalFixture, Unsupported).oneOf()
    nondet identity = Set(true, false).oneOf()
    all {
      source' = sourceKind,
      requested' = requested,
      hasIdentity' = identity,
      pitr' = sourceKind == DynamoDb,
      scan' = sourceKind == DynamoDb,
      enumerate' = sourceKind == AuxStorage or sourceKind == LocalFixture,
      replay' = sourceKind == RawBackup,
      strategy' = Reject,
      reserved' = false,
      unsafeAccepted' = false,
      deleteProofRecorded' = canProveDeletes(sourceKind),
    }
  }

  action Plan = {
    val nextStrategy = requestedStrategy(requested, source, pitr, scan, enumerate, replay)
    val safe = hasIdentity and nextStrategy != Reject
    all {
      source' = source,
      requested' = requested,
      hasIdentity' = hasIdentity,
      pitr' = pitr,
      scan' = scan,
      enumerate' = enumerate,
      replay' = replay,
      strategy' = nextStrategy,
      reserved' = safe,
      unsafeAccepted' = unsafeAccepted or (not(safe) and nextStrategy != Reject),
      deleteProofRecorded' = deleteProofRecorded,
    }
  }

  action RequestMethod = {
    nondet requestedMethod = Set(None, WantExport, WantScan, WantEnumerate, WantReplay, WantFixture).oneOf()
    all {
      source' = source,
      requested' = requestedMethod,
      hasIdentity' = hasIdentity,
      pitr' = pitr,
      scan' = scan,
      enumerate' = enumerate,
      replay' = replay,
      strategy' = Reject,
      reserved' = false,
      unsafeAccepted' = false,
      deleteProofRecorded' = deleteProofRecorded,
    }
  }

  action step = {
    nondet action_choice = Set("discover", "request_method", "plan").oneOf()
    if (action_choice == "discover") Discover
    else if (action_choice == "request_method") RequestMethod
    else Plan
  }

  val missingIdentityNeverReserved = not(reserved and hasIdentity == false)
  val unsupportedNeverReserved = not(reserved and source == Unsupported)
  val rejectedStrategyNeverReserved = not(reserved and strategy == Reject)
  val unsafeCombinationNeverAccepted = unsafeAccepted == false
  val deleteProofMatchesSource = deleteProofRecorded == canProveDeletes(source)
  val reservedStrategyMatchesSource = if (reserved) strategyMatchesSource(strategy, source) else true

  val inv = all {
    missingIdentityNeverReserved,
    unsupportedNeverReserved,
    rejectedStrategyNeverReserved,
    unsafeCombinationNeverAccepted,
    deleteProofMatchesSource,
    reservedStrategyMatchesSource,
  }

  run bughuntExamples = all {
    assert(choose(DynamoDb, true, true, false, false) == Export),
    assert(choose(DynamoDb, false, true, false, false) == Scan),
    assert(choose(AuxStorage, false, false, true, false) == Enumerate),
    assert(requestedStrategy(WantEnumerate, LocalFixture, false, false, true, false) == Reject),
    assert(requestedStrategy(WantFixture, LocalFixture, false, false, true, false) == Fixture),
    assert(choose(Unsupported, false, false, false, false) == Reject),
  }

  val dynamodb_plan_witness = reserved and source == DynamoDb
  val aux_storage_plan_witness = reserved and source == AuxStorage
  val raw_backup_plan_witness = reserved and source == RawBackup
  val rejected_missing_identity_witness = not(reserved) and hasIdentity == false
  val rejected_unsupported_witness = not(reserved) and source == Unsupported
  val rejected_wrong_requested_method_witness = requested == WantEnumerate and source == LocalFixture and strategy == Reject
}
