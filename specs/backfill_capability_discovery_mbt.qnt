module backfill_capability_discovery_mbt {
  type BackendKind = DynamoDb | AuxStorage | RawBackup | LocalFixture | Unsupported
  type DiscoveryResult =
    | NotChecked
    | DiscoveredDynamoDbFull
    | DiscoveredDynamoDbScanOnly
    | DiscoveredAuxStorageFull
    | DiscoveredRawBackupReplay
    | DiscoveredLocalFixtureFull
    | RejectedUnsupported
    | RejectedPartialUnsupported

  type DiscoveryCase = {
    backendKind: BackendKind,
    metadataReachable: bool,
    pitrEnabled: bool,
    scanEnabled: bool,
    enumerationEnabled: bool,
    rawReplayEnabled: bool,
  }

  var lastCase: DiscoveryCase
  var lastResult: DiscoveryResult

  pure def expectedDiscovery(discoveryCase: DiscoveryCase): DiscoveryResult =
    if (not(discoveryCase.metadataReachable)) RejectedPartialUnsupported
    else if (discoveryCase.backendKind == DynamoDb and discoveryCase.pitrEnabled and discoveryCase.scanEnabled) DiscoveredDynamoDbFull
    else if (discoveryCase.backendKind == DynamoDb and discoveryCase.scanEnabled) DiscoveredDynamoDbScanOnly
    else if (discoveryCase.backendKind == AuxStorage and discoveryCase.enumerationEnabled) DiscoveredAuxStorageFull
    else if (discoveryCase.backendKind == RawBackup and discoveryCase.rawReplayEnabled) DiscoveredRawBackupReplay
    else if (discoveryCase.backendKind == LocalFixture and discoveryCase.enumerationEnabled) DiscoveredLocalFixtureFull
    else if (discoveryCase.backendKind == Unsupported) RejectedUnsupported
    else RejectedPartialUnsupported

  action init = all {
    lastCase' = {
      backendKind: Unsupported,
      metadataReachable: false,
      pitrEnabled: false,
      scanEnabled: false,
      enumerationEnabled: false,
      rawReplayEnabled: false,
    },
    lastResult' = NotChecked,
  }

  action CheckDiscovery = {
    nondet backendKind = Set(DynamoDb, AuxStorage, RawBackup, LocalFixture, Unsupported).oneOf()
    nondet metadataReachable = Set(true, false).oneOf()
    nondet pitrEnabled = Set(true, false).oneOf()
    nondet scanEnabled = Set(true, false).oneOf()
    nondet enumerationEnabled = Set(true, false).oneOf()
    nondet rawReplayEnabled = Set(true, false).oneOf()
    val discoveryCase = {
      backendKind: backendKind,
      metadataReachable: metadataReachable,
      pitrEnabled: pitrEnabled,
      scanEnabled: scanEnabled,
      enumerationEnabled: enumerationEnabled,
      rawReplayEnabled: rawReplayEnabled,
    }
    all {
      lastCase' = discoveryCase,
      lastResult' = expectedDiscovery(discoveryCase),
    }
  }

  action step = CheckDiscovery

  val unsupportedRejected =
    if (lastCase.backendKind == Unsupported and lastResult != NotChecked) lastResult == RejectedUnsupported or lastResult == RejectedPartialUnsupported else true
  val unreachableRejected =
    if (not(lastCase.metadataReachable) and lastResult != NotChecked) lastResult == RejectedPartialUnsupported else true
  val auxStorageRequiresEnumeration =
    if (lastCase.backendKind == AuxStorage and lastResult == DiscoveredAuxStorageFull) lastCase.enumerationEnabled else true
  val inv = all {
    unsupportedRejected,
    unreachableRejected,
    auxStorageRequiresEnumeration,
  }

  run capabilityDiscoveryMbtExamples = all {
    assert(expectedDiscovery({
      backendKind: DynamoDb,
      metadataReachable: true,
      pitrEnabled: true,
      scanEnabled: true,
      enumerationEnabled: false,
      rawReplayEnabled: false,
    }) == DiscoveredDynamoDbFull),
    assert(expectedDiscovery({
      backendKind: AuxStorage,
      metadataReachable: true,
      pitrEnabled: false,
      scanEnabled: false,
      enumerationEnabled: false,
      rawReplayEnabled: false,
    }) == RejectedPartialUnsupported),
  }
}
