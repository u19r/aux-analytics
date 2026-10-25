module raw_backup_mbt {
  type BackupCase =
    | MaintenanceWriteUnsupported
    | ReplayFixture
    | VerifyFixture
    | EmptyRejected
    | CorruptRejected

  type BackupResult =
    | NotRun
    | WriteRejectedUnsupported
    | ReplayComplete
    | VerifyComplete
    | RejectedEmpty
    | RejectedChecksum

  type BackupReport = {
    result: BackupResult,
    recordCount: int,
    checksumRejected: bool,
    operationPersisted: bool,
    objectPersisted: bool,
    indexPersisted: bool,
    publicationMarkerPersisted: bool,
  }

  var lastReport: BackupReport

  pure def noDurableEffects: BackupReport = {
    result: NotRun,
    recordCount: 0,
    checksumRejected: false,
    operationPersisted: false,
    objectPersisted: false,
    indexPersisted: false,
    publicationMarkerPersisted: false,
  }

  pure def fixtureEffects: BackupReport = {
    result: NotRun,
    recordCount: 0,
    checksumRejected: false,
    operationPersisted: false,
    objectPersisted: true,
    indexPersisted: true,
    publicationMarkerPersisted: false,
  }

  pure def expectedReport(backupCase: BackupCase): BackupReport =
    if (backupCase == MaintenanceWriteUnsupported) {
      result: WriteRejectedUnsupported,
      recordCount: 0,
      checksumRejected: false,
      operationPersisted: false,
      objectPersisted: false,
      indexPersisted: false,
      publicationMarkerPersisted: false,
    } else if (backupCase == ReplayFixture) {
      result: ReplayComplete,
      recordCount: 1,
      checksumRejected: false,
      operationPersisted: fixtureEffects.operationPersisted,
      objectPersisted: fixtureEffects.objectPersisted,
      indexPersisted: fixtureEffects.indexPersisted,
      publicationMarkerPersisted: fixtureEffects.publicationMarkerPersisted,
    } else if (backupCase == VerifyFixture) {
      result: VerifyComplete,
      recordCount: 1,
      checksumRejected: false,
      operationPersisted: fixtureEffects.operationPersisted,
      objectPersisted: fixtureEffects.objectPersisted,
      indexPersisted: fixtureEffects.indexPersisted,
      publicationMarkerPersisted: fixtureEffects.publicationMarkerPersisted,
    } else if (backupCase == EmptyRejected) {
      result: RejectedEmpty,
      recordCount: 0,
      checksumRejected: false,
      operationPersisted: false,
      objectPersisted: false,
      indexPersisted: false,
      publicationMarkerPersisted: false,
    } else {
      result: RejectedChecksum,
      recordCount: 1,
      checksumRejected: true,
      operationPersisted: fixtureEffects.operationPersisted,
      objectPersisted: fixtureEffects.objectPersisted,
      indexPersisted: fixtureEffects.indexPersisted,
      publicationMarkerPersisted: fixtureEffects.publicationMarkerPersisted,
    }

  action init = lastReport' = noDurableEffects

  action BackupScenario = {
    nondet backupCase = Set(
      MaintenanceWriteUnsupported,
      ReplayFixture,
      VerifyFixture,
      EmptyRejected,
      CorruptRejected,
    ).oneOf()
    all {
      lastReport' = expectedReport(backupCase),
    }
  }

  action step = BackupScenario

  val writeRejectionHasNoEffects =
    if (lastReport.result == WriteRejectedUnsupported)
      not(lastReport.operationPersisted)
        and not(lastReport.objectPersisted)
        and not(lastReport.indexPersisted)
        and not(lastReport.publicationMarkerPersisted)
    else true
  val replayRequiresCommittedFixture =
    if (lastReport.result == ReplayComplete)
      lastReport.recordCount > 0
        and not(lastReport.checksumRejected)
        and not(lastReport.operationPersisted)
        and lastReport.objectPersisted
        and lastReport.indexPersisted
        and not(lastReport.publicationMarkerPersisted)
    else true
  val verifyRequiresCommittedFixture =
    if (lastReport.result == VerifyComplete)
      lastReport.recordCount > 0
        and not(lastReport.checksumRejected)
        and not(lastReport.operationPersisted)
        and lastReport.objectPersisted
        and lastReport.indexPersisted
        and not(lastReport.publicationMarkerPersisted)
    else true
  val emptyRejectionHasNoOutput =
    if (lastReport.result == RejectedEmpty)
      lastReport.recordCount == 0
        and not(lastReport.checksumRejected)
        and not(lastReport.operationPersisted)
        and not(lastReport.objectPersisted)
        and not(lastReport.indexPersisted)
        and not(lastReport.publicationMarkerPersisted)
    else true
  val checksumRejectionHasEvidence =
    if (lastReport.result == RejectedChecksum)
      lastReport.recordCount > 0
        and lastReport.checksumRejected
        and not(lastReport.operationPersisted)
        and lastReport.objectPersisted
        and lastReport.indexPersisted
        and not(lastReport.publicationMarkerPersisted)
    else true
  val reportCountsAreNonnegative = lastReport.recordCount >= 0
  val inv = all {
    writeRejectionHasNoEffects,
    replayRequiresCommittedFixture,
    verifyRequiresCommittedFixture,
    emptyRejectionHasNoOutput,
    checksumRejectionHasEvidence,
    reportCountsAreNonnegative,
  }

  run expectedResults = all {
    assert(expectedReport(MaintenanceWriteUnsupported).result == WriteRejectedUnsupported),
    assert(expectedReport(ReplayFixture).result == ReplayComplete),
    assert(expectedReport(VerifyFixture).result == VerifyComplete),
    assert(expectedReport(EmptyRejected).result == RejectedEmpty),
    assert(expectedReport(CorruptRejected).checksumRejected),
  }

  run hostileTrace = all {
    assert(not(expectedReport(MaintenanceWriteUnsupported).operationPersisted)),
    assert(expectedReport(ReplayFixture).objectPersisted),
    assert(expectedReport(VerifyFixture).indexPersisted),
    assert(not(expectedReport(EmptyRejected).objectPersisted)),
    assert(expectedReport(CorruptRejected).checksumRejected),
    assert(not(expectedReport(CorruptRejected).publicationMarkerPersisted)),
  }

  val write_unsupported_witness =
    lastReport.result == WriteRejectedUnsupported
      and not(lastReport.operationPersisted)
      and not(lastReport.objectPersisted)
      and not(lastReport.indexPersisted)
      and not(lastReport.publicationMarkerPersisted)
  val replay_complete_witness =
    lastReport.result == ReplayComplete and lastReport.recordCount > 0
  val verify_complete_witness =
    lastReport.result == VerifyComplete and lastReport.recordCount > 0
  val empty_rejected_witness = lastReport.result == RejectedEmpty and lastReport.recordCount == 0
  val checksum_rejected_witness =
    lastReport.result == RejectedChecksum and lastReport.checksumRejected
}
