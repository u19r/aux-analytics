module raw_backup_mbt {
  type BackupCase = WriteReplay | EmptyRejected | CorruptRejected
  type BackupResult = NotRun | ReplayComplete | RejectedEmpty | RejectedChecksum

  type BackupReport = {
    result: BackupResult,
    recordCount: int,
    checksumRejected: bool,
  }

  var lastReport: BackupReport

  pure def expectedReport(backupCase: BackupCase): BackupReport =
    if (backupCase == WriteReplay) {
      result: ReplayComplete,
      recordCount: 1,
      checksumRejected: false,
    } else if (backupCase == EmptyRejected) {
      result: RejectedEmpty,
      recordCount: 0,
      checksumRejected: false,
    } else {
      result: RejectedChecksum,
      recordCount: 1,
      checksumRejected: true,
    }

  action init = all {
    lastReport' = {
      result: NotRun,
      recordCount: 0,
      checksumRejected: false,
    },
  }

  action BackupScenario = {
    nondet backupCase = Set(WriteReplay, EmptyRejected, CorruptRejected).oneOf()
    all {
      lastReport' = expectedReport(backupCase),
    }
  }

  action step = BackupScenario

  val inv = if (lastReport.result == ReplayComplete) lastReport.recordCount > 0 else true

  run expectedResults = all {
    assert(expectedReport(WriteReplay).result == ReplayComplete),
    assert(expectedReport(EmptyRejected).result == RejectedEmpty),
    assert(expectedReport(CorruptRejected).checksumRejected),
  }
}
