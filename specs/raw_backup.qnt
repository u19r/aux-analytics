module raw_backup {
  type BackupState =
    | Planned
    | ReceivingRecord
    | PrivacyFiltering
    | WritingObject
    | Checksumming
    | Indexing
    | Committed
    | Replaying
    | ReplayComplete
    | Failed

  var state: BackupState
  var sourceRecords: int
  var filteredRecords: int
  var objectRecords: int
  var bytesWritten: int
  var checksumComputed: bool
  var checksumValid: bool
  var indexWritten: bool
  var replayedRecords: int
  var privateFieldsWritten: bool

  action init = all {
    state' = Planned,
    sourceRecords' = 0,
    filteredRecords' = 0,
    objectRecords' = 0,
    bytesWritten' = 0,
    checksumComputed' = false,
    checksumValid' = false,
    indexWritten' = false,
    replayedRecords' = 0,
    privateFieldsWritten' = false,
  }

  action ReceiveRecord = all {
    state == Planned or state == ReceivingRecord,
    state' = ReceivingRecord,
    sourceRecords' = sourceRecords + 1,
    filteredRecords' = filteredRecords,
    objectRecords' = objectRecords,
    bytesWritten' = bytesWritten,
    checksumComputed' = checksumComputed,
    checksumValid' = checksumValid,
    indexWritten' = indexWritten,
    replayedRecords' = replayedRecords,
    privateFieldsWritten' = privateFieldsWritten,
  }

  action ApplyPrivacyPolicy = all {
    state == ReceivingRecord or state == PrivacyFiltering,
    sourceRecords > filteredRecords,
    state' = PrivacyFiltering,
    sourceRecords' = sourceRecords,
    filteredRecords' = filteredRecords + 1,
    objectRecords' = objectRecords,
    bytesWritten' = bytesWritten,
    checksumComputed' = checksumComputed,
    checksumValid' = checksumValid,
    indexWritten' = indexWritten,
    replayedRecords' = replayedRecords,
    privateFieldsWritten' = false,
  }

  action WriteObjectRecord = all {
    state == PrivacyFiltering or state == WritingObject,
    filteredRecords > objectRecords,
    state' = WritingObject,
    sourceRecords' = sourceRecords,
    filteredRecords' = filteredRecords,
    objectRecords' = objectRecords + 1,
    bytesWritten' = bytesWritten + 1,
    checksumComputed' = checksumComputed,
    checksumValid' = checksumValid,
    indexWritten' = indexWritten,
    replayedRecords' = replayedRecords,
    privateFieldsWritten' = privateFieldsWritten,
  }

  action ComputeChecksum = all {
    state == WritingObject,
    objectRecords > 0,
    state' = Checksumming,
    sourceRecords' = sourceRecords,
    filteredRecords' = filteredRecords,
    objectRecords' = objectRecords,
    bytesWritten' = bytesWritten,
    checksumComputed' = true,
    checksumValid' = true,
    indexWritten' = indexWritten,
    replayedRecords' = replayedRecords,
    privateFieldsWritten' = privateFieldsWritten,
  }

  action WriteIndex = all {
    state == Checksumming,
    checksumComputed,
    checksumValid,
    state' = Indexing,
    sourceRecords' = sourceRecords,
    filteredRecords' = filteredRecords,
    objectRecords' = objectRecords,
    bytesWritten' = bytesWritten,
    checksumComputed' = checksumComputed,
    checksumValid' = checksumValid,
    indexWritten' = true,
    replayedRecords' = replayedRecords,
    privateFieldsWritten' = privateFieldsWritten,
  }

  action CommitObject = all {
    state == Indexing,
    indexWritten,
    state' = Committed,
    sourceRecords' = sourceRecords,
    filteredRecords' = filteredRecords,
    objectRecords' = objectRecords,
    bytesWritten' = bytesWritten,
    checksumComputed' = checksumComputed,
    checksumValid' = checksumValid,
    indexWritten' = indexWritten,
    replayedRecords' = replayedRecords,
    privateFieldsWritten' = privateFieldsWritten,
  }

  action ReplayCommittedObject = all {
    state == Committed or state == Replaying,
    indexWritten,
    checksumValid,
    replayedRecords < objectRecords,
    state' = Replaying,
    sourceRecords' = sourceRecords,
    filteredRecords' = filteredRecords,
    objectRecords' = objectRecords,
    bytesWritten' = bytesWritten,
    checksumComputed' = checksumComputed,
    checksumValid' = checksumValid,
    indexWritten' = indexWritten,
    replayedRecords' = replayedRecords + 1,
    privateFieldsWritten' = privateFieldsWritten,
  }

  action CompleteReplay = all {
    state == Replaying,
    replayedRecords == objectRecords,
    state' = ReplayComplete,
    sourceRecords' = sourceRecords,
    filteredRecords' = filteredRecords,
    objectRecords' = objectRecords,
    bytesWritten' = bytesWritten,
    checksumComputed' = checksumComputed,
    checksumValid' = checksumValid,
    indexWritten' = indexWritten,
    replayedRecords' = replayedRecords,
    privateFieldsWritten' = privateFieldsWritten,
  }

  action FailChecksum = all {
    state == WritingObject or state == Checksumming or state == Replaying,
    state' = Failed,
    sourceRecords' = sourceRecords,
    filteredRecords' = filteredRecords,
    objectRecords' = objectRecords,
    bytesWritten' = bytesWritten,
    checksumComputed' = true,
    checksumValid' = false,
    indexWritten' = false,
    replayedRecords' = replayedRecords,
    privateFieldsWritten' = privateFieldsWritten,
  }

  action step = any {
    ReceiveRecord,
    ApplyPrivacyPolicy,
    WriteObjectRecord,
    ComputeChecksum,
    WriteIndex,
    CommitObject,
    ReplayCommittedObject,
    CompleteReplay,
    FailChecksum,
  }

  val noPrivateFieldsWritten = not(privateFieldsWritten)
  val indexOnlyAfterChecksum = if (indexWritten) checksumComputed and checksumValid else true
  val committedOnlyAfterIndex = if (state == Committed or state == Replaying or state == ReplayComplete) indexWritten else true
  val replayOnlyCommittedObjects = if (state == Replaying or state == ReplayComplete) indexWritten and checksumValid else true
  val replayBoundedByObjectRecords = replayedRecords <= objectRecords
  val objectRecordsWereFiltered = objectRecords <= filteredRecords and filteredRecords <= sourceRecords

  val inv = all {
    noPrivateFieldsWritten,
    indexOnlyAfterChecksum,
    committedOnlyAfterIndex,
    replayOnlyCommittedObjects,
    replayBoundedByObjectRecords,
    objectRecordsWereFiltered,
  }

  run backupCommitRequiresChecksum = all {
    assert(if (state == Committed) checksumComputed and checksumValid and indexWritten else true),
    assert(objectRecords <= filteredRecords),
  }

  run replayUsesOnlyCommittedObjects = all {
    assert(if (state == ReplayComplete) replayedRecords == objectRecords else true),
    assert(if (state == Replaying) indexWritten and checksumValid else true),
  }

  val committed_object_witness = state == Committed
  val replay_complete_witness = state == ReplayComplete
  val checksum_failure_witness = state == Failed and checksumComputed and not(checksumValid)
}
