module polling_storage {
  type Cursor = CursorValue(str) | NoCursor

  type StoragePollingResult =
    | NotChecked
    | FanOutComplete
    | AuxResumeKeyWins
    | AuxRecordCheckpointed
    | AuxTerminalStops
    | AwsIteratorAdvanced
    | AwsRecordAndIteratorCheckpointsEmitted
    | AwsIteratorOnlyCheckpointEmitted

  var lastResult: StoragePollingResult

  pure val storagePollingResults: Set[StoragePollingResult] = Set(
    NotChecked,
    FanOutComplete,
    AuxResumeKeyWins,
    AuxRecordCheckpointed,
    AuxTerminalStops,
    AwsIteratorAdvanced,
    AwsRecordAndIteratorCheckpointsEmitted,
    AwsIteratorOnlyCheckpointEmitted
  )

  pure def fanOutRecordCount(recordCount: int, analyticsTableCount: int): int =
    recordCount * analyticsTableCount

  pure def nextAuxCursor(resumeKey: Cursor, latestRecordKey: Cursor): Cursor =
    match resumeKey {
      | CursorValue(cursor) => CursorValue(cursor)
      | NoCursor => latestRecordKey
    }

  pure def awsCheckpointCount(recordCount: int, nextIterator: Cursor): int =
    match nextIterator {
      | CursorValue(_) => if (recordCount > 0) 2 else 1
      | NoCursor => if (recordCount > 0) 1 else 0
    }

  pure def storagePollingCase(result: StoragePollingResult): StoragePollingResult =
    result

  action init =
    lastResult' = NotChecked

  action CheckFanOut =
    lastResult' = storagePollingCase(FanOutComplete)

  action CheckAuxResumeKeyWins =
    lastResult' = storagePollingCase(AuxResumeKeyWins)

  action CheckAuxRecordCheckpoint =
    lastResult' = storagePollingCase(AuxRecordCheckpointed)

  action CheckAuxTerminalStops =
    lastResult' = storagePollingCase(AuxTerminalStops)

  action CheckAwsIteratorAdvances =
    lastResult' = storagePollingCase(AwsIteratorAdvanced)

  action CheckAwsBatchCheckpoints =
    lastResult' = storagePollingCase(AwsRecordAndIteratorCheckpointsEmitted)

  action CheckAwsIteratorOnlyCheckpoint =
    lastResult' = storagePollingCase(AwsIteratorOnlyCheckpointEmitted)

  action step = any {
    CheckFanOut,
    CheckAuxResumeKeyWins,
    CheckAuxRecordCheckpoint,
    CheckAuxTerminalStops,
    CheckAwsIteratorAdvances,
    CheckAwsBatchCheckpoints,
    CheckAwsIteratorOnlyCheckpoint,
  }

  /* Properties */

  val resultIsKnown = storagePollingResults.contains(lastResult)
  val inv = resultIsKnown

  /* Example Runs */

  run fanOutTest =
    all {
      assert(fanOutRecordCount(1, 2) == 2),
      assert(fanOutRecordCount(2, 3) == 6),
    }

  run auxCheckpointAdvancementTest =
    all {
      assert(nextAuxCursor(CursorValue("next-page"), CursorValue("seq-2")) == CursorValue("next-page")),
      assert(nextAuxCursor(NoCursor, CursorValue("seq-2")) == CursorValue("seq-2")),
      assert(nextAuxCursor(NoCursor, NoCursor) == NoCursor),
    }

  run awsCheckpointAdvancementTest =
    all {
      assert(awsCheckpointCount(2, CursorValue("next-iterator")) == 2),
      assert(awsCheckpointCount(0, CursorValue("next-iterator")) == 1),
      assert(awsCheckpointCount(2, NoCursor) == 1),
      assert(awsCheckpointCount(0, NoCursor) == 0),
    }

  /* Witnesses */

  val fan_out_witness = lastResult == FanOutComplete
  val aux_checkpoint_witness = or {
    lastResult == AuxResumeKeyWins,
    lastResult == AuxRecordCheckpointed,
    lastResult == AuxTerminalStops,
  }
  val aws_checkpoint_witness = or {
    lastResult == AwsIteratorAdvanced,
    lastResult == AwsRecordAndIteratorCheckpointsEmitted,
    lastResult == AwsIteratorOnlyCheckpointEmitted,
  }
}
