module source_batch_integration {
  type BatchIntegrationResult =
    | NotChecked
    | SuccessfulCommit
    | IngestFailureSkippedCheckpoints
    | IteratorCheckpointFiltered
    | CheckpointFailureStopsCommit
    | EmptyBatchCheckpointCommit

  var lastResult: BatchIntegrationResult

  pure val batchIntegrationResults: Set[BatchIntegrationResult] = Set(
    NotChecked,
    SuccessfulCommit,
    IngestFailureSkippedCheckpoints,
    IteratorCheckpointFiltered,
    CheckpointFailureStopsCommit,
    EmptyBatchCheckpointCommit
  )

  pure def shouldAttemptCheckpointSaves(ingestErrors: int): bool =
    ingestErrors == 0

  pure def shouldCommit(ingestErrors: int, checkpointErrors: int): bool =
    ingestErrors == 0 and checkpointErrors == 0

  pure def persistableCheckpointCount(recordCheckpoints: int, iteratorCheckpoints: int): int =
    recordCheckpoints

  pure def checkpointAttempts(ingestErrors: int, recordCheckpoints: int, firstCheckpointFails: bool): int =
    if (not(shouldAttemptCheckpointSaves(ingestErrors))) 0
    else if (recordCheckpoints == 0) 0
    else if (firstCheckpointFails) 1
    else recordCheckpoints

  action init =
    lastResult' = NotChecked

  action CheckAllRecordsAndCheckpointsSucceed =
    lastResult' = SuccessfulCommit

  action CheckIngestFailureSkipsCheckpointSaves =
    lastResult' = IngestFailureSkippedCheckpoints

  action CheckIteratorCheckpointFiltered =
    lastResult' = IteratorCheckpointFiltered

  action CheckCheckpointSaveFailureStopsOrdering =
    lastResult' = CheckpointFailureStopsCommit

  action CheckEmptyBatchCanPersistCheckpoint =
    lastResult' = EmptyBatchCheckpointCommit

  action step = any {
    CheckAllRecordsAndCheckpointsSucceed,
    CheckIngestFailureSkipsCheckpointSaves,
    CheckIteratorCheckpointFiltered,
    CheckCheckpointSaveFailureStopsOrdering,
    CheckEmptyBatchCanPersistCheckpoint,
  }

  /* Properties */

  val resultIsKnown = batchIntegrationResults.contains(lastResult)
  val inv = resultIsKnown

  /* Example Runs */

  run checkpointSaveOrderingTest =
    all {
      assert(checkpointAttempts(0, 2, false) == 2),
      assert(checkpointAttempts(1, 2, false) == 0),
      assert(checkpointAttempts(0, 2, true) == 1),
    }

  run commitGateTest =
    all {
      assert(shouldCommit(0, 0)),
      assert(shouldCommit(1, 0) == false),
      assert(shouldCommit(0, 1) == false),
    }

  run iteratorCheckpointFilteringTest =
    all {
      assert(persistableCheckpointCount(1, 1) == 1),
      assert(persistableCheckpointCount(0, 1) == 0),
    }

  /* Witnesses */

  val source_batch_success_witness = lastResult == SuccessfulCommit
  val source_batch_failure_witness = or {
    lastResult == IngestFailureSkippedCheckpoints,
    lastResult == CheckpointFailureStopsCommit,
  }
  val source_batch_filtering_witness = lastResult == IteratorCheckpointFiltered
}
