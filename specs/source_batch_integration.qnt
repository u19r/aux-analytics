// Source-batch integration contract.
// Ingest and durable checkpoint persistence form one commit boundary. Iterator
// checkpoints are filtered from durable saves, a dirty batch performs no
// durable advancement, and a later checkpoint failure invalidates the whole
// batch even if an earlier save was attempted. Attempt count remains visible so
// partial-failure and retry traces cannot be hidden by a result enum. A
// deterministic contract-invalid source record fails closed: the dirty batch is
// rolled back, its source cursor does not advance, and polling remains blocked
// until an explicit operator repair releases the cursor. No retry loop or silent
// discard is part of this contract.
module source_batch_integration {
  type BatchIntegrationResult =
    | NotChecked
    | SuccessfulCommit
    | IngestFailureSkippedCheckpoints
    | IteratorCheckpointFiltered
    | CheckpointFailureStopsCommit
    | EmptyBatchCheckpointCommit
    | MalformedRecordBlocked
    | PoisonReleasedAfterRepair

  var lastResult: BatchIntegrationResult
  var durableCheckpointCount: int
  var durableCheckpointBefore: int
  var recordCount: int
  var ingestErrorCount: int
  var persistableCount: int
  var iteratorCheckpointCount: int
  var attemptedCheckpointCount: int
  var checkpointErrorCount: int
  var lastBatchClean: bool
  var lastCommitted: bool
  var lastIteratorPersisted: bool
  var lastLeasePermitted: bool
  var lastFirstCheckpointFails: bool
  var lastCheckpointSavesSucceeded: bool
  var malformedRecordCount: int
  var recordsApplied: int
  var durableSourceCursor: int
  var durableSourceCursorBefore: int
  var sourceBlocked: bool
  var retryAllowed: bool
  var failureRecorded: bool
  var lastRecordPosition: int

  pure def shouldAttemptCheckpointSaves(ingestErrors: int): bool =
    ingestErrors == 0

  pure def shouldCommit(ingestErrors: int, checkpointErrors: int): bool =
    ingestErrors == 0 and checkpointErrors == 0

  pure def shouldCommitWithLease(ingestErrors: int, checkpointErrors: int, leasePermitted: bool): bool =
    shouldCommit(ingestErrors, checkpointErrors) and leasePermitted

  pure def persistableCheckpointCount(recordCheckpoints: int, iteratorCheckpoints: int): int =
    recordCheckpoints

  pure def checkpointAttempts(ingestErrors: int, recordCheckpoints: int, firstCheckpointFails: bool): int =
    if (not(shouldAttemptCheckpointSaves(ingestErrors))) 0
    else if (recordCheckpoints == 0) 0
    else if (firstCheckpointFails) 1
    else recordCheckpoints

  pure val iteratorCheckpointDurablePolicy: bool = false

  action init = all {
    lastResult' = NotChecked,
    durableCheckpointCount' = 0,
    durableCheckpointBefore' = 0,
    recordCount' = 0,
    ingestErrorCount' = 0,
    persistableCount' = 0,
    iteratorCheckpointCount' = 0,
    attemptedCheckpointCount' = 0,
    checkpointErrorCount' = 0,
    lastBatchClean' = true,
    lastCommitted' = false,
    lastIteratorPersisted' = false,
    lastLeasePermitted' = true,
    lastFirstCheckpointFails' = false,
    lastCheckpointSavesSucceeded' = true,
    malformedRecordCount' = 0,
    recordsApplied' = 0,
    durableSourceCursor' = 0,
    durableSourceCursorBefore' = 0,
    sourceBlocked' = false,
    retryAllowed' = true,
    failureRecorded' = false,
    lastRecordPosition' = 0,
  }

  action CheckAllRecordsAndCheckpointsSucceed = {
    nondet records = Set(1, 2).oneOf()
    nondet recordCheckpoints = Set(1, 2).oneOf()
    nondet iteratorCheckpoints = Set(0, 1).oneOf()
    val persistable = persistableCheckpointCount(recordCheckpoints, iteratorCheckpoints)
    all {
      not(sourceBlocked),
      lastResult' = SuccessfulCommit,
      durableCheckpointCount' = durableCheckpointCount + persistable,
      durableCheckpointBefore' = durableCheckpointCount,
      recordCount' = records,
      ingestErrorCount' = 0,
      persistableCount' = persistable,
      iteratorCheckpointCount' = iteratorCheckpoints,
      attemptedCheckpointCount' = checkpointAttempts(0, persistable, false),
      checkpointErrorCount' = 0,
      lastBatchClean' = true,
      lastCommitted' = true,
      lastIteratorPersisted' = false,
      lastLeasePermitted' = true,
      lastFirstCheckpointFails' = false,
      lastCheckpointSavesSucceeded' = true,
      malformedRecordCount' = 0,
      recordsApplied' = records,
      durableSourceCursor' = durableSourceCursor + records,
      durableSourceCursorBefore' = durableSourceCursor,
      sourceBlocked' = false,
      retryAllowed' = true,
      failureRecorded' = false,
      lastRecordPosition' = durableSourceCursor,
    }
  }

  action CheckIngestFailureSkipsCheckpointSaves = all {
    not(sourceBlocked),
    lastResult' = IngestFailureSkippedCheckpoints,
    durableCheckpointCount' = durableCheckpointCount,
    durableCheckpointBefore' = durableCheckpointCount,
    recordCount' = 2,
    ingestErrorCount' = 1,
    persistableCount' = 1,
    iteratorCheckpointCount' = 1,
    attemptedCheckpointCount' = checkpointAttempts(1, 1, false),
    checkpointErrorCount' = 0,
    lastBatchClean' = false,
    lastCommitted' = false,
    lastIteratorPersisted' = false,
    lastLeasePermitted' = true,
    lastFirstCheckpointFails' = false,
    lastCheckpointSavesSucceeded' = true,
    malformedRecordCount' = 0,
    recordsApplied' = 0,
    durableSourceCursor' = durableSourceCursor,
    durableSourceCursorBefore' = durableSourceCursor,
    sourceBlocked' = false,
    retryAllowed' = true,
    failureRecorded' = false,
    lastRecordPosition' = durableSourceCursor,
  }

  action CheckIteratorCheckpointFiltered = all {
    not(sourceBlocked),
    lastResult' = IteratorCheckpointFiltered,
    durableCheckpointCount' = durableCheckpointCount + 1,
    durableCheckpointBefore' = durableCheckpointCount,
    recordCount' = 1,
    ingestErrorCount' = 0,
    persistableCount' = persistableCheckpointCount(1, 1),
    iteratorCheckpointCount' = 1,
    attemptedCheckpointCount' = checkpointAttempts(0, 1, false),
    checkpointErrorCount' = 0,
    lastBatchClean' = true,
    lastCommitted' = true,
    lastIteratorPersisted' = iteratorCheckpointDurablePolicy,
    lastLeasePermitted' = true,
    lastFirstCheckpointFails' = false,
    lastCheckpointSavesSucceeded' = true,
    malformedRecordCount' = 0,
    recordsApplied' = 1,
    durableSourceCursor' = durableSourceCursor + 1,
    durableSourceCursorBefore' = durableSourceCursor,
    sourceBlocked' = false,
    retryAllowed' = true,
    failureRecorded' = false,
    lastRecordPosition' = durableSourceCursor,
  }

  action CheckCheckpointSaveFailureStopsOrdering = all {
    not(sourceBlocked),
    lastResult' = CheckpointFailureStopsCommit,
    durableCheckpointCount' = durableCheckpointCount,
    durableCheckpointBefore' = durableCheckpointCount,
    recordCount' = 1,
    ingestErrorCount' = 0,
    persistableCount' = 2,
    iteratorCheckpointCount' = 0,
    attemptedCheckpointCount' = checkpointAttempts(0, 2, true),
    checkpointErrorCount' = 1,
    lastBatchClean' = false,
    lastCommitted' = false,
    lastIteratorPersisted' = false,
    lastLeasePermitted' = true,
    lastFirstCheckpointFails' = true,
    lastCheckpointSavesSucceeded' = false,
    malformedRecordCount' = 0,
    recordsApplied' = 0,
    durableSourceCursor' = durableSourceCursor,
    durableSourceCursorBefore' = durableSourceCursor,
    sourceBlocked' = false,
    retryAllowed' = true,
    failureRecorded' = false,
    lastRecordPosition' = durableSourceCursor,
  }

  action CheckEmptyBatchCanPersistCheckpoint = all {
    not(sourceBlocked),
    lastResult' = EmptyBatchCheckpointCommit,
    durableCheckpointCount' = durableCheckpointCount + 1,
    durableCheckpointBefore' = durableCheckpointCount,
    recordCount' = 0,
    ingestErrorCount' = 0,
    persistableCount' = 1,
    iteratorCheckpointCount' = 0,
    attemptedCheckpointCount' = checkpointAttempts(0, 1, false),
    checkpointErrorCount' = 0,
    lastBatchClean' = true,
    lastCommitted' = true,
    lastIteratorPersisted' = false,
    lastLeasePermitted' = true,
    lastFirstCheckpointFails' = false,
    lastCheckpointSavesSucceeded' = true,
    malformedRecordCount' = 0,
    recordsApplied' = 0,
    durableSourceCursor' = durableSourceCursor,
    durableSourceCursorBefore' = durableSourceCursor,
    sourceBlocked' = false,
    retryAllowed' = true,
    failureRecorded' = false,
    lastRecordPosition' = durableSourceCursor,
  }

  action CheckMalformedRecordFailsClosed = all {
    not(sourceBlocked),
    lastResult' = MalformedRecordBlocked,
    durableCheckpointCount' = durableCheckpointCount,
    durableCheckpointBefore' = durableCheckpointCount,
    recordCount' = 3,
    ingestErrorCount' = 1,
    persistableCount' = 2,
    iteratorCheckpointCount' = 1,
    attemptedCheckpointCount' = checkpointAttempts(1, 2, false),
    checkpointErrorCount' = 0,
    lastBatchClean' = false,
    lastCommitted' = false,
    lastIteratorPersisted' = false,
    lastLeasePermitted' = true,
    lastFirstCheckpointFails' = false,
    lastCheckpointSavesSucceeded' = true,
    malformedRecordCount' = 1,
    recordsApplied' = 0,
    durableSourceCursor' = durableSourceCursor,
    durableSourceCursorBefore' = durableSourceCursor,
    sourceBlocked' = true,
    retryAllowed' = false,
    failureRecorded' = true,
    lastRecordPosition' = durableSourceCursor,
  }

  action OperatorRepairsPoison = {
    val repairAccepted = sourceBlocked
    all {
      sourceBlocked,
      lastResult' = if (repairAccepted) PoisonReleasedAfterRepair else lastResult,
      durableCheckpointCount' = durableCheckpointCount,
      durableCheckpointBefore' = durableCheckpointCount,
      recordCount' = if (repairAccepted) 0 else recordCount,
      ingestErrorCount' = if (repairAccepted) 0 else ingestErrorCount,
      persistableCount' = if (repairAccepted) 0 else persistableCount,
      iteratorCheckpointCount' = if (repairAccepted) 0 else iteratorCheckpointCount,
      attemptedCheckpointCount' = if (repairAccepted) 0 else attemptedCheckpointCount,
      checkpointErrorCount' = if (repairAccepted) 0 else checkpointErrorCount,
      lastBatchClean' = if (repairAccepted) true else lastBatchClean,
      lastCommitted' = false,
      lastIteratorPersisted' = false,
      lastLeasePermitted' = true,
      lastFirstCheckpointFails' = false,
      lastCheckpointSavesSucceeded' = true,
      malformedRecordCount' = if (repairAccepted) 0 else malformedRecordCount,
      recordsApplied' = 0,
      durableSourceCursor' = durableSourceCursor,
      durableSourceCursorBefore' = durableSourceCursor,
      sourceBlocked' = if (repairAccepted) false else sourceBlocked,
      retryAllowed' = if (repairAccepted) true else retryAllowed,
      failureRecorded' = if (repairAccepted) false else failureRecorded,
      lastRecordPosition' = durableSourceCursor,
    }
  }

  action step = any {
    CheckAllRecordsAndCheckpointsSucceed,
    CheckIngestFailureSkipsCheckpointSaves,
    CheckIteratorCheckpointFiltered,
    CheckCheckpointSaveFailureStopsOrdering,
    CheckEmptyBatchCanPersistCheckpoint,
    CheckMalformedRecordFailsClosed,
    OperatorRepairsPoison,
  }

  /* Properties */

  val countsAreNonnegative = and {
    durableCheckpointCount >= 0,
    durableCheckpointBefore >= 0,
    recordCount >= 0,
    ingestErrorCount >= 0,
    persistableCount >= 0,
    iteratorCheckpointCount >= 0,
    attemptedCheckpointCount >= 0,
    checkpointErrorCount >= 0,
    malformedRecordCount >= 0,
    recordsApplied >= 0,
    durableSourceCursor >= 0,
    durableSourceCursorBefore >= 0,
    lastRecordPosition >= 0,
  }
  val cleanMatchesErrors =
    lastBatchClean == (ingestErrorCount == 0 and checkpointErrorCount == 0)
  val dirtyBatchPreservesDurability =
    lastCommitted or durableCheckpointCount == durableCheckpointBefore
  val commitMatchesDurableAdvance =
    lastCommitted implies and {
      lastBatchClean,
      lastCheckpointSavesSucceeded,
      lastLeasePermitted,
      durableCheckpointCount == durableCheckpointBefore + persistableCount,
    }
  val iteratorCheckpointsNeverPersisted = not(lastIteratorPersisted)
  val attemptsRespectIngestGate =
    ingestErrorCount > 0 implies attemptedCheckpointCount == 0
  val failedCheckpointPreservesDurability =
    checkpointErrorCount > 0 implies durableCheckpointCount == durableCheckpointBefore
  val sourceCursorAdvancesOnlyWithCleanCommit =
    durableSourceCursor > durableSourceCursorBefore implies and {
      lastCommitted,
      lastBatchClean,
      recordsApplied > 0,
    }
  val sourceCursorAdvanceMatchesAppliedRecords =
    durableSourceCursor == durableSourceCursorBefore + recordsApplied
  val sourceCursorNeverMovesBackwards = durableSourceCursor >= durableSourceCursorBefore
  val malformedRecordFailsClosed =
    lastResult == MalformedRecordBlocked implies and {
      malformedRecordCount == 1,
      ingestErrorCount > 0,
      not(lastBatchClean),
      not(lastCommitted),
      recordsApplied == 0,
      durableCheckpointCount == durableCheckpointBefore,
      durableSourceCursor == durableSourceCursorBefore,
      lastRecordPosition == durableSourceCursorBefore,
      sourceBlocked,
      not(retryAllowed),
      failureRecorded,
    }
  val blockedSourceCannotRetryOrAdvance = sourceBlocked implies and {
    not(retryAllowed),
    not(lastCommitted),
    durableSourceCursor == durableSourceCursorBefore,
    durableCheckpointCount == durableCheckpointBefore,
    failureRecorded,
  }
  val poisonReleaseRequiresRepair =
    lastResult == PoisonReleasedAfterRepair implies and {
      not(sourceBlocked),
      retryAllowed,
      not(failureRecorded),
      lastBatchClean,
      ingestErrorCount == 0,
      malformedRecordCount == 0,
    }
  val inv = and {
    countsAreNonnegative,
    cleanMatchesErrors,
    dirtyBatchPreservesDurability,
    commitMatchesDurableAdvance,
    iteratorCheckpointsNeverPersisted,
    attemptsRespectIngestGate,
    failedCheckpointPreservesDurability,
    sourceCursorAdvancesOnlyWithCleanCommit,
    sourceCursorAdvanceMatchesAppliedRecords,
    sourceCursorNeverMovesBackwards,
    malformedRecordFailsClosed,
    blockedSourceCannotRetryOrAdvance,
    poisonReleaseRequiresRepair,
  }

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
      assert(shouldCommitWithLease(0, 0, false) == false),
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
  val source_batch_filtering_witness = lastResult == IteratorCheckpointFiltered and iteratorCheckpointCount > 0
  val dirty_batch_recovery_witness = not(lastBatchClean) and durableCheckpointCount == durableCheckpointBefore
  val malformed_record_blocked_witness =
    lastResult == MalformedRecordBlocked and sourceBlocked and failureRecorded and
      durableSourceCursor == durableSourceCursorBefore
  val poison_release_witness =
    lastResult == PoisonReleasedAfterRepair and not(sourceBlocked) and retryAllowed
}
