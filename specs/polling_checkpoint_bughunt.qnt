module polling_checkpoint_bughunt {
  type HealthState = Starting | Healthy | Degraded | Disabled

  var lastIteratorCommit: bool
  var lastBatchClean: bool
  var durableIteratorCheckpointSaved: bool
  var currentBatchHasErrors: bool
  var health: HealthState

  pure val bools: Set[bool] = Set(true, false)
  pure val recordCounts: Set[int] = Set(0, 1, 2)

  pure def ingestErrors(recordCount: int, ingestOk: bool): int =
    if (recordCount > 0 and not(ingestOk)) 1 else 0

  pure def checkpointErrors(recordCount: int, ingestOk: bool, checkpointOk: bool): int =
    if (recordCount > 0 and ingestOk and not(checkpointOk)) 1 else 0

  pure def batchIsClean(recordCount: int, ingestOk: bool, checkpointOk: bool): bool =
    ingestErrors(recordCount, ingestOk) == 0 and checkpointErrors(recordCount, ingestOk, checkpointOk) == 0

  pure def shouldCommitIterator(recordCount: int, ingestOk: bool, checkpointOk: bool, hasIteratorCheckpoint: bool): bool =
    hasIteratorCheckpoint and batchIsClean(recordCount, ingestOk, checkpointOk)

  pure def shouldSaveDurableIteratorCheckpoint(hasIteratorCheckpoint: bool): bool =
    false

  pure def healthAfterBatch(clean: bool): HealthState =
    if (clean) Healthy else Degraded

  action init = all {
    lastIteratorCommit' = false,
    lastBatchClean' = true,
    durableIteratorCheckpointSaved' = false,
    currentBatchHasErrors' = false,
    health' = Starting,
  }

  action ProcessPolledBatch = {
    nondet recordCount = recordCounts.oneOf()
    nondet ingestOk = bools.oneOf()
    nondet checkpointOk = bools.oneOf()
    nondet hasIteratorCheckpoint = bools.oneOf()
    all {
      lastIteratorCommit' = shouldCommitIterator(recordCount, ingestOk, checkpointOk, hasIteratorCheckpoint),
      lastBatchClean' = batchIsClean(recordCount, ingestOk, checkpointOk),
      durableIteratorCheckpointSaved' = durableIteratorCheckpointSaved or shouldSaveDurableIteratorCheckpoint(hasIteratorCheckpoint),
      currentBatchHasErrors' = not(batchIsClean(recordCount, ingestOk, checkpointOk)),
      health' = healthAfterBatch(batchIsClean(recordCount, ingestOk, checkpointOk)),
    }
  }

  action PollError = all {
    lastIteratorCommit' = false,
    lastBatchClean' = false,
    durableIteratorCheckpointSaved' = durableIteratorCheckpointSaved,
    currentBatchHasErrors' = true,
    health' = Degraded,
  }

  action DisablePolling = all {
    lastIteratorCommit' = false,
    lastBatchClean' = true,
    durableIteratorCheckpointSaved' = durableIteratorCheckpointSaved,
    currentBatchHasErrors' = false,
    health' = Disabled,
  }

  action step = any {
    ProcessPolledBatch,
    PollError,
    DisablePolling,
  }

  /* Properties */

  val iteratorCommitRequiresCleanBatch =
    lastIteratorCommit implies lastBatchClean

  val durableCheckpointsExcludeIteratorPositions =
    not(durableIteratorCheckpointSaved)

  val healthyStateHasNoCurrentBatchErrors =
    health == Healthy implies not(currentBatchHasErrors)

  val inv = and {
    iteratorCommitRequiresCleanBatch,
    durableCheckpointsExcludeIteratorPositions,
    healthyStateHasNoCurrentBatchErrors,
  }

  /* Example Runs */

  run batchCommitDecisionTest =
    all {
      assert(shouldCommitIterator(2, true, true, true)),
      assert(shouldCommitIterator(2, false, true, true) == false),
      assert(shouldCommitIterator(2, true, false, true) == false),
      assert(shouldCommitIterator(0, true, true, true)),
      assert(shouldCommitIterator(2, true, true, false) == false),
    }

  run durableCheckpointFilteringTest =
    all {
      assert(shouldSaveDurableIteratorCheckpoint(true) == false),
      assert(shouldSaveDurableIteratorCheckpoint(false) == false),
    }

  run healthErrorStateTest =
    all {
      assert(healthAfterBatch(true) == Healthy),
      assert(healthAfterBatch(false) == Degraded),
    }

  /* Witnesses */

  val iterator_commit_witness = lastIteratorCommit
  val failed_batch_witness = currentBatchHasErrors
  val degraded_health_witness = health == Degraded
}
