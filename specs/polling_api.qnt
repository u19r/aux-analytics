// Stateful polling API contract.
// The model separates durable source checkpoints from transient batch and health
// observations. A checkpoint can advance only for a clean, lease-authorized
// batch whose checkpoint phase succeeded; a failed batch preserves the durable
// boundary. Startup and poll errors are represented explicitly so a healthy
// observation cannot be inferred from an enum value alone.
module polling_api {
  type ApiPollingResult =
    | NotChecked
    | PersistableCheckpointsFiltered
    | SuccessfulBatchCommits
    | IngestFailureBlocksCommit
    | CheckpointFailureBlocksCommit
    | SuccessHealthUpdated
    | PartialFailureHealthDegraded
    | PollErrorHealthDegraded
    | StartupDisabled
    | StartupStarting
    | StartupCheckpointsLoaded

  type HealthState = Healthy | Degraded | Disabled | Starting
  type PollingPhase = Idle | StartingPhase | Polling | HealthyPhase | DegradedPhase | DisabledPhase

  var lastResult: ApiPollingResult
  var health: HealthState
  var phase: PollingPhase
  var durableCheckpointCount: int
  var durableCheckpointBefore: int
  var lastPersistableCheckpointCount: int
  var lastBatchClean: bool
  var lastCheckpointSaveSucceeded: bool
  var lastLeasePermitted: bool
  var lastCommitted: bool
  var lastCurrentBatchHasErrors: bool
  var lastHealthDurableCheckpointCount: int
  var processorEnabled: bool
  var configuredTableCount: int
  var pollerIsEmpty: bool
  var loadedCheckpointCount: int

  pure def shouldCommit(ingestErrors: int, checkpointErrors: int): bool =
    ingestErrors == 0 and checkpointErrors == 0

  pure def shouldCommitWithLease(ingestErrors: int, checkpointErrors: int, leasePermitted: bool): bool =
    shouldCommit(ingestErrors, checkpointErrors) and leasePermitted

  pure def savedCheckpointAttempts(allRecordsIngested: bool, persistableCount: int): int =
    if (allRecordsIngested) persistableCount else 0

  pure def persistableCheckpointCount(recordCheckpoints: int, iteratorCheckpoints: int): int =
    recordCheckpoints

  pure def sourcePollingStartup(configuredCount: int, pollerEmpty: bool): HealthState =
    if (configuredCount == 0 or pollerEmpty) Disabled else Starting

  pure def successHealth(allRecordsIngested: bool): HealthState =
    if (allRecordsIngested) Healthy else Degraded

  pure val pollErrorHealth: HealthState = Degraded

  pure def apiPollingCase(result: ApiPollingResult): ApiPollingResult =
    result

  action init = all {
    lastResult' = NotChecked,
    health' = Starting,
    phase' = Idle,
    durableCheckpointCount' = 0,
    durableCheckpointBefore' = 0,
    lastPersistableCheckpointCount' = 0,
    lastBatchClean' = true,
    lastCheckpointSaveSucceeded' = true,
    lastLeasePermitted' = true,
    lastCommitted' = false,
    lastCurrentBatchHasErrors' = false,
    lastHealthDurableCheckpointCount' = 0,
    processorEnabled' = false,
    configuredTableCount' = 0,
    pollerIsEmpty' = true,
    loadedCheckpointCount' = 0,
  }

  action CheckPersistableCheckpointsFiltered = {
    nondet leasePermitted = Set(true, false).oneOf()
    all {
      lastResult' = apiPollingCase(PersistableCheckpointsFiltered),
      health' = health,
      phase' = Polling,
      durableCheckpointCount' = durableCheckpointCount,
      durableCheckpointBefore' = durableCheckpointCount,
      lastPersistableCheckpointCount' = 0,
      lastBatchClean' = true,
      lastCheckpointSaveSucceeded' = true,
      lastLeasePermitted' = leasePermitted,
      lastCommitted' = false,
      lastCurrentBatchHasErrors' = false,
      lastHealthDurableCheckpointCount' = lastHealthDurableCheckpointCount,
      processorEnabled' = processorEnabled,
      configuredTableCount' = configuredTableCount,
      pollerIsEmpty' = pollerIsEmpty,
      loadedCheckpointCount' = loadedCheckpointCount,
    }
  }

  action CheckSuccessfulBatchCommits = all {
    lastResult' = apiPollingCase(SuccessfulBatchCommits),
    health' = Healthy,
    phase' = HealthyPhase,
    durableCheckpointCount' = durableCheckpointCount + 2,
    durableCheckpointBefore' = durableCheckpointCount,
    lastPersistableCheckpointCount' = 2,
    lastBatchClean' = true,
    lastCheckpointSaveSucceeded' = true,
    lastLeasePermitted' = true,
    lastCommitted' = true,
    lastCurrentBatchHasErrors' = false,
    lastHealthDurableCheckpointCount' = durableCheckpointCount + 2,
    processorEnabled' = true,
    configuredTableCount' = 2,
    pollerIsEmpty' = false,
    loadedCheckpointCount' = loadedCheckpointCount,
  }

  action CheckIngestFailureBlocksCommit = all {
    lastResult' = apiPollingCase(IngestFailureBlocksCommit),
    health' = Degraded,
    phase' = DegradedPhase,
    durableCheckpointCount' = durableCheckpointCount,
    durableCheckpointBefore' = durableCheckpointCount,
    lastPersistableCheckpointCount' = 2,
    lastBatchClean' = false,
    lastCheckpointSaveSucceeded' = false,
    lastLeasePermitted' = true,
    lastCommitted' = false,
    lastCurrentBatchHasErrors' = true,
    lastHealthDurableCheckpointCount' = durableCheckpointCount,
    processorEnabled' = true,
    configuredTableCount' = 2,
    pollerIsEmpty' = false,
    loadedCheckpointCount' = loadedCheckpointCount,
  }

  action CheckCheckpointFailureBlocksCommit = all {
    lastResult' = apiPollingCase(CheckpointFailureBlocksCommit),
    health' = Degraded,
    phase' = DegradedPhase,
    durableCheckpointCount' = durableCheckpointCount,
    durableCheckpointBefore' = durableCheckpointCount,
    lastPersistableCheckpointCount' = 2,
    lastBatchClean' = false,
    lastCheckpointSaveSucceeded' = false,
    lastLeasePermitted' = true,
    lastCommitted' = false,
    lastCurrentBatchHasErrors' = true,
    lastHealthDurableCheckpointCount' = durableCheckpointCount,
    processorEnabled' = true,
    configuredTableCount' = 2,
    pollerIsEmpty' = false,
    loadedCheckpointCount' = loadedCheckpointCount,
  }

  action CheckSuccessHealth = all {
    lastResult' = apiPollingCase(SuccessHealthUpdated),
    health' = Healthy,
    phase' = HealthyPhase,
    durableCheckpointCount' = durableCheckpointCount,
    durableCheckpointBefore' = durableCheckpointCount,
    lastPersistableCheckpointCount' = 0,
    lastBatchClean' = true,
    lastCheckpointSaveSucceeded' = true,
    lastLeasePermitted' = true,
    lastCommitted' = true,
    lastCurrentBatchHasErrors' = false,
    lastHealthDurableCheckpointCount' = durableCheckpointCount,
    processorEnabled' = true,
    configuredTableCount' = 1,
    pollerIsEmpty' = false,
    loadedCheckpointCount' = loadedCheckpointCount,
  }

  action CheckPartialFailureHealth = all {
    lastResult' = apiPollingCase(PartialFailureHealthDegraded),
    health' = Degraded,
    phase' = DegradedPhase,
    durableCheckpointCount' = durableCheckpointCount,
    durableCheckpointBefore' = durableCheckpointCount,
    lastPersistableCheckpointCount' = 1,
    lastBatchClean' = false,
    lastCheckpointSaveSucceeded' = false,
    lastLeasePermitted' = true,
    lastCommitted' = false,
    lastCurrentBatchHasErrors' = true,
    lastHealthDurableCheckpointCount' = durableCheckpointCount,
    processorEnabled' = true,
    configuredTableCount' = 1,
    pollerIsEmpty' = false,
    loadedCheckpointCount' = loadedCheckpointCount,
  }

  action CheckPollErrorHealth = all {
    lastResult' = apiPollingCase(PollErrorHealthDegraded),
    health' = Degraded,
    phase' = DegradedPhase,
    durableCheckpointCount' = durableCheckpointCount,
    durableCheckpointBefore' = durableCheckpointCount,
    lastPersistableCheckpointCount' = 0,
    lastBatchClean' = false,
    lastCheckpointSaveSucceeded' = false,
    lastLeasePermitted' = true,
    lastCommitted' = false,
    lastCurrentBatchHasErrors' = true,
    lastHealthDurableCheckpointCount' = durableCheckpointCount,
    processorEnabled' = processorEnabled,
    configuredTableCount' = configuredTableCount,
    pollerIsEmpty' = pollerIsEmpty,
    loadedCheckpointCount' = loadedCheckpointCount,
  }

  action CheckStartupDisabled = all {
    lastResult' = apiPollingCase(StartupDisabled),
    health' = Disabled,
    phase' = DisabledPhase,
    durableCheckpointCount' = durableCheckpointCount,
    durableCheckpointBefore' = durableCheckpointCount,
    lastPersistableCheckpointCount' = 0,
    lastBatchClean' = true,
    lastCheckpointSaveSucceeded' = true,
    lastLeasePermitted' = false,
    lastCommitted' = false,
    lastCurrentBatchHasErrors' = false,
    lastHealthDurableCheckpointCount' = durableCheckpointCount,
    processorEnabled' = false,
    configuredTableCount' = 0,
    pollerIsEmpty' = true,
    loadedCheckpointCount' = 0,
  }

  action CheckStartupStarting = all {
    lastResult' = apiPollingCase(StartupStarting),
    health' = Starting,
    phase' = StartingPhase,
    durableCheckpointCount' = durableCheckpointCount,
    durableCheckpointBefore' = durableCheckpointCount,
    lastPersistableCheckpointCount' = 0,
    lastBatchClean' = true,
    lastCheckpointSaveSucceeded' = true,
    lastLeasePermitted' = false,
    lastCommitted' = false,
    lastCurrentBatchHasErrors' = false,
    lastHealthDurableCheckpointCount' = durableCheckpointCount,
    processorEnabled' = true,
    configuredTableCount' = 1,
    pollerIsEmpty' = false,
    loadedCheckpointCount' = 0,
  }

  action CheckStartupCheckpointMapping = all {
    lastResult' = apiPollingCase(StartupCheckpointsLoaded),
    health' = Starting,
    phase' = Polling,
    durableCheckpointCount' = durableCheckpointCount,
    durableCheckpointBefore' = durableCheckpointCount,
    lastPersistableCheckpointCount' = 0,
    lastBatchClean' = true,
    lastCheckpointSaveSucceeded' = true,
    lastLeasePermitted' = false,
    lastCommitted' = false,
    lastCurrentBatchHasErrors' = false,
    lastHealthDurableCheckpointCount' = durableCheckpointCount,
    processorEnabled' = true,
    configuredTableCount' = 1,
    pollerIsEmpty' = false,
    loadedCheckpointCount' = 2,
  }

  action step = any {
    CheckPersistableCheckpointsFiltered,
    CheckSuccessfulBatchCommits,
    CheckIngestFailureBlocksCommit,
    CheckCheckpointFailureBlocksCommit,
    CheckSuccessHealth,
    CheckPartialFailureHealth,
    CheckPollErrorHealth,
    CheckStartupDisabled,
    CheckStartupStarting,
    CheckStartupCheckpointMapping,
  }

  /* Properties */

  val durableCheckpointNeverRegresses = durableCheckpointCount >= durableCheckpointBefore
  val failedBatchPreservesDurability =
    lastCommitted or durableCheckpointCount == durableCheckpointBefore
  val committedBatchWasClean =
    lastCommitted implies and {
      lastBatchClean,
      lastCheckpointSaveSucceeded,
      lastLeasePermitted,
      durableCheckpointCount == durableCheckpointBefore + lastPersistableCheckpointCount,
    }
  val healthyObservationMatchesCommit =
    health == Healthy and lastResult != PersistableCheckpointsFiltered implies and {
      lastCommitted,
      not(lastCurrentBatchHasErrors),
      lastHealthDurableCheckpointCount == durableCheckpointCount,
    }
  val filteredCheckpointObservationCannotCommit =
    lastResult == PersistableCheckpointsFiltered implies and {
      lastPersistableCheckpointCount == 0,
      durableCheckpointCount == durableCheckpointBefore,
      not(lastCommitted),
    }
  val disabledPollingCannotCommit = health == Disabled implies not(lastCommitted)
  val inv = and {
    durableCheckpointNeverRegresses,
    failedBatchPreservesDurability,
    committedBatchWasClean,
    healthyObservationMatchesCommit,
    filteredCheckpointObservationCannotCommit,
    disabledPollingCannotCommit,
  }

  /* Example Runs */

  run durableSaveOrderingTest =
    all {
      assert(savedCheckpointAttempts(true, 2) == 2),
      assert(savedCheckpointAttempts(false, 2) == 0),
      assert(shouldCommit(0, 0)),
      assert(shouldCommit(1, 0) == false),
      assert(shouldCommit(0, 1) == false),
      assert(shouldCommitWithLease(0, 0, true)),
      assert(shouldCommitWithLease(0, 0, false) == false),
    }

  run persistableCheckpointsTest =
    all {
      assert(persistableCheckpointCount(1, 1) == 1),
      assert(persistableCheckpointCount(0, 1) == 0),
    }

  run sourceHealthStateTest =
    all {
      assert(sourcePollingStartup(0, false) == Disabled),
      assert(sourcePollingStartup(2, true) == Disabled),
      assert(sourcePollingStartup(2, false) == Starting),
      assert(successHealth(true) == Healthy),
      assert(successHealth(false) == Degraded),
      assert(pollErrorHealth == Degraded),
    }

  run startupCheckpointMappingTest =
    all {
      assert(apiPollingCase(StartupCheckpointsLoaded) == StartupCheckpointsLoaded),
    }

  /* Witnesses */

  val durable_ordering_witness = or {
    lastResult == SuccessfulBatchCommits,
    lastResult == IngestFailureBlocksCommit,
    lastResult == CheckpointFailureBlocksCommit,
  }
  val persistable_checkpoint_witness =
    lastResult == PersistableCheckpointsFiltered and not(lastCommitted)
  val lease_denied_filtered_witness =
    lastResult == PersistableCheckpointsFiltered and not(lastLeasePermitted) and
      not(lastCommitted)
  val health_state_witness = or {
    health == Healthy,
    health == Degraded,
    health == Disabled,
    health == Starting,
  }
  val startup_checkpoint_witness = lastResult == StartupCheckpointsLoaded and loadedCheckpointCount == 2
}
