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

  var lastResult: ApiPollingResult

  pure val apiPollingResults: Set[ApiPollingResult] = Set(
    NotChecked,
    PersistableCheckpointsFiltered,
    SuccessfulBatchCommits,
    IngestFailureBlocksCommit,
    CheckpointFailureBlocksCommit,
    SuccessHealthUpdated,
    PartialFailureHealthDegraded,
    PollErrorHealthDegraded,
    StartupDisabled,
    StartupStarting,
    StartupCheckpointsLoaded
  )

  pure def shouldCommit(ingestErrors: int, checkpointErrors: int): bool =
    ingestErrors == 0 and checkpointErrors == 0

  pure def savedCheckpointAttempts(allRecordsIngested: bool, persistableCheckpointCount: int): int =
    if (allRecordsIngested) persistableCheckpointCount else 0

  pure def persistableCheckpointCount(recordCheckpoints: int, iteratorCheckpoints: int): int =
    recordCheckpoints

  pure def sourcePollingStartup(configuredTableCount: int, pollerIsEmpty: bool): HealthState =
    if (configuredTableCount == 0 or pollerIsEmpty) Disabled else Starting

  pure def successHealth(allRecordsIngested: bool): HealthState =
    if (allRecordsIngested) Healthy else Degraded

  pure val pollErrorHealth: HealthState =
    Degraded

  pure def apiPollingCase(result: ApiPollingResult): ApiPollingResult =
    result

  action init =
    lastResult' = NotChecked

  action CheckPersistableCheckpointsFiltered =
    lastResult' = apiPollingCase(PersistableCheckpointsFiltered)

  action CheckSuccessfulBatchCommits =
    lastResult' = apiPollingCase(SuccessfulBatchCommits)

  action CheckIngestFailureBlocksCommit =
    lastResult' = apiPollingCase(IngestFailureBlocksCommit)

  action CheckCheckpointFailureBlocksCommit =
    lastResult' = apiPollingCase(CheckpointFailureBlocksCommit)

  action CheckSuccessHealth =
    lastResult' = apiPollingCase(SuccessHealthUpdated)

  action CheckPartialFailureHealth =
    lastResult' = apiPollingCase(PartialFailureHealthDegraded)

  action CheckPollErrorHealth =
    lastResult' = apiPollingCase(PollErrorHealthDegraded)

  action CheckStartupDisabled =
    lastResult' = apiPollingCase(StartupDisabled)

  action CheckStartupStarting =
    lastResult' = apiPollingCase(StartupStarting)

  action CheckStartupCheckpointMapping =
    lastResult' = apiPollingCase(StartupCheckpointsLoaded)

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

  val resultIsKnown = apiPollingResults.contains(lastResult)
  val inv = resultIsKnown

  /* Example Runs */

  run durableSaveOrderingTest =
    all {
      assert(savedCheckpointAttempts(true, 2) == 2),
      assert(savedCheckpointAttempts(false, 2) == 0),
      assert(shouldCommit(0, 0)),
      assert(shouldCommit(1, 0) == false),
      assert(shouldCommit(0, 1) == false),
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
  val persistable_checkpoint_witness = lastResult == PersistableCheckpointsFiltered
  val health_state_witness = or {
    lastResult == SuccessHealthUpdated,
    lastResult == PartialFailureHealthDegraded,
    lastResult == PollErrorHealthDegraded,
    lastResult == StartupDisabled,
    lastResult == StartupStarting,
  }
  val startup_checkpoint_witness = lastResult == StartupCheckpointsLoaded
}
