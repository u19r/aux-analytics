module polling_restart_resume {
  type RestartResumeResult =
    | NotChecked
    | AuxPersistedCheckpointSelected
    | AuxWrongTableIgnored
    | AuxWrongShardIgnored
    | AwsRecordCheckpointSelected
    | AwsIteratorCheckpointIgnoredForSequence
    | AwsIteratorCheckpointApplied
    | AwsWrongTableIgnored

  var lastResult: RestartResumeResult

  pure val restartResumeResults: Set[RestartResumeResult] = Set(
    NotChecked,
    AuxPersistedCheckpointSelected,
    AuxWrongTableIgnored,
    AuxWrongShardIgnored,
    AwsRecordCheckpointSelected,
    AwsIteratorCheckpointIgnoredForSequence,
    AwsIteratorCheckpointApplied,
    AwsWrongTableIgnored
  )

  pure def matchingCheckpoint(tableMatches: bool, shardMatches: bool): bool =
    tableMatches and shardMatches

  pure def iteratorCheckpointShardId(shardId: str): str =
    if (shardId == "__iterator:shard-0001") "shard-0001" else ""

  pure def shouldApplyIteratorCheckpoint(tableMatches: bool, shardMatches: bool, iteratorPrefixPresent: bool): bool =
    tableMatches and shardMatches and iteratorPrefixPresent

  action init =
    lastResult' = NotChecked

  action CheckAuxPersistedCheckpointSelected =
    lastResult' = AuxPersistedCheckpointSelected

  action CheckAuxWrongTableIgnored =
    lastResult' = AuxWrongTableIgnored

  action CheckAuxWrongShardIgnored =
    lastResult' = AuxWrongShardIgnored

  action CheckAwsRecordCheckpointSelected =
    lastResult' = AwsRecordCheckpointSelected

  action CheckAwsIteratorCheckpointIgnoredForSequence =
    lastResult' = AwsIteratorCheckpointIgnoredForSequence

  action CheckAwsIteratorCheckpointApplied =
    lastResult' = AwsIteratorCheckpointApplied

  action CheckAwsWrongTableIgnored =
    lastResult' = AwsWrongTableIgnored

  action step = any {
    CheckAuxPersistedCheckpointSelected,
    CheckAuxWrongTableIgnored,
    CheckAuxWrongShardIgnored,
    CheckAwsRecordCheckpointSelected,
    CheckAwsIteratorCheckpointIgnoredForSequence,
    CheckAwsIteratorCheckpointApplied,
    CheckAwsWrongTableIgnored,
  }

  /* Properties */

  val resultIsKnown = restartResumeResults.contains(lastResult)
  val inv = resultIsKnown

  /* Example Runs */

  run auxResumeSelectionTest =
    all {
      assert(matchingCheckpoint(true, true)),
      assert(matchingCheckpoint(false, true) == false),
      assert(matchingCheckpoint(true, false) == false),
    }

  run awsResumeSelectionTest =
    all {
      assert(iteratorCheckpointShardId("__iterator:shard-0001") == "shard-0001"),
      assert(iteratorCheckpointShardId("shard-0001") == ""),
      assert(shouldApplyIteratorCheckpoint(true, true, true)),
      assert(shouldApplyIteratorCheckpoint(true, true, false) == false),
      assert(shouldApplyIteratorCheckpoint(false, true, true) == false),
    }

  /* Witnesses */

  val aux_resume_witness = or {
    lastResult == AuxPersistedCheckpointSelected,
    lastResult == AuxWrongTableIgnored,
    lastResult == AuxWrongShardIgnored,
  }
  val aws_resume_witness = or {
    lastResult == AwsRecordCheckpointSelected,
    lastResult == AwsIteratorCheckpointIgnoredForSequence,
    lastResult == AwsIteratorCheckpointApplied,
    lastResult == AwsWrongTableIgnored,
  }
}
