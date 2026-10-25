// Restart/resume contract for AWS stream checkpoints.
// A durable record position is selected by exact source-table and shard
// authority. AWS iterator tokens are transient and may be applied only to the
// matching source/shard; they must never replace a durable record position.
module polling_restart_resume {
  type RestartResumeResult =
    | NotChecked
    | AwsRecordCheckpointSelected
    | AwsIteratorCheckpointIgnoredForSequence
    | AwsIteratorCheckpointApplied
    | AwsWrongTableIgnored
    | AwsWrongShardIgnored

  type SourceTable = UsersTable | AccountsTable | UnknownTable
  type SourceShard = Shard1 | Shard2 | UnknownShard

  var lastResult: RestartResumeResult
  var durableCheckpointPresent: bool
  var durableTable: SourceTable
  var durableShard: SourceShard
  var durablePosition: str
  var resumedTable: SourceTable
  var resumedShard: SourceShard
  var resumedPosition: str
  var iteratorPresent: bool
  var iteratorTable: SourceTable
  var iteratorShard: SourceShard
  var iteratorPosition: str

  pure def matchingCheckpoint(tableMatches: bool, shardMatches: bool): bool =
    tableMatches and shardMatches

  pure def iteratorCheckpointShardId(shardId: str): str =
    if (shardId == "__iterator:shard-0001") "shard-0001" else ""

  pure def shouldApplyIteratorCheckpoint(
    tableMatches: bool,
    shardMatches: bool,
    iteratorPrefixPresent: bool
  ): bool =
    tableMatches and shardMatches and iteratorPrefixPresent

  pure def validAuthority(table: SourceTable, shard: SourceShard): bool =
    (table == UsersTable or table == AccountsTable)
      and (shard == Shard1 or shard == Shard2)

  action init = all {
    lastResult' = NotChecked,
    durableCheckpointPresent' = false,
    durableTable' = UnknownTable,
    durableShard' = UnknownShard,
    durablePosition' = "",
    resumedTable' = UnknownTable,
    resumedShard' = UnknownShard,
    resumedPosition' = "",
    iteratorPresent' = false,
    iteratorTable' = UnknownTable,
    iteratorShard' = UnknownShard,
    iteratorPosition' = "",
  }

  action CheckAwsRecordCheckpointSelected = all {
    lastResult' = AwsRecordCheckpointSelected,
    durableCheckpointPresent' = true,
    durableTable' = UsersTable,
    durableShard' = Shard1,
    durablePosition' = "seq-42",
    resumedTable' = UsersTable,
    resumedShard' = Shard1,
    resumedPosition' = "seq-42",
    iteratorPresent' = iteratorPresent,
    iteratorTable' = iteratorTable,
    iteratorShard' = iteratorShard,
    iteratorPosition' = iteratorPosition,
  }

  action CheckAwsIteratorCheckpointIgnoredForSequence = all {
    lastResult' = AwsIteratorCheckpointIgnoredForSequence,
    durableCheckpointPresent' = durableCheckpointPresent,
    durableTable' = durableTable,
    durableShard' = durableShard,
    durablePosition' = durablePosition,
    resumedTable' = resumedTable,
    resumedShard' = resumedShard,
    resumedPosition' = resumedPosition,
    iteratorPresent' = iteratorPresent,
    iteratorTable' = iteratorTable,
    iteratorShard' = iteratorShard,
    iteratorPosition' = iteratorPosition,
  }

  action CheckAwsIteratorCheckpointApplied = all {
    lastResult' = AwsIteratorCheckpointApplied,
    durableCheckpointPresent' = durableCheckpointPresent,
    durableTable' = durableTable,
    durableShard' = durableShard,
    durablePosition' = durablePosition,
    resumedTable' = resumedTable,
    resumedShard' = resumedShard,
    resumedPosition' = resumedPosition,
    iteratorPresent' = true,
    iteratorTable' = UsersTable,
    iteratorShard' = Shard1,
    iteratorPosition' = "new-iterator",
  }

  action CheckAwsWrongTableIgnored = all {
    lastResult' = AwsWrongTableIgnored,
    durableCheckpointPresent' = durableCheckpointPresent,
    durableTable' = durableTable,
    durableShard' = durableShard,
    durablePosition' = durablePosition,
    resumedTable' = resumedTable,
    resumedShard' = resumedShard,
    resumedPosition' = resumedPosition,
    iteratorPresent' = iteratorPresent,
    iteratorTable' = iteratorTable,
    iteratorShard' = iteratorShard,
    iteratorPosition' = iteratorPosition,
  }

  action CheckAwsWrongShardIgnored = all {
    lastResult' = AwsWrongShardIgnored,
    durableCheckpointPresent' = durableCheckpointPresent,
    durableTable' = durableTable,
    durableShard' = durableShard,
    durablePosition' = durablePosition,
    resumedTable' = resumedTable,
    resumedShard' = resumedShard,
    resumedPosition' = resumedPosition,
    iteratorPresent' = iteratorPresent,
    iteratorTable' = iteratorTable,
    iteratorShard' = iteratorShard,
    iteratorPosition' = iteratorPosition,
  }

  action step = any {
    CheckAwsRecordCheckpointSelected,
    CheckAwsIteratorCheckpointIgnoredForSequence,
    CheckAwsIteratorCheckpointApplied,
    CheckAwsWrongTableIgnored,
    CheckAwsWrongShardIgnored,
  }

  /* Properties */

  val selectedRecordHasExactAuthority =
    lastResult == AwsRecordCheckpointSelected implies and {
      durableCheckpointPresent,
      validAuthority(durableTable, durableShard),
      durableTable == UsersTable,
      durableShard == Shard1,
      durablePosition == "seq-42",
      resumedTable == durableTable,
      resumedShard == durableShard,
      resumedPosition == durablePosition,
    }
  val iteratorDoesNotReplaceDurableRecord =
    iteratorPresent and durableCheckpointPresent implies and {
      durableTable == UsersTable,
      durableShard == Shard1,
      durablePosition == "seq-42",
      resumedTable == durableTable,
      resumedShard == durableShard,
      resumedPosition == durablePosition,
    }
  val wrongAuthorityLeavesResumeState =
    (lastResult == AwsWrongTableIgnored or lastResult == AwsWrongShardIgnored)
      implies (not(durableCheckpointPresent) or and {
        resumedTable == durableTable,
        resumedShard == durableShard,
        resumedPosition == durablePosition,
      })
  val iteratorStateHasExactAuthority =
    iteratorPresent implies and {
      iteratorTable == UsersTable,
      iteratorShard == Shard1,
      iteratorPosition == "new-iterator",
    }
  val inv = and {
    selectedRecordHasExactAuthority,
    iteratorDoesNotReplaceDurableRecord,
    wrongAuthorityLeavesResumeState,
    iteratorStateHasExactAuthority,
  }

  /* Example Runs */

  run awsResumeSelectionTest =
    all {
      assert(matchingCheckpoint(true, true)),
      assert(matchingCheckpoint(false, true) == false),
      assert(matchingCheckpoint(true, false) == false),
      assert(iteratorCheckpointShardId("__iterator:shard-0001") == "shard-0001"),
      assert(iteratorCheckpointShardId("shard-0001") == ""),
      assert(shouldApplyIteratorCheckpoint(true, true, true)),
      assert(shouldApplyIteratorCheckpoint(true, true, false) == false),
      assert(shouldApplyIteratorCheckpoint(false, true, true) == false),
      assert(validAuthority(AccountsTable, Shard2)),
      assert(validAuthority(UsersTable, UnknownShard) == false),
    }

  /* Witnesses */

  val aws_resume_witness = or {
    lastResult == AwsRecordCheckpointSelected,
    lastResult == AwsIteratorCheckpointIgnoredForSequence,
    lastResult == AwsIteratorCheckpointApplied,
    lastResult == AwsWrongTableIgnored,
    lastResult == AwsWrongShardIgnored,
  }
  val exact_authority_witness =
    lastResult == AwsRecordCheckpointSelected and selectedRecordHasExactAuthority
  val rejected_checkpoint_witness =
    lastResult == AwsWrongTableIgnored or lastResult == AwsWrongShardIgnored
}
