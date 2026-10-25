// Storage polling contract for fan-out, AWS checkpointing, and the AuxStorage
// source-scope boundary. AuxStorage system-stream responses currently lack a
// persisted shard or partition identity, so polling fails closed without
// emitting records or advancing a global cursor. AWS record checkpoints and
// iterator tokens remain distinct: only record positions are durable and an
// iterator token is scoped to its exact source table and shard.
module polling_storage {
  type Cursor = CursorValue(str) | NoCursor
  // A rejected AuxStorage response has no source identity. Do not represent
  // that absence with a fabricated table or shard name.
  type SourceTable = NoSourceTable | UsersTable
  type SourceShard = NoSourceShard | Shard1

  type StoragePollingResult =
    | NotChecked
    | FanOutComplete
    | AuxScopeRejected
    | AwsIteratorAdvanced
    | AwsRecordAndIteratorCheckpointsEmitted
    | AwsIteratorOnlyCheckpointEmitted

  var lastResult: StoragePollingResult
  var sourceTable: SourceTable
  var sourceShard: SourceShard
  var recordCount: int
  var analyticsTableCount: int
  var routedRecordCount: int
  var auxScopeRejected: bool
  var auxCursorPreserved: bool
  var auxCursorBefore: Cursor
  var auxCursorAfter: Cursor
  var durableCheckpointPresent: bool
  var durableCheckpointCursor: Cursor
  var durableCheckpointPresentBefore: bool
  var durableCheckpointCursorBefore: Cursor
  var awsRecordCheckpointPresent: bool
  var recordCheckpointCount: int
  var awsIteratorCheckpointPresent: bool
  var iteratorCheckpointCount: int
  var iteratorCheckpointCursor: Cursor
  var awsIteratorApplied: bool
  var iteratorCheckpointDurable: bool
  var iteratorSourceTable: SourceTable
  var iteratorSourceShard: SourceShard

  pure def fanOutRecordCount(recordCount: int, analyticsTableCount: int): int =
    recordCount * analyticsTableCount

  pure def awsCheckpointCount(recordCount: int, nextIterator: Cursor): int =
    match nextIterator {
      | CursorValue(_) => if (recordCount > 0) 2 else 1
      | NoCursor => if (recordCount > 0) 1 else 0
    }

  pure val durableIteratorCheckpointPolicy: bool = false

  pure def storagePollingCase(result: StoragePollingResult): StoragePollingResult =
    result

  action init = all {
    lastResult' = NotChecked,
    sourceTable' = NoSourceTable,
    sourceShard' = NoSourceShard,
    recordCount' = 0,
    analyticsTableCount' = 0,
    routedRecordCount' = 0,
    auxScopeRejected' = false,
    auxCursorPreserved' = false,
    auxCursorBefore' = NoCursor,
    auxCursorAfter' = NoCursor,
    durableCheckpointPresent' = false,
    durableCheckpointCursor' = NoCursor,
    durableCheckpointPresentBefore' = false,
    durableCheckpointCursorBefore' = NoCursor,
    awsRecordCheckpointPresent' = false,
    recordCheckpointCount' = 0,
    awsIteratorCheckpointPresent' = false,
    iteratorCheckpointCount' = 0,
    iteratorCheckpointCursor' = NoCursor,
    awsIteratorApplied' = false,
    iteratorCheckpointDurable' = false,
    iteratorSourceTable' = NoSourceTable,
    iteratorSourceShard' = NoSourceShard,
  }

  action CheckFanOut = all {
    lastResult' = storagePollingCase(FanOutComplete),
    sourceTable' = UsersTable,
    sourceShard' = Shard1,
    recordCount' = 1,
    analyticsTableCount' = 2,
    routedRecordCount' = fanOutRecordCount(1, 2),
    auxScopeRejected' = false,
    auxCursorPreserved' = false,
    auxCursorBefore' = NoCursor,
    auxCursorAfter' = NoCursor,
    durableCheckpointPresent' = durableCheckpointPresent,
    durableCheckpointCursor' = durableCheckpointCursor,
    durableCheckpointPresentBefore' = durableCheckpointPresent,
    durableCheckpointCursorBefore' = durableCheckpointCursor,
    awsRecordCheckpointPresent' = false,
    recordCheckpointCount' = 0,
    awsIteratorCheckpointPresent' = false,
    iteratorCheckpointCount' = 0,
    iteratorCheckpointCursor' = NoCursor,
    awsIteratorApplied' = false,
    iteratorCheckpointDurable' = false,
    iteratorSourceTable' = NoSourceTable,
    iteratorSourceShard' = NoSourceShard,
  }

  action CheckAuxScopeRejected = all {
    lastResult' = storagePollingCase(AuxScopeRejected),
    sourceTable' = NoSourceTable,
    sourceShard' = NoSourceShard,
    recordCount' = 0,
    analyticsTableCount' = 0,
    routedRecordCount' = 0,
    auxScopeRejected' = true,
    auxCursorPreserved' = true,
    auxCursorBefore' = CursorValue("prior-page"),
    auxCursorAfter' = CursorValue("prior-page"),
    durableCheckpointPresent' = durableCheckpointPresent,
    durableCheckpointCursor' = durableCheckpointCursor,
    durableCheckpointPresentBefore' = durableCheckpointPresent,
    durableCheckpointCursorBefore' = durableCheckpointCursor,
    awsRecordCheckpointPresent' = false,
    recordCheckpointCount' = 0,
    awsIteratorCheckpointPresent' = false,
    iteratorCheckpointCount' = 0,
    iteratorCheckpointCursor' = NoCursor,
    awsIteratorApplied' = false,
    iteratorCheckpointDurable' = false,
    iteratorSourceTable' = NoSourceTable,
    iteratorSourceShard' = NoSourceShard,
  }

  action CheckAwsIteratorAdvances = all {
    lastResult' = storagePollingCase(AwsIteratorAdvanced),
    sourceTable' = UsersTable,
    sourceShard' = Shard1,
    recordCount' = 0,
    analyticsTableCount' = 0,
    routedRecordCount' = 0,
    auxScopeRejected' = false,
    auxCursorPreserved' = false,
    auxCursorBefore' = NoCursor,
    auxCursorAfter' = NoCursor,
    durableCheckpointPresent' = durableCheckpointPresent,
    durableCheckpointCursor' = durableCheckpointCursor,
    durableCheckpointPresentBefore' = durableCheckpointPresent,
    durableCheckpointCursorBefore' = durableCheckpointCursor,
    awsRecordCheckpointPresent' = false,
    recordCheckpointCount' = 0,
    awsIteratorCheckpointPresent' = true,
    iteratorCheckpointCount' = 1,
    iteratorCheckpointCursor' = CursorValue("new-iterator"),
    awsIteratorApplied' = true,
    iteratorCheckpointDurable' = false,
    iteratorSourceTable' = UsersTable,
    iteratorSourceShard' = Shard1,
  }

  action CheckAwsBatchCheckpoints = all {
    lastResult' = storagePollingCase(AwsRecordAndIteratorCheckpointsEmitted),
    sourceTable' = UsersTable,
    sourceShard' = Shard1,
    recordCount' = 2,
    analyticsTableCount' = 2,
    routedRecordCount' = 4,
    auxScopeRejected' = false,
    auxCursorPreserved' = false,
    auxCursorBefore' = NoCursor,
    auxCursorAfter' = NoCursor,
    durableCheckpointPresent' = true,
    durableCheckpointCursor' = CursorValue("seq-2"),
    durableCheckpointPresentBefore' = durableCheckpointPresent,
    durableCheckpointCursorBefore' = durableCheckpointCursor,
    awsRecordCheckpointPresent' = true,
    recordCheckpointCount' = 1,
    awsIteratorCheckpointPresent' = true,
    iteratorCheckpointCount' = 1,
    iteratorCheckpointCursor' = CursorValue("next-iterator"),
    awsIteratorApplied' = false,
    iteratorCheckpointDurable' = durableIteratorCheckpointPolicy,
    iteratorSourceTable' = UsersTable,
    iteratorSourceShard' = Shard1,
  }

  action CheckAwsIteratorOnlyCheckpoint = all {
    lastResult' = storagePollingCase(AwsIteratorOnlyCheckpointEmitted),
    sourceTable' = UsersTable,
    sourceShard' = Shard1,
    recordCount' = 0,
    analyticsTableCount' = 0,
    routedRecordCount' = 0,
    auxScopeRejected' = false,
    auxCursorPreserved' = false,
    auxCursorBefore' = NoCursor,
    auxCursorAfter' = NoCursor,
    durableCheckpointPresent' = durableCheckpointPresent,
    durableCheckpointCursor' = durableCheckpointCursor,
    durableCheckpointPresentBefore' = durableCheckpointPresent,
    durableCheckpointCursorBefore' = durableCheckpointCursor,
    awsRecordCheckpointPresent' = false,
    recordCheckpointCount' = 0,
    awsIteratorCheckpointPresent' = true,
    iteratorCheckpointCount' = 1,
    iteratorCheckpointCursor' = CursorValue("next-iterator"),
    awsIteratorApplied' = false,
    iteratorCheckpointDurable' = false,
    iteratorSourceTable' = UsersTable,
    iteratorSourceShard' = Shard1,
  }

  action step = any {
    CheckFanOut,
    CheckAuxScopeRejected,
    CheckAwsIteratorAdvances,
    CheckAwsBatchCheckpoints,
    CheckAwsIteratorOnlyCheckpoint,
  }

  /* Properties */

  val fanOutMatchesSource =
    routedRecordCount == fanOutRecordCount(recordCount, analyticsTableCount)
  val fanOutHasOneSourceScope =
    lastResult == FanOutComplete implies and {
      sourceTable == UsersTable,
      sourceShard == Shard1,
      recordCount > 0,
      analyticsTableCount > 0,
    }
  val auxScopeFailureIsFailClosed =
    lastResult == AuxScopeRejected implies and {
      sourceTable == NoSourceTable,
      sourceShard == NoSourceShard,
      auxScopeRejected,
      auxCursorPreserved,
      auxCursorBefore == auxCursorAfter,
      routedRecordCount == 0,
      not(awsRecordCheckpointPresent),
      recordCheckpointCount == 0,
      iteratorCheckpointCount == 0,
      durableCheckpointPresent == durableCheckpointPresentBefore,
      durableCheckpointCursor == durableCheckpointCursorBefore,
    }
  val awsRecordCheckpointMatchesRecords =
    awsRecordCheckpointPresent implies and {
      recordCount > 0,
      recordCheckpointCount == 1,
      durableCheckpointPresent,
      durableCheckpointCursor != NoCursor,
    }
  val awsBatchHasOneSourceScope =
    (lastResult == AwsRecordAndIteratorCheckpointsEmitted or
      lastResult == AwsIteratorOnlyCheckpointEmitted) implies and {
      sourceTable == UsersTable,
      sourceShard == Shard1,
    }
  val iteratorCheckpointHasExactSource =
    awsIteratorCheckpointPresent implies and {
      iteratorCheckpointCount == 1,
      iteratorCheckpointCursor != NoCursor,
      iteratorSourceTable == sourceTable,
      iteratorSourceShard == sourceShard,
    }
  val iteratorCheckpointsAreTransient = not(iteratorCheckpointDurable)
  val iteratorCheckpointPreservesDurability =
    lastResult == AwsIteratorAdvanced or lastResult == AwsIteratorOnlyCheckpointEmitted
      implies and {
        durableCheckpointPresent == durableCheckpointPresentBefore,
        durableCheckpointCursor == durableCheckpointCursorBefore,
      }
  val iteratorApplicationHasExactSource =
    awsIteratorApplied implies and {
      awsIteratorCheckpointPresent,
      iteratorCheckpointCursor != NoCursor,
    }
  val durableRecordCheckpointShape =
    lastResult == AwsRecordAndIteratorCheckpointsEmitted implies and {
      durableCheckpointPresent,
      durableCheckpointCursor == CursorValue("seq-2"),
      awsRecordCheckpointPresent,
      recordCheckpointCount == 1,
      awsIteratorCheckpointPresent,
      iteratorCheckpointCount == 1,
    }
  val inv = and {
    fanOutMatchesSource,
    fanOutHasOneSourceScope,
    auxScopeFailureIsFailClosed,
    awsRecordCheckpointMatchesRecords,
    awsBatchHasOneSourceScope,
    iteratorCheckpointHasExactSource,
    iteratorCheckpointsAreTransient,
    iteratorCheckpointPreservesDurability,
    iteratorApplicationHasExactSource,
    durableRecordCheckpointShape,
  }

  /* Example Runs */

  run fanOutTest =
    all {
      assert(fanOutRecordCount(1, 2) == 2),
      assert(fanOutRecordCount(2, 3) == 6),
    }

  run awsCheckpointAdvancementTest =
    all {
      assert(awsCheckpointCount(2, CursorValue("next-iterator")) == 2),
      assert(awsCheckpointCount(0, CursorValue("next-iterator")) == 1),
      assert(awsCheckpointCount(2, NoCursor) == 1),
      assert(awsCheckpointCount(0, NoCursor) == 0),
    }

  /* Witnesses */

  val fan_out_witness = lastResult == FanOutComplete and routedRecordCount > 0
  val aux_scope_rejected_witness =
    lastResult == AuxScopeRejected and auxScopeRejected and auxCursorPreserved
  val aws_checkpoint_witness = or {
    lastResult == AwsIteratorAdvanced,
    lastResult == AwsRecordAndIteratorCheckpointsEmitted,
    lastResult == AwsIteratorOnlyCheckpointEmitted,
  }
  val transient_iterator_witness = awsIteratorCheckpointPresent and not(iteratorCheckpointDurable)
  val iterator_durable_preservation_witness =
    (lastResult == AwsIteratorAdvanced or lastResult == AwsIteratorOnlyCheckpointEmitted) and
      durableCheckpointPresentBefore and
      durableCheckpointPresent == durableCheckpointPresentBefore and
      durableCheckpointCursor == durableCheckpointCursorBefore
}
