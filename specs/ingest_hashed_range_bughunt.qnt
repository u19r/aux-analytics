module ingest_hashed_range_bughunt {
  type Version = V0 | V1 | V2
  type ProcessorMode = QueryOnly | IngestCapable

  var mode: ProcessorMode
  var heartbeatFresh: bool
  var leaseHeld: bool
  var ingestSucceeded: bool
  var progressSaved: bool
  var progressVersion: Version
  var trimmedThrough: Version
  var bootstrapCommitters: int
  var invalidProgressCommit: bool

  pure def rank(version: Version): int =
    match version {
      | V0 => 0
      | V1 => 1
      | V2 => 2
    }

  pure def nextVersion(version: Version): Version =
    match version {
      | V0 => V1
      | V1 => V2
      | V2 => V2
    }

  action init = all {
    mode' = QueryOnly,
    heartbeatFresh' = false,
    leaseHeld' = false,
    ingestSucceeded' = false,
    progressSaved' = false,
    progressVersion' = V0,
    trimmedThrough' = V0,
    bootstrapCommitters' = 0,
    invalidProgressCommit' = false,
  }

  action WriteHeartbeat = {
    nondet nextMode = Set(QueryOnly, IngestCapable).oneOf()
    all {
      mode' = nextMode,
      heartbeatFresh' = nextMode == IngestCapable,
      leaseHeld' = if (nextMode == QueryOnly) false else leaseHeld,
      ingestSucceeded' = ingestSucceeded,
      progressSaved' = progressSaved,
      progressVersion' = progressVersion,
      trimmedThrough' = trimmedThrough,
      bootstrapCommitters' = bootstrapCommitters,
      invalidProgressCommit' = invalidProgressCommit,
    }
  }

  action LoseHeartbeat = all {
    mode' = mode,
    heartbeatFresh' = false,
    leaseHeld' = if (mode == QueryOnly) false else leaseHeld,
    ingestSucceeded' = ingestSucceeded,
    progressSaved' = progressSaved,
    progressVersion' = progressVersion,
    trimmedThrough' = trimmedThrough,
    bootstrapCommitters' = bootstrapCommitters,
    invalidProgressCommit' = invalidProgressCommit,
  }

  action AcquireLease = all {
    mode' = mode,
    heartbeatFresh' = heartbeatFresh,
    leaseHeld' = mode == IngestCapable and heartbeatFresh,
    ingestSucceeded' = ingestSucceeded,
    progressSaved' = progressSaved,
    progressVersion' = progressVersion,
    trimmedThrough' = trimmedThrough,
    bootstrapCommitters' = bootstrapCommitters,
    invalidProgressCommit' = invalidProgressCommit,
  }

  action LoseLease = all {
    mode' = mode,
    heartbeatFresh' = heartbeatFresh,
    leaseHeld' = false,
    ingestSucceeded' = ingestSucceeded,
    progressSaved' = false,
    progressVersion' = progressVersion,
    trimmedThrough' = trimmedThrough,
    bootstrapCommitters' = bootstrapCommitters,
    invalidProgressCommit' = invalidProgressCommit,
  }

  action PollAndIngest = {
    nondet succeeds = Set(true, false).oneOf()
    all {
      mode' = mode,
      heartbeatFresh' = heartbeatFresh,
      leaseHeld' = leaseHeld,
      ingestSucceeded' = leaseHeld and succeeds,
      progressSaved' = progressSaved,
      progressVersion' = progressVersion,
      trimmedThrough' = trimmedThrough,
      bootstrapCommitters' = bootstrapCommitters,
      invalidProgressCommit' = invalidProgressCommit,
    }
  }

  action SaveProgress = {
    nondet saveSucceeds = Set(true, false).oneOf()
    val canSave = leaseHeld and ingestSucceeded and saveSucceeds
    val invalidSave = saveSucceeds and (leaseHeld == false or ingestSucceeded == false)
    all {
      mode' = mode,
      heartbeatFresh' = heartbeatFresh,
      leaseHeld' = leaseHeld,
      ingestSucceeded' = ingestSucceeded,
      progressSaved' = canSave,
      progressVersion' = if (canSave) nextVersion(progressVersion) else progressVersion,
      trimmedThrough' = trimmedThrough,
      bootstrapCommitters' = bootstrapCommitters,
      invalidProgressCommit' = invalidProgressCommit or invalidSave,
    }
  }

  action Trim = all {
    mode' = mode,
    heartbeatFresh' = heartbeatFresh,
    leaseHeld' = leaseHeld,
    ingestSucceeded' = ingestSucceeded,
    progressSaved' = progressSaved,
    progressVersion' = progressVersion,
    trimmedThrough' = if (progressSaved) progressVersion else trimmedThrough,
    bootstrapCommitters' = bootstrapCommitters,
    invalidProgressCommit' = invalidProgressCommit,
  }

  action CommitBootstrap = all {
    mode' = mode,
    heartbeatFresh' = heartbeatFresh,
    leaseHeld' = leaseHeld,
    ingestSucceeded' = ingestSucceeded,
    progressSaved' = progressSaved,
    progressVersion' = progressVersion,
    trimmedThrough' = trimmedThrough,
    bootstrapCommitters' = if (bootstrapCommitters == 0 and leaseHeld) 1 else bootstrapCommitters,
    invalidProgressCommit' = invalidProgressCommit,
  }

  action step = any {
    WriteHeartbeat,
    LoseHeartbeat,
    AcquireLease,
    LoseLease,
    PollAndIngest,
    SaveProgress,
    Trim,
    CommitBootstrap,
  }

  val queryOnlyDoesNotLease =
    mode != QueryOnly or leaseHeld == false

  val progressRequiresSuccessfulSave =
    invalidProgressCommit == false

  val progressRequiresLease =
    invalidProgressCommit == false

  val trimNeverAheadOfProgress =
    rank(trimmedThrough) <= rank(progressVersion)

  val bootstrapHasSingleCommitter =
    bootstrapCommitters <= 1

  val inv = all {
    queryOnlyDoesNotLease,
    progressRequiresSuccessfulSave,
    progressRequiresLease,
    trimNeverAheadOfProgress,
    bootstrapHasSingleCommitter,
  }
}
