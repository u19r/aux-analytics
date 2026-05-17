module backfill_bughunt {
  type BackfillState =
    | Planned
    | Loading
    | CatchingUp
    | Validating
    | Succeeded
    | Failed
    | Cancelled

  var state: BackfillState
  var snapshotRowsLoaded: int
  var streamRowsApplied: int
  var checkpoint: int
  var dirtyBatch: bool
  var retryableChunks: int
  var validationPassed: bool
  var streamRetentionExpired: bool
  var tableDeleted: bool
  var tableAdded: bool

  action init = all {
    state' = Planned,
    snapshotRowsLoaded' = 0,
    streamRowsApplied' = 0,
    checkpoint' = 0,
    dirtyBatch' = false,
    retryableChunks' = 0,
    validationPassed' = false,
    streamRetentionExpired' = false,
    tableDeleted' = false,
    tableAdded' = false,
  }

  action Start = all {
    state == Planned,
    state' = Loading,
    snapshotRowsLoaded' = snapshotRowsLoaded,
    streamRowsApplied' = streamRowsApplied,
    checkpoint' = checkpoint,
    dirtyBatch' = dirtyBatch,
    retryableChunks' = retryableChunks,
    validationPassed' = validationPassed,
    streamRetentionExpired' = streamRetentionExpired,
    tableDeleted' = tableDeleted,
    tableAdded' = tableAdded,
  }

  action LoadSnapshotChunk = all {
    state == Loading,
    not(tableDeleted),
    state' = Loading,
    snapshotRowsLoaded' = snapshotRowsLoaded + 1,
    streamRowsApplied' = streamRowsApplied,
    checkpoint' = checkpoint,
    dirtyBatch' = false,
    retryableChunks' = retryableChunks,
    validationPassed' = validationPassed,
    streamRetentionExpired' = streamRetentionExpired,
    tableDeleted' = tableDeleted,
    tableAdded' = tableAdded,
  }

  action DirtyChunkFailure = all {
    state == Loading,
    state' = Loading,
    snapshotRowsLoaded' = snapshotRowsLoaded,
    streamRowsApplied' = streamRowsApplied,
    checkpoint' = checkpoint,
    dirtyBatch' = true,
    retryableChunks' = retryableChunks + 1,
    validationPassed' = validationPassed,
    streamRetentionExpired' = streamRetentionExpired,
    tableDeleted' = tableDeleted,
    tableAdded' = tableAdded,
  }

  action RetryDirtyChunk = all {
    state == Loading,
    retryableChunks > 0,
    state' = Loading,
    snapshotRowsLoaded' = snapshotRowsLoaded + 1,
    streamRowsApplied' = streamRowsApplied,
    checkpoint' = checkpoint,
    dirtyBatch' = false,
    retryableChunks' = retryableChunks - 1,
    validationPassed' = validationPassed,
    streamRetentionExpired' = streamRetentionExpired,
    tableDeleted' = tableDeleted,
    tableAdded' = tableAdded,
  }

  action ObserveStreamUpdate = all {
    state == Loading,
    state' = CatchingUp,
    snapshotRowsLoaded' = snapshotRowsLoaded,
    streamRowsApplied' = streamRowsApplied + 1,
    checkpoint' = checkpoint,
    dirtyBatch' = dirtyBatch,
    retryableChunks' = retryableChunks,
    validationPassed' = validationPassed,
    streamRetentionExpired' = streamRetentionExpired,
    tableDeleted' = tableDeleted,
    tableAdded' = tableAdded,
  }

  action AdvanceCheckpoint = all {
    state == CatchingUp,
    not(dirtyBatch),
    state' = CatchingUp,
    snapshotRowsLoaded' = snapshotRowsLoaded,
    streamRowsApplied' = streamRowsApplied,
    checkpoint' = streamRowsApplied,
    dirtyBatch' = dirtyBatch,
    retryableChunks' = retryableChunks,
    validationPassed' = validationPassed,
    streamRetentionExpired' = streamRetentionExpired,
    tableDeleted' = tableDeleted,
    tableAdded' = tableAdded,
  }

  action StreamRetentionExpires = all {
    state != Succeeded,
    state' = state,
    snapshotRowsLoaded' = snapshotRowsLoaded,
    streamRowsApplied' = streamRowsApplied,
    checkpoint' = checkpoint,
    dirtyBatch' = dirtyBatch,
    retryableChunks' = retryableChunks,
    validationPassed' = validationPassed,
    streamRetentionExpired' = true,
    tableDeleted' = tableDeleted,
    tableAdded' = tableAdded,
  }

  action DeleteTable = all {
    state != Succeeded,
    state' = Failed,
    snapshotRowsLoaded' = snapshotRowsLoaded,
    streamRowsApplied' = streamRowsApplied,
    checkpoint' = checkpoint,
    dirtyBatch' = dirtyBatch,
    retryableChunks' = retryableChunks,
    validationPassed' = validationPassed,
    streamRetentionExpired' = streamRetentionExpired,
    tableDeleted' = true,
    tableAdded' = tableAdded,
  }

  action AddTable = all {
    state != Succeeded,
    state' = state,
    snapshotRowsLoaded' = snapshotRowsLoaded,
    streamRowsApplied' = streamRowsApplied,
    checkpoint' = checkpoint,
    dirtyBatch' = dirtyBatch,
    retryableChunks' = retryableChunks,
    validationPassed' = validationPassed,
    streamRetentionExpired' = streamRetentionExpired,
    tableDeleted' = tableDeleted,
    tableAdded' = true,
  }

  action Validate = all {
    state == CatchingUp,
    checkpoint == streamRowsApplied,
    retryableChunks == 0,
    state' = Validating,
    snapshotRowsLoaded' = snapshotRowsLoaded,
    streamRowsApplied' = streamRowsApplied,
    checkpoint' = checkpoint,
    dirtyBatch' = dirtyBatch,
    retryableChunks' = retryableChunks,
    validationPassed' = true,
    streamRetentionExpired' = streamRetentionExpired,
    tableDeleted' = tableDeleted,
    tableAdded' = tableAdded,
  }

  action Complete = all {
    state == Validating,
    validationPassed,
    not(tableDeleted),
    state' = Succeeded,
    snapshotRowsLoaded' = snapshotRowsLoaded,
    streamRowsApplied' = streamRowsApplied,
    checkpoint' = checkpoint,
    dirtyBatch' = dirtyBatch,
    retryableChunks' = retryableChunks,
    validationPassed' = validationPassed,
    streamRetentionExpired' = streamRetentionExpired,
    tableDeleted' = tableDeleted,
    tableAdded' = tableAdded,
  }

  action step = {
    nondet action_choice = Set("start", "load", "dirty", "retry", "stream", "checkpoint", "expire", "delete", "add", "validate", "complete").oneOf()
    if (action_choice == "start") Start
    else if (action_choice == "load") LoadSnapshotChunk
    else if (action_choice == "dirty") DirtyChunkFailure
    else if (action_choice == "retry") RetryDirtyChunk
    else if (action_choice == "stream") ObserveStreamUpdate
    else if (action_choice == "checkpoint") AdvanceCheckpoint
    else if (action_choice == "expire") StreamRetentionExpires
    else if (action_choice == "delete") DeleteTable
    else if (action_choice == "add") AddTable
    else if (action_choice == "validate") Validate
    else Complete
  }

  val noDirtyCheckpointAdvance = not(dirtyBatch and checkpoint == streamRowsApplied and streamRowsApplied > 0)
  val failedChunksRetryable = retryableChunks >= 0
  val successRequiresCaughtUpCheckpoint = if (state == Succeeded) checkpoint == streamRowsApplied else true
  val successRequiresValidation = if (state == Succeeded) validationPassed else true
  val deletedTableCannotSucceed = if (tableDeleted) state != Succeeded else true
  val retentionExpiryDoesNotForceStreamOnlySuccess = if (streamRetentionExpired and state == Succeeded) validationPassed else true

  val inv = all {
    noDirtyCheckpointAdvance,
    failedChunksRetryable,
    successRequiresCaughtUpCheckpoint,
    successRequiresValidation,
    deletedTableCannotSucceed,
    retentionExpiryDoesNotForceStreamOnlySuccess,
  }

  run interruptionAndRetry = all {
    assert(retryableChunks >= 0),
    assert(if (state == Succeeded) checkpoint == streamRowsApplied else true),
    assert(if (state == Succeeded) validationPassed else true),
    assert(if (tableDeleted) state != Succeeded else true),
  }
}
