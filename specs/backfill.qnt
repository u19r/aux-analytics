module backfill {
  type BackfillState =
    | Planned
    | SnapshotAcquiring
    | ChunkEnumerating
    | ChunkLoading
    | StreamCatchingUp
    | Validating
    | Checkpointing
    | Succeeded
    | Failed
    | Cancelled

  var state: BackfillState
  var destinationCommittedRows: int
  var loadedChunks: int
  var dirtyChunk: bool
  var checkpoint: int
  var streamCursor: int
  var validationPassed: bool
  var finalCheckRequired: bool
  var retryableChunks: int
  var newerStreamRowVisible: bool

  action init = all {
    state' = Planned,
    destinationCommittedRows' = 0,
    loadedChunks' = 0,
    dirtyChunk' = false,
    checkpoint' = 0,
    streamCursor' = 0,
    validationPassed' = false,
    finalCheckRequired' = true,
    retryableChunks' = 0,
    newerStreamRowVisible' = false,
  }

  action AcquireSnapshot = all {
    state == Planned,
    state' = SnapshotAcquiring,
    destinationCommittedRows' = destinationCommittedRows,
    loadedChunks' = loadedChunks,
    dirtyChunk' = dirtyChunk,
    checkpoint' = checkpoint,
    streamCursor' = streamCursor,
    validationPassed' = validationPassed,
    finalCheckRequired' = finalCheckRequired,
    retryableChunks' = retryableChunks,
    newerStreamRowVisible' = newerStreamRowVisible,
  }

  action EnumerateChunks = all {
    state == SnapshotAcquiring,
    state' = ChunkEnumerating,
    destinationCommittedRows' = destinationCommittedRows,
    loadedChunks' = loadedChunks,
    dirtyChunk' = dirtyChunk,
    checkpoint' = checkpoint,
    streamCursor' = streamCursor,
    validationPassed' = validationPassed,
    finalCheckRequired' = finalCheckRequired,
    retryableChunks' = retryableChunks,
    newerStreamRowVisible' = newerStreamRowVisible,
  }

  action LoadChunk = all {
    state == ChunkEnumerating or state == ChunkLoading,
    state' = ChunkLoading,
    destinationCommittedRows' = destinationCommittedRows + 1,
    loadedChunks' = loadedChunks + 1,
    dirtyChunk' = false,
    checkpoint' = checkpoint,
    streamCursor' = streamCursor,
    validationPassed' = validationPassed,
    finalCheckRequired' = finalCheckRequired,
    retryableChunks' = retryableChunks,
    newerStreamRowVisible' = newerStreamRowVisible,
  }

  action FailChunkBeforeCommit = all {
    state == ChunkLoading or state == ChunkEnumerating,
    state' = ChunkLoading,
    destinationCommittedRows' = destinationCommittedRows,
    loadedChunks' = loadedChunks,
    dirtyChunk' = true,
    checkpoint' = checkpoint,
    streamCursor' = streamCursor,
    validationPassed' = validationPassed,
    finalCheckRequired' = finalCheckRequired,
    retryableChunks' = retryableChunks + 1,
    newerStreamRowVisible' = newerStreamRowVisible,
  }

  action RetryChunk = all {
    state == ChunkLoading,
    retryableChunks > 0,
    state' = ChunkLoading,
    destinationCommittedRows' = destinationCommittedRows + 1,
    loadedChunks' = loadedChunks + 1,
    dirtyChunk' = false,
    checkpoint' = checkpoint,
    streamCursor' = streamCursor,
    validationPassed' = validationPassed,
    finalCheckRequired' = finalCheckRequired,
    retryableChunks' = retryableChunks - 1,
    newerStreamRowVisible' = newerStreamRowVisible,
  }

  action CatchUpStream = all {
    state == ChunkLoading,
    not(dirtyChunk),
    state' = StreamCatchingUp,
    destinationCommittedRows' = destinationCommittedRows + 1,
    loadedChunks' = loadedChunks,
    dirtyChunk' = dirtyChunk,
    checkpoint' = checkpoint,
    streamCursor' = streamCursor + 1,
    validationPassed' = validationPassed,
    finalCheckRequired' = finalCheckRequired,
    retryableChunks' = retryableChunks,
    newerStreamRowVisible' = true,
  }

  action Checkpoint = all {
    state == StreamCatchingUp,
    not(dirtyChunk),
    state' = Checkpointing,
    destinationCommittedRows' = destinationCommittedRows,
    loadedChunks' = loadedChunks,
    dirtyChunk' = dirtyChunk,
    checkpoint' = streamCursor,
    streamCursor' = streamCursor,
    validationPassed' = validationPassed,
    finalCheckRequired' = finalCheckRequired,
    retryableChunks' = retryableChunks,
    newerStreamRowVisible' = newerStreamRowVisible,
  }

  action Validate = all {
    state == Checkpointing,
    retryableChunks == 0,
    state' = Validating,
    destinationCommittedRows' = destinationCommittedRows,
    loadedChunks' = loadedChunks,
    dirtyChunk' = dirtyChunk,
    checkpoint' = checkpoint,
    streamCursor' = streamCursor,
    validationPassed' = true,
    finalCheckRequired' = finalCheckRequired,
    retryableChunks' = retryableChunks,
    newerStreamRowVisible' = newerStreamRowVisible,
  }

  action Complete = all {
    state == Validating,
    validationPassed,
    state' = Succeeded,
    destinationCommittedRows' = destinationCommittedRows,
    loadedChunks' = loadedChunks,
    dirtyChunk' = dirtyChunk,
    checkpoint' = checkpoint,
    streamCursor' = streamCursor,
    validationPassed' = validationPassed,
    finalCheckRequired' = finalCheckRequired,
    retryableChunks' = retryableChunks,
    newerStreamRowVisible' = newerStreamRowVisible,
  }

  action Cancel = all {
    state != Succeeded,
    state != Failed,
    state' = Cancelled,
    destinationCommittedRows' = destinationCommittedRows,
    loadedChunks' = loadedChunks,
    dirtyChunk' = dirtyChunk,
    checkpoint' = checkpoint,
    streamCursor' = streamCursor,
    validationPassed' = validationPassed,
    finalCheckRequired' = finalCheckRequired,
    retryableChunks' = retryableChunks,
    newerStreamRowVisible' = newerStreamRowVisible,
  }

  action step = any {
    AcquireSnapshot,
    EnumerateChunks,
    LoadChunk,
    FailChunkBeforeCommit,
    RetryChunk,
    CatchUpStream,
    Checkpoint,
    Validate,
    Complete,
    Cancel,
  }

  val checkpointOnlyAfterCleanCommit = checkpoint <= streamCursor and not(checkpoint > 0 and dirtyChunk)
  val chunkCompletionDurableAfterCommit = loadedChunks <= destinationCommittedRows
  val failedChunksRemainRetryable = retryableChunks >= 0
  val successRequiresValidation = if (state == Succeeded) validationPassed and finalCheckRequired else true
  val snapshotDoesNotOverwriteNewerStream = if (newerStreamRowVisible) destinationCommittedRows >= streamCursor else true
  val inv = all {
    checkpointOnlyAfterCleanCommit,
    chunkCompletionDurableAfterCommit,
    failedChunksRemainRetryable,
    successRequiresValidation,
    snapshotDoesNotOverwriteNewerStream,
  }

  run successfulBackfill = all {
    assert(finalCheckRequired),
    assert(checkpoint <= streamCursor),
    assert(if (state == Succeeded) validationPassed else true),
    assert(if (newerStreamRowVisible) destinationCommittedRows >= streamCursor else true),
  }

  run retryableChunk = all {
    assert(retryableChunks >= 0),
    assert(loadedChunks <= destinationCommittedRows),
    assert(if (dirtyChunk) checkpoint == 0 else true),
  }
}
