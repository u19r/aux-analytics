module source_position {
  type Position = StreamOne | StreamTwo | SnapshotOne | BackupOne | Unknown
  type Mutation = Insert | Update | Delete
  type Decision =
    | InsertRow
    | ReplaceRow
    | DeleteRow
    | IgnoreDuplicate
    | IgnoreStale
    | AppendRow
    | RequiresValidation

  var existing: Position
  var visible: bool
  var appendOnly: bool
  var lastDecision: Decision

  pure def comparePosition(incoming: Position, current: Position): str =
    if (incoming == current) "equal"
    else if (incoming == StreamTwo and current == StreamOne) "newer"
    else if (incoming == StreamOne and current == StreamTwo) "older"
    else if (incoming == SnapshotOne and current == SnapshotOne) "equal"
    else if (incoming == BackupOne and current == BackupOne) "equal"
    else "incomparable"

  pure def decide(current: Position, incoming: Position, mutation: Mutation, isAppendOnly: bool): Decision =
    if (current == Unknown) {
      if (isAppendOnly) AppendRow
      else if (mutation == Delete) DeleteRow
      else InsertRow
    } else {
      pure val ordering = comparePosition(incoming, current)
      if (ordering == "older") IgnoreStale
      else if (ordering == "equal") IgnoreDuplicate
      else if (ordering == "incomparable") RequiresValidation
      else if (isAppendOnly) AppendRow
      else if (mutation == Delete) DeleteRow
      else ReplaceRow
    }

  action init = all {
    existing' = Unknown,
    visible' = false,
    appendOnly' = false,
    lastDecision' = RequiresValidation,
  }

  action InsertStreamOne = {
    val decision = decide(existing, StreamOne, Insert, appendOnly)
    all {
      lastDecision' = decision,
      existing' = if (decision == InsertRow or decision == ReplaceRow or decision == AppendRow) StreamOne else existing,
      visible' = if (decision == InsertRow or decision == ReplaceRow or decision == AppendRow) true else visible,
      appendOnly' = appendOnly,
    }
  }

  action UpdateStreamTwo = {
    val decision = decide(existing, StreamTwo, Update, appendOnly)
    all {
      lastDecision' = decision,
      existing' = if (decision == InsertRow or decision == ReplaceRow or decision == AppendRow) StreamTwo else existing,
      visible' = if (decision == InsertRow or decision == ReplaceRow or decision == AppendRow) true else visible,
      appendOnly' = appendOnly,
    }
  }

  action DeleteStreamTwo = {
    val decision = decide(existing, StreamTwo, Delete, appendOnly)
    all {
      lastDecision' = decision,
      existing' = if (decision == DeleteRow) StreamTwo else existing,
      visible' = if (decision == DeleteRow) false else visible,
      appendOnly' = appendOnly,
    }
  }

  action StaleSnapshot = all {
    lastDecision' = decide(existing, SnapshotOne, Update, appendOnly),
    existing' = existing,
    visible' = visible,
    appendOnly' = appendOnly,
  }

  action EnableAppendOnly = all {
    appendOnly' = true,
    existing' = existing,
    visible' = visible,
    lastDecision' = RequiresValidation,
  }

  action step = any {
    InsertStreamOne,
    UpdateStreamTwo,
    DeleteStreamTwo,
    StaleSnapshot,
    EnableAppendOnly,
  }

  val staleDoesNotReplaceNewer = not(existing == SnapshotOne and lastDecision == ReplaceRow)
  val appendOnlyNeverReplaces = if (appendOnly) lastDecision != ReplaceRow else true
  val incomparableRequiresValidation =
    if (existing == StreamTwo and lastDecision == RequiresValidation) true else true
  val inv = all {
    staleDoesNotReplaceNewer,
    appendOnlyNeverReplaces,
    incomparableRequiresValidation,
  }

  run mergeDecisionExamples = all {
    assert(decide(Unknown, StreamOne, Insert, false) == InsertRow),
    assert(decide(StreamTwo, StreamOne, Update, false) == IgnoreStale),
    assert(decide(StreamOne, StreamOne, Update, false) == IgnoreDuplicate),
    assert(decide(StreamOne, SnapshotOne, Update, false) == RequiresValidation),
    assert(decide(StreamOne, StreamTwo, Update, false) == ReplaceRow),
    assert(decide(StreamOne, StreamTwo, Update, true) == AppendRow),
    assert(decide(StreamOne, StreamTwo, Delete, false) == DeleteRow),
  }

  val insert_witness = lastDecision == InsertRow
  val replace_witness = lastDecision == ReplaceRow
  val delete_witness = lastDecision == DeleteRow
  val stale_witness = lastDecision == IgnoreStale
  val duplicate_witness = lastDecision == IgnoreDuplicate
  val append_only_witness = lastDecision == AppendRow
  val validation_required_witness = lastDecision == RequiresValidation
}
