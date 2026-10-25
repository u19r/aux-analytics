// Compact conformance model for the source-position merge reducer.
// A driver supplies existing/incoming source positions, mutation kind, and
// append-only policy; expected is the single canonical decision reducer.
// Comparable stream positions model ordered time, equal positions are
// duplicates, stale positions are ignored, and incomparable source kinds
// require validation. The compared state contains the decision inputs and
// public merge result; validation/storage details remain owner-local.
module source_position_mbt {
  type ExistingPosition = NoExisting | StreamOld | StreamNew | Snapshot
  type IncomingPosition = IncomingStreamOld | IncomingStreamNew | IncomingSnapshot
  type Mutation = Upsert | Delete
  type MergeResult =
    | NotChecked
    | InsertRow
    | ReplaceRow
    | DeleteRow
    | IgnoreDuplicate
    | IgnoreStale
    | AppendRow
    | RequiresValidation

  var existingPosition: ExistingPosition
  var incomingPosition: IncomingPosition
  var mutation: Mutation
  var appendOnly: bool
  var lastResult: MergeResult

  pure def comparable(existing: ExistingPosition, incoming: IncomingPosition): bool =
    if (existing == NoExisting) true
    else if (existing == Snapshot and incoming == IncomingSnapshot) true
    else if (existing == StreamOld and incoming != IncomingSnapshot) true
    else if (existing == StreamNew and incoming != IncomingSnapshot) true
    else false

  pure def isDuplicate(existing: ExistingPosition, incoming: IncomingPosition): bool =
    (existing == StreamOld and incoming == IncomingStreamOld)
      or (existing == StreamNew and incoming == IncomingStreamNew)
      or (existing == Snapshot and incoming == IncomingSnapshot)

  pure def isStale(existing: ExistingPosition, incoming: IncomingPosition): bool =
    existing == StreamNew and incoming == IncomingStreamOld

  pure def expected(
    existing: ExistingPosition,
    incoming: IncomingPosition,
    change: Mutation,
    isAppendOnly: bool,
  ): MergeResult =
    if (existing == NoExisting) {
      if (isAppendOnly) AppendRow else if (change == Delete) DeleteRow else InsertRow
    } else if (not(comparable(existing, incoming))) RequiresValidation
    else if (isDuplicate(existing, incoming)) IgnoreDuplicate
    else if (isStale(existing, incoming)) IgnoreStale
    else if (isAppendOnly) AppendRow
    else if (change == Delete) DeleteRow
    else ReplaceRow

  action init = all {
    existingPosition' = NoExisting,
    incomingPosition' = IncomingStreamOld,
    mutation' = Upsert,
    appendOnly' = false,
    lastResult' = NotChecked,
  }

  action CheckMerge = {
    nondet existing = Set(NoExisting, StreamOld, StreamNew, Snapshot).oneOf()
    nondet incoming = Set(IncomingStreamOld, IncomingStreamNew, IncomingSnapshot).oneOf()
    nondet change = Set(Upsert, Delete).oneOf()
    nondet isAppendOnly = Set(true, false).oneOf()
    all {
      existingPosition' = existing,
      incomingPosition' = incoming,
      mutation' = change,
      appendOnly' = isAppendOnly,
      lastResult' = expected(existing, incoming, change, isAppendOnly),
    }
  }

  action step = CheckMerge

  val staleNeverWrites =
    if (existingPosition == StreamNew and incomingPosition == IncomingStreamOld) {
      lastResult == IgnoreStale
    } else true
  val appendOnlyNeverReplaces =
    if (appendOnly) lastResult != ReplaceRow else true
  val incomparableRequiresValidation =
    if (existingPosition != NoExisting and not(comparable(existingPosition, incomingPosition))) {
      lastResult == RequiresValidation
    } else true
  val resultMatchesExpected =
    lastResult == NotChecked or
      lastResult == expected(existingPosition, incomingPosition, mutation, appendOnly)
  val inv = all {
    staleNeverWrites,
    appendOnlyNeverReplaces,
    incomparableRequiresValidation,
    resultMatchesExpected,
  }

  // Each decision branch is reachable from CheckMerge and is compared by the
  // Rust adapter as part of the complete post-action state.
  val insert_witness = lastResult == InsertRow
  val replace_witness = lastResult == ReplaceRow
  val delete_witness = lastResult == DeleteRow
  val duplicate_witness = lastResult == IgnoreDuplicate
  val stale_witness = lastResult == IgnoreStale
  val append_witness = lastResult == AppendRow
  val validation_witness = lastResult == RequiresValidation

  run sourcePositionMbtExamples = all {
    assert(expected(NoExisting, IncomingStreamOld, Upsert, false) == InsertRow),
    assert(expected(StreamNew, IncomingStreamOld, Upsert, false) == IgnoreStale),
    assert(expected(StreamOld, IncomingStreamOld, Upsert, false) == IgnoreDuplicate),
    assert(expected(StreamOld, IncomingSnapshot, Upsert, false) == RequiresValidation),
    assert(expected(StreamOld, IncomingStreamNew, Upsert, true) == AppendRow),
  }
}
