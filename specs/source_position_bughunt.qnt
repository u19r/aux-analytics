module source_position_bughunt {
  type Position = None | StreamOld | StreamNew | Snapshot | Backup
  type Decision = Write | Delete | Ignore | Append | Validate

  var current: Position
  var rowVisible: bool
  var appendOnly: bool
  var validationRequired: bool
  var staleOverwriteCount: int
  var inPlaceAppendOnlyUpdates: int
  var deleteCount: int

  pure def comparable(left: Position, right: Position): bool =
    left != None and right != None and (
      (left == StreamOld and right == StreamNew) or
      (left == StreamNew and right == StreamOld) or
      left == right
    )

  pure def incomingIsNewer(incoming: Position, existing: Position): bool =
    incoming == StreamNew and existing == StreamOld

  pure def incomingIsOlder(incoming: Position, existing: Position): bool =
    incoming == StreamOld and existing == StreamNew

  action init = all {
    current' = None,
    rowVisible' = false,
    appendOnly' = false,
    validationRequired' = false,
    staleOverwriteCount' = 0,
    inPlaceAppendOnlyUpdates' = 0,
    deleteCount' = 0,
  }

  action ReceiveUpsert = {
    nondet incoming = Set(StreamOld, StreamNew, Snapshot, Backup).oneOf()
    val shouldValidate = current != None and not(comparable(incoming, current))
    val shouldIgnore = current != None and incomingIsOlder(incoming, current)
    val shouldWrite = current == None or incomingIsNewer(incoming, current)
    all {
      current' = if (shouldWrite and not(appendOnly)) incoming else current,
      rowVisible' = if (shouldWrite or appendOnly) true else rowVisible,
      appendOnly' = appendOnly,
      validationRequired' = validationRequired or shouldValidate,
      staleOverwriteCount' = staleOverwriteCount + (if (shouldIgnore and shouldWrite) 1 else 0),
      inPlaceAppendOnlyUpdates' = inPlaceAppendOnlyUpdates,
      deleteCount' = deleteCount,
    }
  }

  action ReceiveDelete = {
    nondet incoming = Set(StreamOld, StreamNew, Snapshot, Backup).oneOf()
    val canDelete = current == None or comparable(incoming, current) and not(incomingIsOlder(incoming, current))
    all {
      current' = if (canDelete) incoming else current,
      rowVisible' = if (canDelete) false else rowVisible,
      appendOnly' = appendOnly,
      validationRequired' = validationRequired or (current != None and not(comparable(incoming, current))),
      staleOverwriteCount' = staleOverwriteCount,
      inPlaceAppendOnlyUpdates' = inPlaceAppendOnlyUpdates,
      deleteCount' = deleteCount + (if (canDelete) 1 else 0),
    }
  }

  action SetAppendOnly = all {
    appendOnly' = true,
    current' = current,
    rowVisible' = rowVisible,
    validationRequired' = validationRequired,
    staleOverwriteCount' = staleOverwriteCount,
    inPlaceAppendOnlyUpdates' = inPlaceAppendOnlyUpdates,
    deleteCount' = deleteCount,
  }

  action step = any {
    ReceiveUpsert,
    ReceiveDelete,
    SetAppendOnly,
  }

  val newerStateNeverOverwrittenByOlder = staleOverwriteCount == 0
  val appendOnlyNeverUpdatesInPlace = inPlaceAppendOnlyUpdates == 0
  val deleteCountBounded = deleteCount <= 8
  val inv = all {
    newerStateNeverOverwrittenByOlder,
    appendOnlyNeverUpdatesInPlace,
    deleteCountBounded,
  }

  run sourcePositionBughuntExamples = all {
    assert(incomingIsNewer(StreamNew, StreamOld)),
    assert(incomingIsOlder(StreamOld, StreamNew)),
    assert(comparable(Snapshot, StreamNew) == false),
  }

  val stale_snapshot_witness = validationRequired and current == StreamNew
  val duplicate_event_witness = current == StreamOld or current == StreamNew
  val delete_witness = rowVisible == false and deleteCount > 0
  val append_only_witness = appendOnly
  val incomparable_witness = validationRequired
}
