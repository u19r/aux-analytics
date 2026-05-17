module operations_mbt {
  type OperationAction =
    | Create
    | DuplicateCreate
    | Start
    | AdvanceCursor
    | MoveCursorBackwards
    | RequestCancel
    | AcceptCancel
    | Succeed
  type OperationResult =
    | NotChecked
    | Submitted
    | DuplicateRejected
    | Running
    | CursorAdvanced
    | BackwardsCursorRejected
    | Cancelling
    | Cancelled
    | Succeeded

  var lastAction: OperationAction
  var lastResult: OperationResult

  pure def expected(opAction: OperationAction): OperationResult =
    if (opAction == Create) Submitted
    else if (opAction == DuplicateCreate) DuplicateRejected
    else if (opAction == Start) Running
    else if (opAction == AdvanceCursor) CursorAdvanced
    else if (opAction == MoveCursorBackwards) BackwardsCursorRejected
    else if (opAction == RequestCancel) Cancelling
    else if (opAction == AcceptCancel) Cancelled
    else Succeeded

  action init = all {
    lastAction' = Create,
    lastResult' = NotChecked,
  }

  action CheckOperation = {
    nondet opAction = Set(
      Create,
      DuplicateCreate,
      Start,
      AdvanceCursor,
      MoveCursorBackwards,
      RequestCancel,
      AcceptCancel,
      Succeed,
    ).oneOf()
    all {
      lastAction' = opAction,
      lastResult' = expected(opAction),
    }
  }

  action step = CheckOperation

  val duplicateIsRejected =
    if (lastAction == DuplicateCreate) lastResult == DuplicateRejected else true
  val backwardsCursorRejected =
    if (lastAction == MoveCursorBackwards) lastResult == BackwardsCursorRejected else true
  val cancellationHasNoSucceededStatus =
    if (lastAction == RequestCancel or lastAction == AcceptCancel) lastResult != Succeeded else true
  val inv = all {
    duplicateIsRejected,
    backwardsCursorRejected,
    cancellationHasNoSucceededStatus,
  }

  run operationMbtExamples = all {
    assert(expected(Create) == Submitted),
    assert(expected(DuplicateCreate) == DuplicateRejected),
    assert(expected(MoveCursorBackwards) == BackwardsCursorRejected),
  }
}
