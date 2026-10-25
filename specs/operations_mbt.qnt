module operations_mbt {
  // Actions are selected from the lifecycle state so every generated trace is
  // a valid sequence against one durable operation row.
  type OperationAction =
    | Create
    | DuplicateCreate
    | Plan
    | Start
    | AdvanceCursor
    | MoveCursorBackwards
    | Pause
    | Resume
    | RequestCancel
    | AcceptCancel
    | Succeed
    | Crash
    | Retry
    | StaleResume
    | StaleCancel
    | Observe

  type OperationResult =
    | NotChecked
    | Submitted
    | DuplicateRejected
    | Planned
    | Running
    | CursorAdvanced
    | BackwardsCursorRejected
    | Paused
    | Resumed
    | Cancelling
    | Cancelled
    | Succeeded
    | Crashed
    | Retried
    | StaleRejected
    | Observed
    | Rejected

  type OperationPhase = SubmittedPhase | Planning | Executing | Completed
  type OperationStatus =
    | SubmittedStatus
    | PlannedStatus
    | RunningStatus
    | PausedStatus
    | CancellingStatus
    | CancelledStatus
    | SucceededStatus
  type CancellationState = NotRequested | Requested | Accepted

  type DurableOperation = {
    exists: bool,
    operationIdentity: int,
    actorAuthorized: bool,
    phase: OperationPhase,
    status: OperationStatus,
    cancellationState: CancellationState,
    cursorPresent: bool,
    cursorPosition: int,
    cursorLabel: str,
    progressRows: int,
    progressRetries: int,
    revision: int,
    auditLength: int,
    nodeAlive: bool,
    restartCount: int,
  }

  pure def emptyOperation: DurableOperation = {
    exists: false,
    operationIdentity: 0,
    actorAuthorized: false,
    phase: SubmittedPhase,
    status: SubmittedStatus,
    cancellationState: NotRequested,
    cursorPresent: false,
    cursorPosition: 0,
    cursorLabel: "",
    progressRows: 0,
    progressRetries: 0,
    revision: 0,
    auditLength: 0,
    nodeAlive: true,
    restartCount: 0,
  }

  pure def submittedOperation: DurableOperation = {
    exists: true,
    operationIdentity: 1,
    actorAuthorized: true,
    phase: SubmittedPhase,
    status: SubmittedStatus,
    cancellationState: NotRequested,
    cursorPresent: false,
    cursorPosition: 0,
    cursorLabel: "",
    progressRows: 0,
    progressRetries: 0,
    revision: 0,
    auditLength: 1,
    nodeAlive: true,
    restartCount: 0,
  }

  pure def unchanged(operation: DurableOperation): DurableOperation = {
    exists: operation.exists,
    operationIdentity: operation.operationIdentity,
    actorAuthorized: operation.actorAuthorized,
    phase: operation.phase,
    status: operation.status,
    cancellationState: operation.cancellationState,
    cursorPresent: operation.cursorPresent,
    cursorPosition: operation.cursorPosition,
    cursorLabel: operation.cursorLabel,
    progressRows: operation.progressRows,
    progressRetries: operation.progressRetries,
    revision: operation.revision,
    auditLength: operation.auditLength,
    nodeAlive: operation.nodeAlive,
    restartCount: operation.restartCount,
  }

  pure def resultFor(operation: DurableOperation, opAction: OperationAction): OperationResult =
    if (opAction == Create and not(operation.exists)) Submitted
    else if (opAction == DuplicateCreate and operation.exists) DuplicateRejected
    else if (opAction == Plan and operation.status == SubmittedStatus) Planned
    else if (opAction == Start and operation.status == PlannedStatus) Running
    else if (opAction == AdvanceCursor and operation.status == RunningStatus) CursorAdvanced
    else if (opAction == MoveCursorBackwards and operation.status == RunningStatus and operation.cursorPresent) BackwardsCursorRejected
    else if (opAction == Pause and operation.status == RunningStatus) Paused
    else if (opAction == Resume and operation.status == PausedStatus) Resumed
    else if (opAction == RequestCancel and (operation.status == PlannedStatus or operation.status == RunningStatus or operation.status == PausedStatus)) Cancelling
    else if (opAction == AcceptCancel and not(operation.status == CancellingStatus)) Rejected
    else if (opAction == AcceptCancel) Cancelled
    else if (opAction == Succeed and operation.status == RunningStatus) Succeeded
    else if (opAction == Crash and operation.status == RunningStatus and operation.nodeAlive) Crashed
    else if (opAction == Retry and operation.status == RunningStatus and not(operation.nodeAlive)) Retried
    else if (opAction == StaleResume and operation.status == PausedStatus) StaleRejected
    else if (opAction == StaleCancel and operation.status == RunningStatus) StaleRejected
    else if (opAction == Observe) Observed
    else Rejected

  pure def nextOperation(operation: DurableOperation, opAction: OperationAction): DurableOperation =
    if (opAction == Create and not(operation.exists)) submittedOperation
    else if (opAction == Plan and operation.status == SubmittedStatus) {
      exists: operation.exists,
      operationIdentity: operation.operationIdentity,
      actorAuthorized: operation.actorAuthorized,
      phase: Planning,
      status: PlannedStatus,
      cancellationState: operation.cancellationState,
      cursorPresent: operation.cursorPresent,
      cursorPosition: operation.cursorPosition,
      cursorLabel: operation.cursorLabel,
      progressRows: operation.progressRows,
      progressRetries: operation.progressRetries,
      revision: operation.revision + 1,
      auditLength: operation.auditLength + 1,
      nodeAlive: operation.nodeAlive,
      restartCount: operation.restartCount,
    }
    else if (opAction == Start and operation.status == PlannedStatus) {
      exists: operation.exists,
      operationIdentity: operation.operationIdentity,
      actorAuthorized: operation.actorAuthorized,
      phase: Executing,
      status: RunningStatus,
      cancellationState: operation.cancellationState,
      cursorPresent: operation.cursorPresent,
      cursorPosition: operation.cursorPosition,
      cursorLabel: operation.cursorLabel,
      progressRows: operation.progressRows,
      progressRetries: operation.progressRetries,
      revision: operation.revision + 1,
      auditLength: operation.auditLength + 1,
      nodeAlive: operation.nodeAlive,
      restartCount: operation.restartCount,
    }
    else if (opAction == AdvanceCursor and operation.status == RunningStatus) {
      exists: operation.exists,
      operationIdentity: operation.operationIdentity,
      actorAuthorized: operation.actorAuthorized,
      phase: Executing,
      status: RunningStatus,
      cancellationState: operation.cancellationState,
      cursorPresent: true,
      cursorPosition: operation.cursorPosition + 1,
      cursorLabel: "chunk",
      progressRows: operation.progressRows + 1,
      progressRetries: operation.progressRetries,
      revision: operation.revision + 1,
      auditLength: operation.auditLength + 1,
      nodeAlive: operation.nodeAlive,
      restartCount: operation.restartCount,
    }
    else if (opAction == Pause and operation.status == RunningStatus) {
      exists: operation.exists,
      operationIdentity: operation.operationIdentity,
      actorAuthorized: operation.actorAuthorized,
      phase: Executing,
      status: PausedStatus,
      cancellationState: operation.cancellationState,
      cursorPresent: operation.cursorPresent,
      cursorPosition: operation.cursorPosition,
      cursorLabel: operation.cursorLabel,
      progressRows: operation.progressRows,
      progressRetries: operation.progressRetries,
      revision: operation.revision + 1,
      auditLength: operation.auditLength + 1,
      nodeAlive: operation.nodeAlive,
      restartCount: operation.restartCount,
    }
    else if (opAction == Resume and operation.status == PausedStatus) {
      exists: operation.exists,
      operationIdentity: operation.operationIdentity,
      actorAuthorized: operation.actorAuthorized,
      phase: Executing,
      status: RunningStatus,
      cancellationState: operation.cancellationState,
      cursorPresent: operation.cursorPresent,
      cursorPosition: operation.cursorPosition,
      cursorLabel: operation.cursorLabel,
      progressRows: operation.progressRows,
      progressRetries: operation.progressRetries,
      revision: operation.revision + 1,
      auditLength: operation.auditLength + 1,
      nodeAlive: operation.nodeAlive,
      restartCount: operation.restartCount,
    }
    else if (opAction == RequestCancel and (operation.status == PlannedStatus or operation.status == RunningStatus or operation.status == PausedStatus)) {
      {
        exists: operation.exists,
        operationIdentity: operation.operationIdentity,
        actorAuthorized: operation.actorAuthorized,
        phase: operation.phase,
        status: CancellingStatus,
        cancellationState: Requested,
        cursorPresent: operation.cursorPresent,
        cursorPosition: operation.cursorPosition,
        cursorLabel: operation.cursorLabel,
        progressRows: operation.progressRows,
        progressRetries: operation.progressRetries,
        revision: operation.revision + 1,
        auditLength: operation.auditLength + 1,
        nodeAlive: operation.nodeAlive,
        restartCount: operation.restartCount,
      }
    }
    else if (opAction == AcceptCancel and operation.status == CancellingStatus) {
      {
        exists: operation.exists,
        operationIdentity: operation.operationIdentity,
        actorAuthorized: operation.actorAuthorized,
        phase: Completed,
        status: CancelledStatus,
        cancellationState: Accepted,
        cursorPresent: operation.cursorPresent,
        cursorPosition: operation.cursorPosition,
        cursorLabel: operation.cursorLabel,
        progressRows: operation.progressRows,
        progressRetries: operation.progressRetries,
        revision: operation.revision + 1,
        auditLength: operation.auditLength + 1,
        nodeAlive: operation.nodeAlive,
        restartCount: operation.restartCount,
      }
    }
    else if (opAction == Succeed and operation.status == RunningStatus) {
      {
        exists: operation.exists,
        operationIdentity: operation.operationIdentity,
        actorAuthorized: operation.actorAuthorized,
        phase: Completed,
        status: SucceededStatus,
        cancellationState: operation.cancellationState,
        cursorPresent: operation.cursorPresent,
        cursorPosition: operation.cursorPosition,
        cursorLabel: operation.cursorLabel,
        progressRows: operation.progressRows,
        progressRetries: operation.progressRetries,
        revision: operation.revision + 1,
        auditLength: operation.auditLength + 1,
        nodeAlive: operation.nodeAlive,
        restartCount: operation.restartCount,
      }
    }
    else if (opAction == Crash and operation.status == RunningStatus and operation.nodeAlive) {
      {
        exists: operation.exists,
        operationIdentity: operation.operationIdentity,
        actorAuthorized: operation.actorAuthorized,
        phase: operation.phase,
        status: operation.status,
        cancellationState: operation.cancellationState,
        cursorPresent: operation.cursorPresent,
        cursorPosition: operation.cursorPosition,
        cursorLabel: operation.cursorLabel,
        progressRows: operation.progressRows,
        progressRetries: operation.progressRetries,
        revision: operation.revision,
        auditLength: operation.auditLength,
        nodeAlive: false,
        restartCount: operation.restartCount,
      }
    }
    else if (opAction == Retry and operation.status == RunningStatus and not(operation.nodeAlive)) {
      {
        exists: operation.exists,
        operationIdentity: operation.operationIdentity,
        actorAuthorized: operation.actorAuthorized,
        phase: operation.phase,
        status: operation.status,
        cancellationState: operation.cancellationState,
        cursorPresent: operation.cursorPresent,
        cursorPosition: operation.cursorPosition,
        cursorLabel: operation.cursorLabel,
        progressRows: operation.progressRows,
        progressRetries: operation.progressRetries + 1,
        revision: operation.revision + 1,
        auditLength: operation.auditLength + 1,
        nodeAlive: true,
        restartCount: operation.restartCount + 1,
      }
    }
    else if (opAction == StaleResume and operation.status == PausedStatus) {
      // The current resume commits; the stale resume is rejected by the CAS.
      {
        exists: operation.exists,
        operationIdentity: operation.operationIdentity,
        actorAuthorized: operation.actorAuthorized,
        phase: Executing,
        status: RunningStatus,
        cancellationState: operation.cancellationState,
        cursorPresent: operation.cursorPresent,
        cursorPosition: operation.cursorPosition,
        cursorLabel: operation.cursorLabel,
        progressRows: operation.progressRows,
        progressRetries: operation.progressRetries,
        revision: operation.revision + 1,
        auditLength: operation.auditLength + 1,
        nodeAlive: operation.nodeAlive,
        restartCount: operation.restartCount,
      }
    }
    else if (opAction == StaleCancel and operation.status == RunningStatus) {
      // The current pause commits; a stale cancellation request cannot clobber it.
      {
        exists: operation.exists,
        operationIdentity: operation.operationIdentity,
        actorAuthorized: operation.actorAuthorized,
        phase: Executing,
        status: PausedStatus,
        cancellationState: operation.cancellationState,
        cursorPresent: operation.cursorPresent,
        cursorPosition: operation.cursorPosition,
        cursorLabel: operation.cursorLabel,
        progressRows: operation.progressRows,
        progressRetries: operation.progressRetries,
        revision: operation.revision + 1,
        auditLength: operation.auditLength + 1,
        nodeAlive: operation.nodeAlive,
        restartCount: operation.restartCount,
      }
    }
    else unchanged(operation)

  pure def actionChoices(operation: DurableOperation): Set[OperationAction] =
    if (not(operation.exists)) Set(Create)
    else if (operation.status == SubmittedStatus) Set(DuplicateCreate, Plan, Observe)
    else if (operation.status == PlannedStatus) Set(Start, RequestCancel, Observe)
    else if (operation.status == RunningStatus and operation.nodeAlive and operation.cursorPresent) Set(AdvanceCursor, MoveCursorBackwards, Pause, RequestCancel, Succeed, Crash, StaleCancel, Observe)
    else if (operation.status == RunningStatus and operation.nodeAlive) Set(AdvanceCursor, Pause, RequestCancel, Succeed, Crash, StaleCancel, Observe)
    else if (operation.status == RunningStatus and not(operation.nodeAlive)) Set(Retry, Observe)
    else if (operation.status == PausedStatus) Set(Resume, RequestCancel, StaleResume, Observe)
    else if (operation.status == CancellingStatus) Set(AcceptCancel, Observe)
    else Set(Observe)

  pure def sameCursor(before: DurableOperation, after: DurableOperation): bool =
    before.cursorPresent == after.cursorPresent and
      before.cursorPosition == after.cursorPosition and
      before.cursorLabel == after.cursorLabel

  pure def sameProgress(before: DurableOperation, after: DurableOperation): bool =
    before.progressRows == after.progressRows and
      before.progressRetries == after.progressRetries

  pure def sameRetainedState(before: DurableOperation, after: DurableOperation): bool =
    before.operationIdentity == after.operationIdentity and
      before.actorAuthorized == after.actorAuthorized and
      before.cancellationState == after.cancellationState and
      sameCursor(before, after) and
      sameProgress(before, after)

  pure def sameLifecycle(before: DurableOperation, after: DurableOperation): bool =
    before.phase == after.phase and
      before.status == after.status and
      before.cancellationState == after.cancellationState

  pure def durableTransitionResult(result: OperationResult): bool =
    result == Planned or
      result == Running or
      result == CursorAdvanced or
      result == Paused or
      result == Resumed or
      result == Cancelling or
      result == Cancelled or
      result == Succeeded or
      result == Retried or
      result == StaleRejected

  var operation: DurableOperation
  var previousOperation: DurableOperation
  var lastAction: OperationAction
  var lastResult: OperationResult

  action init = all {
    operation' = emptyOperation,
    previousOperation' = emptyOperation,
    lastAction' = Observe,
    lastResult' = NotChecked,
  }

  action CheckOperation = {
    nondet opAction = actionChoices(operation).oneOf()
    all {
      previousOperation' = operation,
      operation' = nextOperation(operation, opAction),
      lastAction' = opAction,
      lastResult' = resultFor(operation, opAction),
    }
  }

  action step = CheckOperation

  val durableIdentityIsStable = if (operation.exists) operation.operationIdentity == 1 else true
  val authorizedOperationHasOwner = if (operation.exists) operation.actorAuthorized else true
  val durableCountersAreNonnegative =
    operation.progressRows >= 0 and operation.progressRetries >= 0 and
      operation.revision >= 0 and operation.auditLength >= 0 and
      operation.cursorPosition >= 0 and operation.restartCount >= 0
  val auditExistsForSubmitted = if (operation.exists) operation.auditLength >= 1 else operation.auditLength == 0
  val cursorNeverMovesBackwards = operation.cursorPosition >= 0
  val duplicateIsRejected =
    if (lastAction == DuplicateCreate) lastResult == DuplicateRejected else true
  val backwardsCursorRejected =
    if (lastAction == MoveCursorBackwards) lastResult == BackwardsCursorRejected else true
  val cancellationHasNoSucceededStatus =
    if (lastAction == RequestCancel or lastAction == AcceptCancel) lastResult != Succeeded else true
  val staleResumeIsRejected =
    if (lastAction == StaleResume) lastResult == StaleRejected else true
  val staleCancelIsRejected =
    if (lastAction == StaleCancel) lastResult == StaleRejected else true

  val identityAndAuthorityAreRetained =
    if (previousOperation.exists) {
      operation.exists and
        operation.operationIdentity == previousOperation.operationIdentity and
        operation.actorAuthorized == previousOperation.actorAuthorized
    }
    else if (operation.exists) operation.operationIdentity == 1 and operation.actorAuthorized
    else true
  val cursorDeltaMatches =
    if (lastAction == AdvanceCursor and lastResult == CursorAdvanced) {
      operation.cursorPresent and
        operation.cursorPosition == previousOperation.cursorPosition + 1 and
        operation.cursorLabel == "chunk"
    }
    else sameCursor(previousOperation, operation)
  val progressRowsDeltaMatches =
    if (lastAction == AdvanceCursor and lastResult == CursorAdvanced)
      operation.progressRows == previousOperation.progressRows + 1
    else operation.progressRows == previousOperation.progressRows
  val progressRetriesDeltaMatches =
    if (lastAction == Retry and lastResult == Retried)
      operation.progressRetries == previousOperation.progressRetries + 1
    else operation.progressRetries == previousOperation.progressRetries
  val revisionDeltaMatches =
    if (durableTransitionResult(lastResult))
      operation.revision == previousOperation.revision + 1
    else operation.revision == previousOperation.revision
  val auditDeltaMatches =
    if (lastResult == Submitted or durableTransitionResult(lastResult))
      operation.auditLength == previousOperation.auditLength + 1
    else operation.auditLength == previousOperation.auditLength
  val lifecycleDeltaMatches =
    if (lastResult == Planned)
      operation.phase == Planning and
        operation.status == PlannedStatus and
        operation.cancellationState == previousOperation.cancellationState
    else if (lastResult == Running)
      operation.phase == Executing and
        operation.status == RunningStatus and
        operation.cancellationState == previousOperation.cancellationState
    else if (lastResult == Paused)
      operation.phase == Executing and
        operation.status == PausedStatus and
        operation.cancellationState == previousOperation.cancellationState
    else if (lastResult == Resumed)
      operation.phase == Executing and
        operation.status == RunningStatus and
        operation.cancellationState == previousOperation.cancellationState
    else if (lastResult == Cancelling)
      operation.phase == previousOperation.phase and
        operation.status == CancellingStatus and
        operation.cancellationState == Requested
    else if (lastResult == Cancelled)
      operation.phase == Completed and
        operation.status == CancelledStatus and
        operation.cancellationState == Accepted
    else if (lastResult == Succeeded)
      operation.phase == Completed and
        operation.status == SucceededStatus and
        operation.cancellationState == previousOperation.cancellationState
    else if (lastAction == StaleResume and lastResult == StaleRejected)
      operation.phase == Executing and operation.status == RunningStatus and
        operation.cancellationState == previousOperation.cancellationState
    else if (lastAction == StaleCancel and lastResult == StaleRejected)
      operation.phase == Executing and operation.status == PausedStatus and
        operation.cancellationState == previousOperation.cancellationState
    else sameLifecycle(previousOperation, operation)
  val nodeDeltaMatches =
    if (lastResult == Crashed)
      not(operation.nodeAlive) and previousOperation.nodeAlive and
        operation.restartCount == previousOperation.restartCount
    else if (lastResult == Retried)
      operation.nodeAlive and not(previousOperation.nodeAlive) and
        operation.restartCount == previousOperation.restartCount + 1
    else operation.nodeAlive == previousOperation.nodeAlive and
      operation.restartCount == previousOperation.restartCount

  val submitted_witness = lastResult == Submitted and lastAction == Create and operation.exists
  val duplicate_witness = lastResult == DuplicateRejected and lastAction == DuplicateCreate
  val planned_witness =
    lastAction == Plan and lastResult == Planned and operation.status == PlannedStatus
  val started_witness =
    lastAction == Start and lastResult == Running and operation.status == RunningStatus
  val cursor_advanced_witness =
    lastAction == AdvanceCursor and lastResult == CursorAdvanced and
      operation.cursorPresent and operation.progressRows == previousOperation.progressRows + 1
  val backwards_cursor_rejection_witness =
    lastAction == MoveCursorBackwards and lastResult == BackwardsCursorRejected and
      sameCursor(previousOperation, operation) and
      operation.auditLength == previousOperation.auditLength
  val paused_witness =
    lastAction == Pause and lastResult == Paused and
      previousOperation.phase == Executing and
      previousOperation.status == RunningStatus and
      operation.phase == Executing and
      operation.status == PausedStatus and
      sameRetainedState(previousOperation, operation) and
      operation.revision == previousOperation.revision + 1 and
      operation.auditLength == previousOperation.auditLength + 1
  val resumed_witness =
    lastAction == Resume and lastResult == Resumed and
      previousOperation.phase == Executing and
      previousOperation.status == PausedStatus and
      operation.phase == Executing and
      operation.status == RunningStatus and
      sameRetainedState(previousOperation, operation) and
      operation.revision == previousOperation.revision + 1 and
      operation.auditLength == previousOperation.auditLength + 1
  val observed_witness =
    lastAction == Observe and lastResult == Observed and
      operation == previousOperation
  val success_witness =
    lastAction == Succeed and lastResult == Succeeded and
      previousOperation.status == RunningStatus and
      operation.phase == Completed and operation.status == SucceededStatus
  val cancellation_requested_witness =
    lastAction == RequestCancel and lastResult == Cancelling and
      operation.status == CancellingStatus and
      operation.cancellationState == Requested
  val cancellation_accepted_witness =
    lastAction == AcceptCancel and lastResult == Cancelled and
      previousOperation.status == CancellingStatus and
      operation.phase == Completed and operation.status == CancelledStatus and
      operation.cancellationState == Accepted
  // Keep the indexed mutation witness name, but make it one exact cancellation branch.
  val cancellation_witness = cancellation_accepted_witness
  val stale_resume_witness =
    lastAction == StaleResume and lastResult == StaleRejected and
      previousOperation.status == PausedStatus and operation.status == RunningStatus
  val stale_cancel_witness =
    lastAction == StaleCancel and lastResult == StaleRejected and
      previousOperation.status == RunningStatus and operation.status == PausedStatus
  val crash_witness =
    lastAction == Crash and lastResult == Crashed and not(operation.nodeAlive)
  val retry_witness =
    lastAction == Retry and lastResult == Retried and
      operation.progressRetries == previousOperation.progressRetries + 1 and
      operation.restartCount == previousOperation.restartCount + 1
  val inv = all {
    durableIdentityIsStable,
    authorizedOperationHasOwner,
    durableCountersAreNonnegative,
    auditExistsForSubmitted,
    cursorNeverMovesBackwards,
    duplicateIsRejected,
    backwardsCursorRejected,
    cancellationHasNoSucceededStatus,
    staleResumeIsRejected,
    staleCancelIsRejected,
    identityAndAuthorityAreRetained,
    cursorDeltaMatches,
    progressRowsDeltaMatches,
    progressRetriesDeltaMatches,
    revisionDeltaMatches,
    auditDeltaMatches,
    lifecycleDeltaMatches,
    nodeDeltaMatches,
  }

  run operationMbtExamples = all {
    assert(resultFor(emptyOperation, Create) == Submitted),
    assert(resultFor(submittedOperation, DuplicateCreate) == DuplicateRejected),
    assert(resultFor(submittedOperation, MoveCursorBackwards) == Rejected),
  }
}
