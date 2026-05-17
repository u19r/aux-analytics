module operations {
  type OperationStatus =
    | Submitted
    | Planned
    | Running
    | Paused
    | Cancelling
    | Cancelled
    | Failed
    | Succeeded

  type OperationEvent =
    | SubmittedEvent
    | PlannedEvent
    | StartedEvent
    | PausedEvent
    | ResumedEvent
    | CursorAdvancedEvent
    | CancellationRequestedEvent
    | CancellationAcceptedEvent
    | FailedEvent
    | SucceededEvent

  var status: OperationStatus
  var cursor: int
  var auditLength: int
  var failedRequiredPhase: bool
  var destinationWritesAfterCancel: int
  var duplicateRejected: bool
  var auditWriteFailed: bool

  pure def appendAudit(eventCount: int): int = eventCount + 1
  pure def cursorDoesNotMoveBackwards(current: int, candidate: int): bool = candidate >= current
  pure def mayWriteDestination(status: OperationStatus): bool =
    status != Cancelled and status != Cancelling
  pure def canSucceed(status: OperationStatus, failedPhase: bool): bool =
    status == Running and failedPhase == false

  action init = all {
    status' = Submitted,
    cursor' = 0,
    auditLength' = 1,
    failedRequiredPhase' = false,
    destinationWritesAfterCancel' = 0,
    duplicateRejected' = false,
    auditWriteFailed' = false,
  }

  action Plan = all {
    status == Submitted,
    status' = Planned,
    cursor' = cursor,
    auditLength' = appendAudit(auditLength),
    failedRequiredPhase' = failedRequiredPhase,
    destinationWritesAfterCancel' = destinationWritesAfterCancel,
    duplicateRejected' = duplicateRejected,
    auditWriteFailed' = auditWriteFailed,
  }

  action Start = all {
    status == Planned or status == Paused,
    status' = Running,
    cursor' = cursor,
    auditLength' = appendAudit(auditLength),
    failedRequiredPhase' = failedRequiredPhase,
    destinationWritesAfterCancel' = destinationWritesAfterCancel,
    duplicateRejected' = duplicateRejected,
    auditWriteFailed' = auditWriteFailed,
  }

  action AdvanceCursor = all {
    status == Running,
    cursorDoesNotMoveBackwards(cursor, cursor + 1),
    status' = status,
    cursor' = cursor + 1,
    auditLength' = appendAudit(auditLength),
    failedRequiredPhase' = failedRequiredPhase,
    destinationWritesAfterCancel' = destinationWritesAfterCancel,
    duplicateRejected' = duplicateRejected,
    auditWriteFailed' = auditWriteFailed,
  }

  action Pause = all {
    status == Running,
    status' = Paused,
    cursor' = cursor,
    auditLength' = appendAudit(auditLength),
    failedRequiredPhase' = failedRequiredPhase,
    destinationWritesAfterCancel' = destinationWritesAfterCancel,
    duplicateRejected' = duplicateRejected,
    auditWriteFailed' = auditWriteFailed,
  }

  action RequestCancel = all {
    status == Running or status == Paused or status == Planned,
    status' = Cancelling,
    cursor' = cursor,
    auditLength' = appendAudit(auditLength),
    failedRequiredPhase' = failedRequiredPhase,
    destinationWritesAfterCancel' = destinationWritesAfterCancel,
    duplicateRejected' = duplicateRejected,
    auditWriteFailed' = auditWriteFailed,
  }

  action AcceptCancel = all {
    status == Cancelling,
    status' = Cancelled,
    cursor' = cursor,
    auditLength' = appendAudit(auditLength),
    failedRequiredPhase' = failedRequiredPhase,
    destinationWritesAfterCancel' = destinationWritesAfterCancel,
    duplicateRejected' = duplicateRejected,
    auditWriteFailed' = auditWriteFailed,
  }

  action FailRequiredPhase = all {
    status == Running,
    status' = Failed,
    cursor' = cursor,
    auditLength' = appendAudit(auditLength),
    failedRequiredPhase' = true,
    destinationWritesAfterCancel' = destinationWritesAfterCancel,
    duplicateRejected' = duplicateRejected,
    auditWriteFailed' = auditWriteFailed,
  }

  action Succeed = all {
    canSucceed(status, failedRequiredPhase),
    status' = Succeeded,
    cursor' = cursor,
    auditLength' = appendAudit(auditLength),
    failedRequiredPhase' = failedRequiredPhase,
    destinationWritesAfterCancel' = destinationWritesAfterCancel,
    duplicateRejected' = duplicateRejected,
    auditWriteFailed' = auditWriteFailed,
  }

  action RejectDuplicate = all {
    status' = status,
    cursor' = cursor,
    auditLength' = auditLength,
    failedRequiredPhase' = failedRequiredPhase,
    destinationWritesAfterCancel' = destinationWritesAfterCancel,
    duplicateRejected' = true,
    auditWriteFailed' = auditWriteFailed,
  }

  action AuditWriteFailure = all {
    status == Submitted,
    status' = Failed,
    cursor' = cursor,
    auditLength' = auditLength,
    failedRequiredPhase' = true,
    destinationWritesAfterCancel' = destinationWritesAfterCancel,
    duplicateRejected' = duplicateRejected,
    auditWriteFailed' = true,
  }

  action step = any {
    Plan,
    Start,
    AdvanceCursor,
    Pause,
    RequestCancel,
    AcceptCancel,
    FailRequiredPhase,
    Succeed,
    RejectDuplicate,
    AuditWriteFailure,
  }

  val noSuccessWithFailedRequiredPhase = not(status == Succeeded and failedRequiredPhase)
  val auditAppendOnly = auditLength >= 1
  val cancelledWritesNoDestinationData =
    if (status == Cancelled) destinationWritesAfterCancel == 0 else true
  val cursorDurableForward = cursor >= 0
  val inv = all {
    noSuccessWithFailedRequiredPhase,
    auditAppendOnly,
    cancelledWritesNoDestinationData,
    cursorDurableForward,
  }

  run operationTransitionExamples = all {
    assert(cursorDoesNotMoveBackwards(1, 1)),
    assert(cursorDoesNotMoveBackwards(1, 2)),
    assert(cursorDoesNotMoveBackwards(2, 1) == false),
    assert(mayWriteDestination(Running)),
    assert(mayWriteDestination(Cancelling) == false),
    assert(canSucceed(Running, false)),
    assert(canSucceed(Running, true) == false),
  }

  val success_witness = status == Succeeded
  val failure_witness = status == Failed
  val cancellation_witness = status == Cancelled
  val resume_after_crash_witness = status == Running and cursor > 0
  val paused_operation_witness = status == Paused
  val rejected_duplicate_witness = duplicateRejected
  val audit_write_failure_witness = auditWriteFailed and status == Failed
}
