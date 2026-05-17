module operations_bughunt {
  type Status = Submitted | Running | Paused | Cancelling | Cancelled | Failed | Succeeded
  type Node = Alive | Dead

  var status: Status
  var node: Node
  var durableCursor: int
  var transientCursor: int
  var auditLength: int
  var destinationWrites: int
  var cancelAcceptedAtWrites: int
  var duplicateSubmissions: int
  var failedRequiredPhase: bool
  var auditFailed: bool

  action init = all {
    status' = Submitted,
    node' = Alive,
    durableCursor' = 0,
    transientCursor' = 0,
    auditLength' = 1,
    destinationWrites' = 0,
    cancelAcceptedAtWrites' = 0,
    duplicateSubmissions' = 0,
    failedRequiredPhase' = false,
    auditFailed' = false,
  }

  action Start = all {
    node == Alive,
    status == Submitted or status == Paused,
    status' = Running,
    node' = node,
    durableCursor' = durableCursor,
    transientCursor' = durableCursor,
    auditLength' = auditLength + 1,
    destinationWrites' = destinationWrites,
    cancelAcceptedAtWrites' = cancelAcceptedAtWrites,
    duplicateSubmissions' = duplicateSubmissions,
    failedRequiredPhase' = failedRequiredPhase,
    auditFailed' = auditFailed,
  }

  action WriteAndCommit = all {
    node == Alive,
    status == Running,
    status' = status,
    node' = node,
    transientCursor' = transientCursor + 1,
    durableCursor' = transientCursor + 1,
    auditLength' = auditLength + 1,
    destinationWrites' = destinationWrites + 1,
    cancelAcceptedAtWrites' = cancelAcceptedAtWrites,
    duplicateSubmissions' = duplicateSubmissions,
    failedRequiredPhase' = failedRequiredPhase,
    auditFailed' = auditFailed,
  }

  action CrashBeforeCommit = all {
    node == Alive,
    status == Running,
    status' = status,
    node' = Dead,
    durableCursor' = durableCursor,
    transientCursor' = transientCursor + 1,
    auditLength' = auditLength,
    destinationWrites' = destinationWrites,
    cancelAcceptedAtWrites' = cancelAcceptedAtWrites,
    duplicateSubmissions' = duplicateSubmissions,
    failedRequiredPhase' = failedRequiredPhase,
    auditFailed' = auditFailed,
  }

  action Resume = all {
    node == Dead,
    status == Running,
    status' = Running,
    node' = Alive,
    durableCursor' = durableCursor,
    transientCursor' = durableCursor,
    auditLength' = auditLength + 1,
    destinationWrites' = destinationWrites,
    cancelAcceptedAtWrites' = cancelAcceptedAtWrites,
    duplicateSubmissions' = duplicateSubmissions,
    failedRequiredPhase' = failedRequiredPhase,
    auditFailed' = auditFailed,
  }

  action DuplicateSubmission = all {
    status' = status,
    node' = node,
    durableCursor' = durableCursor,
    transientCursor' = transientCursor,
    auditLength' = auditLength,
    destinationWrites' = destinationWrites,
    cancelAcceptedAtWrites' = cancelAcceptedAtWrites,
    duplicateSubmissions' = duplicateSubmissions + 1,
    failedRequiredPhase' = failedRequiredPhase,
    auditFailed' = auditFailed,
  }

  action RequestCancel = all {
    node == Alive,
    status == Running or status == Paused,
    status' = Cancelling,
    node' = node,
    durableCursor' = durableCursor,
    transientCursor' = transientCursor,
    auditLength' = auditLength + 1,
    destinationWrites' = destinationWrites,
    cancelAcceptedAtWrites' = cancelAcceptedAtWrites,
    duplicateSubmissions' = duplicateSubmissions,
    failedRequiredPhase' = failedRequiredPhase,
    auditFailed' = auditFailed,
  }

  action AcceptCancel = all {
    node == Alive,
    status == Cancelling,
    status' = Cancelled,
    node' = node,
    durableCursor' = durableCursor,
    transientCursor' = transientCursor,
    auditLength' = auditLength + 1,
    destinationWrites' = destinationWrites,
    cancelAcceptedAtWrites' = destinationWrites,
    duplicateSubmissions' = duplicateSubmissions,
    failedRequiredPhase' = failedRequiredPhase,
    auditFailed' = auditFailed,
  }

  action AuditWriteFailure = all {
    node == Alive,
    status != Succeeded,
    status' = Failed,
    node' = node,
    durableCursor' = durableCursor,
    transientCursor' = transientCursor,
    auditLength' = auditLength,
    destinationWrites' = destinationWrites,
    cancelAcceptedAtWrites' = cancelAcceptedAtWrites,
    duplicateSubmissions' = duplicateSubmissions,
    failedRequiredPhase' = true,
    auditFailed' = true,
  }

  action Succeed = all {
    node == Alive,
    status == Running,
    failedRequiredPhase == false,
    status' = Succeeded,
    node' = node,
    durableCursor' = durableCursor,
    transientCursor' = transientCursor,
    auditLength' = auditLength + 1,
    destinationWrites' = destinationWrites,
    cancelAcceptedAtWrites' = cancelAcceptedAtWrites,
    duplicateSubmissions' = duplicateSubmissions,
    failedRequiredPhase' = failedRequiredPhase,
    auditFailed' = auditFailed,
  }

  action step = any {
    Start,
    WriteAndCommit,
    CrashBeforeCommit,
    Resume,
    DuplicateSubmission,
    RequestCancel,
    AcceptCancel,
    AuditWriteFailure,
    Succeed,
  }

  val noSuccessAfterRequiredFailure = not(status == Succeeded and failedRequiredPhase)
  val durableCursorNeverExceedsCommittedWrites = durableCursor <= destinationWrites
  val resumeDropsTransientUncommittedWork = if (node == Alive) transientCursor >= durableCursor else true
  val cancelledWritesStop = if (status == Cancelled) destinationWrites == cancelAcceptedAtWrites else true
  val duplicateSubmissionDoesNotAppendAudit = if (duplicateSubmissions > 0) auditLength >= 1 else true
  val inv = all {
    noSuccessAfterRequiredFailure,
    durableCursorNeverExceedsCommittedWrites,
    resumeDropsTransientUncommittedWork,
    cancelledWritesStop,
    duplicateSubmissionDoesNotAppendAudit,
  }

  run crashResumeExamples = all {
    assert(0 <= durableCursor),
    assert(destinationWrites >= durableCursor),
  }

  val crash_witness = node == Dead
  val retry_witness = node == Alive and durableCursor > 0
  val duplicate_submission_witness = duplicateSubmissions > 0
  val cancellation_witness = status == Cancelled
  val audit_failure_witness = auditFailed and status == Failed
  val resume_witness = node == Alive and status == Running and transientCursor == durableCursor
}
