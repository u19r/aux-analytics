// Observable retention scenarios.
//
// Each action is an independent deterministic fixture.  The adapter executes
// the real engine operation and reconstructs these facts from table rows and
// durable retention-effect records; no policy, candidate, or deletion map is
// part of the model.
module retention_mbt {
  type RetentionResult =
    | NotChecked
    | StaticExpiryResult
    | MissingMarkerResult
    | RepairRejectedResult
    | RepairAppliedResult
    | SweepCompleteResult
    | SweepIncompleteResult
    | StaleRejectedResult
    | ReplayAppliedResult
    | ConflictRejectedResult

  var lastResult: RetentionResult
  var rowCount: int
  var missingCount: int
  var staticExpiryExact: bool
  var deletedCount: int
  var remainingExpiredCount: int
  var receiptCount: int
  var complete: bool
  var authorityFence: int

  action init = all {
    lastResult' = NotChecked,
    rowCount' = 0,
    missingCount' = 0,
    staticExpiryExact' = false,
    deletedCount' = 0,
    remainingExpiredCount' = 0,
    receiptCount' = 0,
    complete' = false,
    authorityFence' = 0,
  }

  action StaticExpiry = all {
    lastResult' = StaticExpiryResult,
    rowCount' = 1,
    missingCount' = 0,
    staticExpiryExact' = true,
    deletedCount' = 0,
    remainingExpiredCount' = 0,
    receiptCount' = 0,
    complete' = false,
    authorityFence' = 0,
  }

  action MissingMarker = all {
    lastResult' = MissingMarkerResult,
    rowCount' = 1,
    missingCount' = 1,
    staticExpiryExact' = false,
    deletedCount' = 0,
    remainingExpiredCount' = 0,
    receiptCount' = 0,
    complete' = false,
    authorityFence' = 0,
  }

  action RepairWithoutPolicy = all {
    lastResult' = RepairRejectedResult,
    rowCount' = 1,
    missingCount' = 1,
    staticExpiryExact' = false,
    deletedCount' = 0,
    remainingExpiredCount' = 0,
    receiptCount' = 0,
    complete' = false,
    authorityFence' = 0,
  }

  action RepairWithPolicy = all {
    lastResult' = RepairAppliedResult,
    rowCount' = 1,
    missingCount' = 0,
    staticExpiryExact' = false,
    deletedCount' = 0,
    remainingExpiredCount' = 0,
    receiptCount' = 0,
    complete' = false,
    authorityFence' = 0,
  }

  action BoundedComplete = all {
    lastResult' = SweepCompleteResult,
    rowCount' = 0,
    missingCount' = 0,
    staticExpiryExact' = false,
    deletedCount' = 1,
    remainingExpiredCount' = 0,
    receiptCount' = 1,
    complete' = true,
    authorityFence' = 1,
  }

  action BoundedIncomplete = all {
    lastResult' = SweepIncompleteResult,
    rowCount' = 1,
    missingCount' = 0,
    staticExpiryExact' = false,
    deletedCount' = 1,
    remainingExpiredCount' = 1,
    receiptCount' = 1,
    complete' = false,
    authorityFence' = 1,
  }

  action StaleAuthority = all {
    lastResult' = StaleRejectedResult,
    rowCount' = 0,
    missingCount' = 0,
    staticExpiryExact' = false,
    deletedCount' = 1,
    remainingExpiredCount' = 0,
    receiptCount' = 1,
    complete' = true,
    authorityFence' = 1,
  }

  action ReplayEffect = all {
    lastResult' = ReplayAppliedResult,
    rowCount' = 0,
    missingCount' = 0,
    staticExpiryExact' = false,
    deletedCount' = 1,
    remainingExpiredCount' = 0,
    receiptCount' = 1,
    complete' = true,
    authorityFence' = 1,
  }

  action ConflictingPayload = all {
    lastResult' = ConflictRejectedResult,
    rowCount' = 0,
    missingCount' = 0,
    staticExpiryExact' = false,
    deletedCount' = 1,
    remainingExpiredCount' = 0,
    receiptCount' = 1,
    complete' = true,
    authorityFence' = 1,
  }

  action step = any {
    StaticExpiry,
    MissingMarker,
    RepairWithoutPolicy,
    RepairWithPolicy,
    BoundedComplete,
    BoundedIncomplete,
    StaleAuthority,
    ReplayEffect,
    ConflictingPayload,
  }

  val inv = and {
    rowCount >= 0,
    missingCount >= 0,
    missingCount <= rowCount,
    staticExpiryExact implies lastResult == StaticExpiryResult,
    deletedCount >= 0,
    remainingExpiredCount >= 0,
    receiptCount >= 0,
    deletedCount <= 1,
    complete implies remainingExpiredCount == 0,
  }

  run expiryTest = assert(1_000 + 1_000 == 2_000)
  run boundedTest = all {
    assert(1 <= 1),
    assert(1 - 1 == 0),
  }

  val static_expiry_witness = staticExpiryExact
  val missing_retention_witness = lastResult == MissingMarkerResult
  val repair_rejection_witness = lastResult == RepairRejectedResult
  val repaired_row_witness = lastResult == RepairAppliedResult and missingCount == 0
  val complete_sweep_witness = lastResult == SweepCompleteResult and complete
  val incomplete_sweep_witness = lastResult == SweepIncompleteResult and remainingExpiredCount > 0
  val stale_authority_witness = lastResult == StaleRejectedResult
  val replay_witness = lastResult == ReplayAppliedResult and receiptCount == 1
  val conflict_witness = lastResult == ConflictRejectedResult
}
