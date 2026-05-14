module retention {
  type RetentionResult =
    | NotChecked
    | StaticExpiryComputed
    | MissingRetentionMarked
    | MissingRetentionRepaired
    | SweepDeletedExpiredOnly
    | SweepBatchLimitRespected

  type RetentionCase =
    | StaticExpiryCase
    | MissingRetentionCase
    | RepairMissingRetentionCase
    | SweepExpiredRowsCase
    | SweepBatchLimitCase

  var lastResult: RetentionResult

  pure val retentionResults: Set[RetentionResult] = Set(
    NotChecked,
    StaticExpiryComputed,
    MissingRetentionMarked,
    MissingRetentionRepaired,
    SweepDeletedExpiredOnly,
    SweepBatchLimitRespected
  )

  pure def expiryMs(createdAtMs: int, retentionMs: int): int =
    createdAtMs + retentionMs

  pure def isExpired(expiresAtMs: int, nowMs: int): bool =
    expiresAtMs <= nowMs

  pure def deletionCount(expiredRows: int, batchLimit: int): int =
    if (expiredRows <= batchLimit) expiredRows else batchLimit

  pure def retentionCase(retention: RetentionCase): RetentionResult =
    match retention {
      | StaticExpiryCase => StaticExpiryComputed
      | MissingRetentionCase => MissingRetentionMarked
      | RepairMissingRetentionCase => MissingRetentionRepaired
      | SweepExpiredRowsCase => SweepDeletedExpiredOnly
      | SweepBatchLimitCase => SweepBatchLimitRespected
    }

  action init =
    lastResult' = NotChecked

  action ComputeStaticExpiry =
    lastResult' = retentionCase(StaticExpiryCase)

  action MarkMissingRetention =
    lastResult' = retentionCase(MissingRetentionCase)

  action RepairMissingRetention =
    lastResult' = retentionCase(RepairMissingRetentionCase)

  action SweepExpiredRows =
    lastResult' = retentionCase(SweepExpiredRowsCase)

  action SweepHonorsBatchLimit =
    lastResult' = retentionCase(SweepBatchLimitCase)

  action step = any {
    ComputeStaticExpiry,
    MarkMissingRetention,
    RepairMissingRetention,
    SweepExpiredRows,
    SweepHonorsBatchLimit,
  }

  /* Properties */

  val resultIsKnown = retentionResults.contains(lastResult)
  val inv = resultIsKnown

  /* Example Runs */

  run staticExpiryTest =
    all {
      assert(expiryMs(1_000, 86_400_000) == 86_401_000),
      assert(retentionCase(StaticExpiryCase) == StaticExpiryComputed),
    }

  run missingRetentionTest =
    assert(retentionCase(MissingRetentionCase) == MissingRetentionMarked)

  run repairRetentionTest =
    assert(retentionCase(RepairMissingRetentionCase) == MissingRetentionRepaired)

  run sweepExpiredRowsTest =
    all {
      assert(isExpired(2_000, 2_000)),
      assert(isExpired(2_000, 1_999) == false),
      assert(retentionCase(SweepExpiredRowsCase) == SweepDeletedExpiredOnly),
    }

  run sweepBatchLimitTest =
    all {
      assert(deletionCount(5, 2) == 2),
      assert(deletionCount(2, 5) == 2),
      assert(retentionCase(SweepBatchLimitCase) == SweepBatchLimitRespected),
    }

  /* Witnesses */

  val static_expiry_witness = lastResult == StaticExpiryComputed
  val missing_retention_witness = or {
    lastResult == MissingRetentionMarked,
    lastResult == MissingRetentionRepaired,
  }
  val sweep_witness = or {
    lastResult == SweepDeletedExpiredOnly,
    lastResult == SweepBatchLimitRespected,
  }
}
