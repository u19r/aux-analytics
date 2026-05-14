module retention_bughunt {
  var rowExpiry: str -> int
  var rowMissing: str -> bool
  var rowDeleted: str -> bool
  var rowHasPolicy: str -> bool
  var rowTenant: str -> str
  var deletedThisSweep: int
  var repairedWithoutPolicy: bool
  var deletedUnexpired: bool
  var batchLimitExceeded: bool

  pure val row_ids: Set[str] = Set("row-1", "row-2", "row-3")
  pure val tenant_ids: Set[str] = Set("tenant-a", "tenant-b")
  pure val expiries: Set[int] = Set(1_000, 2_000, 3_000)
  pure val now_values: Set[int] = Set(1_500, 2_500)
  pure val sweep_limits: Set[int] = Set(0, 1, 2)
  pure val bools: Set[bool] = Set(true, false)

  pure def mapPut(m: a -> b, key: a, value: b): a -> b =
    m.keys().union(Set(key)).mapBy(k => if (k == key) value else m.get(k))

  pure def isExpired(expires_at_ms: int, now_ms: int): bool =
    expires_at_ms <= now_ms

  pure def sweepDeletionAllowed(expires_at_ms: int, now_ms: int, batch_limit: int): bool =
    batch_limit > 0 and isExpired(expires_at_ms, now_ms)

  action init = all {
    rowExpiry' = Map(),
    rowMissing' = Map(),
    rowDeleted' = Map(),
    rowHasPolicy' = Map(),
    rowTenant' = Map(),
    deletedThisSweep' = 0,
    repairedWithoutPolicy' = false,
    deletedUnexpired' = false,
    batchLimitExceeded' = false,
  }

  action InsertPresentRetention = {
    nondet row_id = row_ids.oneOf()
    nondet tenant_id = tenant_ids.oneOf()
    nondet expiry = expiries.oneOf()
    all {
      rowExpiry' = rowExpiry.mapPut(row_id, expiry),
      rowMissing' = rowMissing.mapPut(row_id, false),
      rowDeleted' = rowDeleted.mapPut(row_id, false),
      rowHasPolicy' = rowHasPolicy.mapPut(row_id, true),
      rowTenant' = rowTenant.mapPut(row_id, tenant_id),
      deletedThisSweep' = deletedThisSweep,
      repairedWithoutPolicy' = repairedWithoutPolicy,
      deletedUnexpired' = deletedUnexpired,
      batchLimitExceeded' = batchLimitExceeded,
    }
  }

  action InsertMissingRetention = {
    nondet row_id = row_ids.oneOf()
    nondet tenant_id = tenant_ids.oneOf()
    nondet has_policy = bools.oneOf()
    all {
      rowExpiry' = rowExpiry.mapPut(row_id, 0),
      rowMissing' = rowMissing.mapPut(row_id, true),
      rowDeleted' = rowDeleted.mapPut(row_id, false),
      rowHasPolicy' = rowHasPolicy.mapPut(row_id, has_policy),
      rowTenant' = rowTenant.mapPut(row_id, tenant_id),
      deletedThisSweep' = deletedThisSweep,
      repairedWithoutPolicy' = repairedWithoutPolicy,
      deletedUnexpired' = deletedUnexpired,
      batchLimitExceeded' = batchLimitExceeded,
    }
  }

  action RepairMissingRetention = {
    nondet row_id = rowMissing.keys().oneOf()
    all {
      rowExpiry' = if (rowHasPolicy.get(row_id)) rowExpiry.mapPut(row_id, 3_000) else rowExpiry,
      rowMissing' = if (rowHasPolicy.get(row_id)) rowMissing.mapPut(row_id, false) else rowMissing,
      rowDeleted' = rowDeleted,
      rowHasPolicy' = rowHasPolicy,
      rowTenant' = rowTenant,
      deletedThisSweep' = deletedThisSweep,
      repairedWithoutPolicy' = or {
        repairedWithoutPolicy,
        not(rowHasPolicy.get(row_id)) and false,
      },
      deletedUnexpired' = deletedUnexpired,
      batchLimitExceeded' = batchLimitExceeded,
    }
  }

  action SweepOneCandidate = {
    nondet row_id = rowExpiry.keys().oneOf()
    nondet now_ms = now_values.oneOf()
    nondet batch_limit = sweep_limits.oneOf()
    all {
      rowDeleted' = if (sweepDeletionAllowed(rowExpiry.get(row_id), now_ms, batch_limit)) rowDeleted.mapPut(row_id, true) else rowDeleted,
      rowExpiry' = rowExpiry,
      rowMissing' = rowMissing,
      rowHasPolicy' = rowHasPolicy,
      rowTenant' = rowTenant,
      deletedThisSweep' = if (sweepDeletionAllowed(rowExpiry.get(row_id), now_ms, batch_limit)) 1 else 0,
      repairedWithoutPolicy' = repairedWithoutPolicy,
      deletedUnexpired' = or {
        deletedUnexpired,
        not(isExpired(rowExpiry.get(row_id), now_ms)) and sweepDeletionAllowed(rowExpiry.get(row_id), now_ms, batch_limit),
      },
      batchLimitExceeded' = or {
        batchLimitExceeded,
        (if (sweepDeletionAllowed(rowExpiry.get(row_id), now_ms, batch_limit)) 1 else 0) > batch_limit,
      },
    }
  }

  action step = any {
    InsertPresentRetention,
    InsertMissingRetention,
    RepairMissingRetention,
    SweepOneCandidate,
  }

  /* Properties */

  val sweepsNeverDeleteUnexpiredRows = not(deletedUnexpired)
  val repairRequiresValidPolicy = not(repairedWithoutPolicy)
  val sweepBatchLimitIsRespected = not(batchLimitExceeded)

  val inv = and {
    sweepsNeverDeleteUnexpiredRows,
    repairRequiresValidPolicy,
    sweepBatchLimitIsRespected,
  }

  /* Example Runs */

  run expiryAndSweepDecisionTest =
    all {
      assert(isExpired(1_000, 1_500)),
      assert(isExpired(2_000, 1_500) == false),
      assert(sweepDeletionAllowed(1_000, 1_500, 1)),
      assert(sweepDeletionAllowed(1_000, 1_500, 0) == false),
    }

  /* Witnesses */

  val missing_retention_row_witness = rowMissing.keys().exists(row_id => rowMissing.get(row_id))
  val repaired_row_witness = rowMissing.keys().exists(row_id => not(rowMissing.get(row_id)) and rowHasPolicy.get(row_id))
  val sweep_deleted_row_witness = rowDeleted.keys().exists(row_id => rowDeleted.get(row_id))
}
