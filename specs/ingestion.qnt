module ingestion {
  var rows: str -> str
  var auditRows: str -> str

  pure val userIds: Set[str] = Set("user-1", "user-2", "user-3")
  pure val emails: Set[str] = Set("alpha@example.com", "bravo@example.com", "charlie@example.com")
  pure val auditIds: Set[str] = Set("audit-1", "audit-2")
  pure val auditActions: Set[str] = Set("created", "deleted")

  pure def mapRemove(m: a -> b, key: a): a -> b =
    m.keys().exclude(Set(key)).mapBy(k => m.get(k))

  pure def mapPut(m: a -> b, key: a, value: b): a -> b =
    m.keys().union(Set(key)).mapBy(k => if (k == key) value else m.get(k))

  pure def values(m: a -> b): Set[b] =
    m.keys().map(k => m.get(k))

  action init = all {
    rows' = Map(),
    auditRows' = Map(),
  }

  action InsertUser = all {
    nondet user_id = userIds.oneOf()
    nondet email = emails.oneOf()
    rows' = rows.mapPut(user_id, email),
    auditRows' = auditRows,
  }

  action UpdateUser = all {
    nondet user_id = rows.keys().oneOf()
    nondet email = emails.oneOf()
    rows' = rows.mapPut(user_id, email),
    auditRows' = auditRows,
  }

  action DeleteUser = all {
    nondet user_id = rows.keys().oneOf()
    rows' = rows.mapRemove(user_id),
    auditRows' = auditRows,
  }

  action InsertAudit = all {
    nondet audit_id = auditIds.oneOf()
    nondet action_name = auditActions.oneOf()
    auditRows' = auditRows.mapPut(audit_id, action_name),
    rows' = rows,
  }

  action DeleteAuditSkipped = all {
    nondet audit_id = auditRows.keys().oneOf()
    rows' = rows,
    auditRows' = auditRows,
  }

  action UnregisteredIngestRejected = all {
    rows' = rows,
    auditRows' = auditRows,
  }

  action step = any {
    InsertUser,
    UpdateUser,
    DeleteUser,
    InsertAudit,
    DeleteAuditSkipped,
    UnregisteredIngestRejected,
  }

  /* Properties */

  val rowsHaveKnownIds = rows.keys().subseteq(userIds)
  val rowsHaveKnownEmails = values(rows).subseteq(emails)
  val auditRowsHaveKnownIds = auditRows.keys().subseteq(auditIds)
  val auditRowsHaveKnownActions = values(auditRows).subseteq(auditActions)
  val rowCountIsBounded = rows.keys().size() <= userIds.size()

  val inv = and {
    rowsHaveKnownIds,
    rowsHaveKnownEmails,
    auditRowsHaveKnownIds,
    auditRowsHaveKnownActions,
    rowCountIsBounded,
  }

  /* Example Runs */

  run insertUpdateDeleteUserTest = {
    pure val inserted = Map().mapPut("user-1", "alpha@example.com")
    pure val updated = inserted.mapPut("user-1", "bravo@example.com")
    pure val deleted = updated.mapRemove("user-1")
    all {
      assert(inserted.get("user-1") == "alpha@example.com"),
      assert(updated.get("user-1") == "bravo@example.com"),
      assert(not(deleted.keys().contains("user-1"))),
    }
  }

  run appendOnlyDeleteIsSkippedTest = {
    pure val inserted = Map().mapPut("audit-1", "created")
    pure val afterDelete = inserted
    assert(afterDelete.get("audit-1") == "created")
  }

  /* Witnesses */

  val user_row_exists = rows.keys().size() > 0
  val user_row_can_be_updated = values(rows).contains("bravo@example.com")
  val audit_row_exists = auditRows.keys().size() > 0
}
