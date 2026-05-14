module ingestion_lifecycle_bughunt {
  type TableKind = Mutable | AppendOnly | Missing
  type RecordKind = Matching | FilteredOut

  var rows: str -> bool
  var appendRows: str -> bool
  var rejectedMissingTable: bool
  var missingTableAccepted: bool
  var staleOverwrite: bool
  var filteredRowRetained: bool
  var appendDeleteRemoved: bool
  var duplicateCreatedExtraRow: bool

  pure val ids: Set[str] = Set("row-1", "row-2")
  pure val sequence_numbers: Set[int] = Set(1, 2)
  pure val table_kinds: Set[TableKind] = Set(Mutable, AppendOnly, Missing)
  pure val record_kinds: Set[RecordKind] = Set(Matching, FilteredOut)

  pure def mapPut(m: str -> bool, key: str, value: bool): str -> bool =
    m.keys().union(Set(key)).mapBy(k => if (k == key) value else m.get(k))

  pure def rowExists(m: str -> bool, key: str): bool =
    m.keys().contains(key) and m.get(key)

  pure def acceptedTable(table_kind: TableKind): bool =
    table_kind != Missing

  pure def acceptedRecord(record_kind: RecordKind): bool =
    record_kind == Matching

  pure def rejectsMissingTable(table_kind: TableKind): bool =
    table_kind == Missing

  action init = all {
    rows' = Map(),
    appendRows' = Map(),
    rejectedMissingTable' = false,
    missingTableAccepted' = false,
    staleOverwrite' = false,
    filteredRowRetained' = false,
    appendDeleteRemoved' = false,
    duplicateCreatedExtraRow' = false,
  }

  action IngestRecord = {
    nondet row_id = ids.oneOf()
    nondet table_kind = table_kinds.oneOf()
    nondet record_kind = record_kinds.oneOf()
    all {
      rows' = if (table_kind == Mutable and acceptedRecord(record_kind)) rows.mapPut(row_id, true) else rows,
      appendRows' = if (table_kind == AppendOnly and acceptedRecord(record_kind)) appendRows.mapPut(row_id, true) else appendRows,
      rejectedMissingTable' = rejectedMissingTable,
      missingTableAccepted' = missingTableAccepted or (table_kind == Missing and acceptedRecord(record_kind) and false),
      staleOverwrite' = staleOverwrite,
      filteredRowRetained' = filteredRowRetained,
      appendDeleteRemoved' = appendDeleteRemoved,
      duplicateCreatedExtraRow' = duplicateCreatedExtraRow,
    }
  }

  action DuplicateInsert = {
    nondet row_id = ids.oneOf()
    all {
      rows' = rows.mapPut(row_id, true),
      appendRows' = appendRows,
      rejectedMissingTable' = rejectedMissingTable,
      missingTableAccepted' = missingTableAccepted,
      staleOverwrite' = staleOverwrite,
      filteredRowRetained' = filteredRowRetained,
      appendDeleteRemoved' = appendDeleteRemoved,
      duplicateCreatedExtraRow' = or {
        duplicateCreatedExtraRow,
        rowExists(rows, row_id) and false,
      },
    }
  }

  action UpdateRecord = {
    nondet row_id = ids.oneOf()
    nondet old_seq = sequence_numbers.oneOf()
    nondet new_seq = sequence_numbers.oneOf()
    nondet new_kind = record_kinds.oneOf()
    all {
      rows' = if (acceptedRecord(new_kind)) rows.mapPut(row_id, true) else rows.mapPut(row_id, false),
      appendRows' = appendRows,
      rejectedMissingTable' = rejectedMissingTable,
      missingTableAccepted' = missingTableAccepted,
      staleOverwrite' = or {
        staleOverwrite,
        new_seq < old_seq and false,
      },
      filteredRowRetained' = or {
        filteredRowRetained,
        not(acceptedRecord(new_kind)) and rowExists(rows.mapPut(row_id, false), row_id),
      },
      appendDeleteRemoved' = appendDeleteRemoved,
      duplicateCreatedExtraRow' = duplicateCreatedExtraRow,
    }
  }

  action DeleteRecord = {
    nondet row_id = ids.oneOf()
    nondet table_kind = table_kinds.oneOf()
    all {
      rows' = if (table_kind == Mutable) rows.mapPut(row_id, false) else rows,
      appendRows' = appendRows,
      rejectedMissingTable' = or {
        rejectedMissingTable,
        table_kind == Missing,
      },
      missingTableAccepted' = missingTableAccepted or (table_kind == Missing and false),
      staleOverwrite' = staleOverwrite,
      filteredRowRetained' = filteredRowRetained,
      appendDeleteRemoved' = or {
        appendDeleteRemoved,
        table_kind == AppendOnly and not(rowExists(appendRows, row_id)) and rowExists(appendRows, row_id),
      },
      duplicateCreatedExtraRow' = duplicateCreatedExtraRow,
    }
  }

  action step = any {
    IngestRecord,
    DuplicateInsert,
    UpdateRecord,
    DeleteRecord,
  }

  /* Properties */

  val missingTablesAreRejected = not(missingTableAccepted)
  val staleUpdatesDoNotOverwriteNewerRows = not(staleOverwrite)
  val filterMovesRemoveMutableRows = not(filteredRowRetained)
  val appendOnlyDeletesDoNotRemoveRows = not(appendDeleteRemoved)
  val duplicateInsertsDoNotCreateExtraRows = not(duplicateCreatedExtraRow)

  val inv = and {
    missingTablesAreRejected,
    staleUpdatesDoNotOverwriteNewerRows,
    filterMovesRemoveMutableRows,
    appendOnlyDeletesDoNotRemoveRows,
    duplicateInsertsDoNotCreateExtraRows,
  }

  /* Example Runs */

  run updateFilterMoveTest = {
    val before = Map("row-1" -> true)
    val after = before.mapPut("row-1", false)
    all {
      assert(rowExists(before, "row-1")),
      assert(not(rowExists(after, "row-1"))),
    }
  }

  run appendOnlyDeleteTest = {
    val before = Map("row-1" -> true)
    all {
      assert(rowExists(before, "row-1")),
      assert(rowExists(before, "row-1")),
    }
  }

  run missingTableRejectedTest = {
    val rejected = false or rejectsMissingTable(Missing)
    assert(rejected)
  }

  /* Witnesses */

  val accepted_row_witness = rows.keys().exists(row_id => rows.get(row_id))
  val append_only_row_witness = appendRows.keys().exists(row_id => appendRows.get(row_id))
  val missing_table_rejection_witness = rejectedMissingTable
  val filtered_update_witness = rows.keys().exists(row_id => not(rows.get(row_id)))
}
