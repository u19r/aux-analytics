// Bug-hunt contract for composed ingestion lifecycle decisions.
// Actors: an ingestion reducer receives records, duplicate writes, updates,
// and deletes for mutable, append-only, or missing registrations. Durable
// facts are the logical row/count/sequence maps; the last-operation fields are transient
// evidence used to state what a rejected/stale action must preserve. Sequence
// numbers model source order, record kind models a filter move, and failures
// are explicit missing-table rejection or protected-state preservation.
// The model intentionally abstracts SQL and tenant naming.
module ingestion_lifecycle_bughunt {
  type TableKind = Mutable | AppendOnly | Missing
  type RecordKind = Matching | FilteredOut
  type Operation = NoOperation | Ingest | Duplicate | Update | Delete

  var rows: str -> bool
  var appendRows: str -> bool
  var rowCounts: str -> int
  var rowSequences: str -> int
  var rejectedMissingTable: bool
  var lastRowsBefore: str -> bool
  var lastAppendRowsBefore: str -> bool
  var lastRowCountsBefore: str -> int
  var lastRowSequencesBefore: str -> int
  var lastOperation: Operation
  var lastRowId: str
  var lastTableKind: TableKind
  var lastRecordKind: RecordKind
  var lastRowExistedBefore: bool
  var lastAppendRowExistedBefore: bool
  var lastSequenceBefore: int
  var lastIncomingSequence: int
  var lastSequenceAfter: int
  var lastUpdateStale: bool

  pure val ids: Set[str] = Set("row-1", "row-2")
  pure val sequence_numbers: Set[int] = Set(1, 2, 3)
  pure val table_kinds: Set[TableKind] = Set(Mutable, AppendOnly, Missing)
  pure val record_kinds: Set[RecordKind] = Set(Matching, FilteredOut)

  pure def mapPut(m: str -> bool, key: str, value: bool): str -> bool =
    m.keys().union(Set(key)).mapBy(k => if (k == key) value else m.get(k))

  pure def intMapPut(m: str -> int, key: str, value: int): str -> int =
    m.keys().union(Set(key)).mapBy(k => if (k == key) value else m.get(k))

  pure def countAt(m: str -> int, key: str): int =
    if (m.keys().contains(key)) m.get(key) else 0

  pure def sequenceAt(m: str -> int, key: str): int =
    if (m.keys().contains(key)) m.get(key) else 0

  pure def nextDeleteSequence(current: int): int =
    current + 1

  pure def nextIngestSequence(row_exists: bool, current: int): int =
    if (row_exists) current else nextDeleteSequence(current)

  pure def updateIsStale(current: int, incoming: int): bool =
    incoming <= current

  pure def updateSequence(current: int, incoming: int): int =
    if (updateIsStale(current, incoming)) current else incoming

  pure def rowExists(m: str -> bool, key: str): bool =
    m.keys().contains(key) and m.get(key)

  pure def acceptedTable(table_kind: TableKind): bool =
    table_kind != Missing

  pure def acceptedRecord(record_kind: RecordKind): bool =
    record_kind == Matching

  pure def rejectsMissingTable(table_kind: TableKind): bool =
    table_kind == Missing

  pure def deleteForTable(m: str -> bool, key: str, table_kind: TableKind): str -> bool =
    if (table_kind == Mutable) m.mapPut(key, false) else m

  action init = all {
    rows' = Map(),
    appendRows' = Map(),
    rowCounts' = Map(),
    rowSequences' = Map(),
    rejectedMissingTable' = false,
    lastRowsBefore' = Map(),
    lastAppendRowsBefore' = Map(),
    lastRowCountsBefore' = Map(),
    lastRowSequencesBefore' = Map(),
    lastOperation' = NoOperation,
    lastRowId' = "",
    lastTableKind' = Missing,
    lastRecordKind' = FilteredOut,
    lastRowExistedBefore' = false,
    lastAppendRowExistedBefore' = false,
    lastSequenceBefore' = 0,
    lastIncomingSequence' = 0,
    lastSequenceAfter' = 0,
    lastUpdateStale' = false,
  }

  action IngestRecord = {
    nondet row_id = ids.oneOf()
    nondet table_kind = table_kinds.oneOf()
    nondet record_kind = record_kinds.oneOf()
    val ingest_sequence = nextIngestSequence(rowExists(rows, row_id), sequenceAt(rowSequences, row_id))
    val nextRows = if (table_kind == Mutable and acceptedRecord(record_kind)) rows.mapPut(row_id, true) else rows
    val nextAppendRows = if (table_kind == AppendOnly and acceptedRecord(record_kind)) appendRows.mapPut(row_id, true) else appendRows
    val nextRowCounts = if (table_kind == Mutable and acceptedRecord(record_kind))
      intMapPut(rowCounts, row_id, if (rowExists(rows, row_id)) countAt(rowCounts, row_id) else 1)
      else rowCounts
    val nextRowSequences = if (table_kind == Mutable and acceptedRecord(record_kind))
      intMapPut(rowSequences, row_id, ingest_sequence)
      else rowSequences
    all {
      rows' = nextRows,
      appendRows' = nextAppendRows,
      rowCounts' = nextRowCounts,
      rowSequences' = nextRowSequences,
      rejectedMissingTable' = or {
        rejectedMissingTable,
        table_kind == Missing,
      },
      lastRowsBefore' = rows,
      lastAppendRowsBefore' = appendRows,
      lastRowCountsBefore' = rowCounts,
      lastRowSequencesBefore' = rowSequences,
      lastOperation' = Ingest,
      lastRowId' = row_id,
      lastTableKind' = table_kind,
      lastRecordKind' = record_kind,
      lastRowExistedBefore' = rowExists(rows, row_id),
      lastAppendRowExistedBefore' = rowExists(appendRows, row_id),
      lastSequenceBefore' = sequenceAt(rowSequences, row_id),
      lastIncomingSequence' = if (table_kind == Mutable and acceptedRecord(record_kind)) ingest_sequence else 0,
      lastSequenceAfter' = sequenceAt(nextRowSequences, row_id),
      lastUpdateStale' = false,
    }
  }

  action DuplicateInsert = {
    nondet row_id = ids.oneOf()
    val row_existed = rowExists(rows, row_id)
    all {
      rows' = rows,
      appendRows' = appendRows,
      rowCounts' = rowCounts,
      rowSequences' = rowSequences,
      rejectedMissingTable' = rejectedMissingTable,
      lastRowsBefore' = rows,
      lastAppendRowsBefore' = appendRows,
      lastRowCountsBefore' = rowCounts,
      lastRowSequencesBefore' = rowSequences,
      lastOperation' = if (row_existed) Duplicate else NoOperation,
      lastRowId' = row_id,
      lastTableKind' = Mutable,
      lastRecordKind' = Matching,
      lastRowExistedBefore' = row_existed,
      lastAppendRowExistedBefore' = false,
      lastSequenceBefore' = sequenceAt(rowSequences, row_id),
      lastIncomingSequence' = 0,
      lastSequenceAfter' = sequenceAt(rowSequences, row_id),
      lastUpdateStale' = false,
    }
  }

  action UpdateRecord = {
    nondet row_id = ids.oneOf()
    nondet new_seq = sequence_numbers.oneOf()
    nondet new_kind = record_kinds.oneOf()
    val durable_seq = sequenceAt(rowSequences, row_id)
    val durable_sequence_known = rowSequences.keys().contains(row_id)
    val stale = updateIsStale(durable_seq, new_seq)
    val nextRows = if (stale) rows else if (acceptedRecord(new_kind)) rows.mapPut(row_id, true)
      else if (durable_sequence_known) rows.mapPut(row_id, false) else rows
    val nextRowCounts = if (stale) rowCounts else if (acceptedRecord(new_kind))
      intMapPut(rowCounts, row_id, if (rowExists(rows, row_id)) countAt(rowCounts, row_id) else 1)
      else if (durable_sequence_known) intMapPut(rowCounts, row_id, 0) else rowCounts
    val nextRowSequences = if (stale) rowSequences else if (acceptedRecord(new_kind) or durable_sequence_known) intMapPut(rowSequences, row_id, updateSequence(durable_seq, new_seq)) else rowSequences
    all {
      rows' = nextRows,
      appendRows' = appendRows,
      rowCounts' = nextRowCounts,
      rowSequences' = nextRowSequences,
      rejectedMissingTable' = rejectedMissingTable,
      lastRowsBefore' = rows,
      lastAppendRowsBefore' = appendRows,
      lastRowCountsBefore' = rowCounts,
      lastRowSequencesBefore' = rowSequences,
      lastOperation' = Update,
      lastRowId' = row_id,
      lastTableKind' = Mutable,
      lastRecordKind' = new_kind,
      lastRowExistedBefore' = rowExists(rows, row_id),
      lastAppendRowExistedBefore' = false,
      lastSequenceBefore' = durable_seq,
      lastIncomingSequence' = new_seq,
      lastSequenceAfter' = sequenceAt(nextRowSequences, row_id),
      lastUpdateStale' = stale,
    }
  }

  action DeleteRecord = {
    nondet row_id = ids.oneOf()
    nondet table_kind = table_kinds.oneOf()
    val delete_sequence = nextDeleteSequence(sequenceAt(rowSequences, row_id))
    val nextRows = if (table_kind == Mutable) rows.mapPut(row_id, false) else rows
    val nextRowCounts = if (table_kind == Mutable) intMapPut(rowCounts, row_id, 0) else rowCounts
    val nextRowSequences = if (table_kind == Mutable) intMapPut(rowSequences, row_id, delete_sequence) else rowSequences
    all {
      rows' = nextRows,
      appendRows' = appendRows,
      rowCounts' = nextRowCounts,
      rowSequences' = nextRowSequences,
      rejectedMissingTable' = or {
        rejectedMissingTable,
        table_kind == Missing,
      },
      lastRowsBefore' = rows,
      lastAppendRowsBefore' = appendRows,
      lastRowCountsBefore' = rowCounts,
      lastRowSequencesBefore' = rowSequences,
      lastOperation' = Delete,
      lastRowId' = row_id,
      lastTableKind' = table_kind,
      lastRecordKind' = Matching,
      lastRowExistedBefore' = rowExists(rows, row_id),
      lastAppendRowExistedBefore' = rowExists(appendRows, row_id),
      lastSequenceBefore' = sequenceAt(rowSequences, row_id),
      lastIncomingSequence' = if (table_kind == Mutable) delete_sequence else 0,
      lastSequenceAfter' = sequenceAt(nextRowSequences, row_id),
      lastUpdateStale' = false,
    }
  }

  action step = any {
    IngestRecord,
    DuplicateInsert,
    UpdateRecord,
    DeleteRecord,
  }

  /* Properties */

  val missingTablesPreserveAllState = not(
    lastTableKind == Missing and (
      rows != lastRowsBefore or
        appendRows != lastAppendRowsBefore or
        rowCounts != lastRowCountsBefore or
        rowSequences != lastRowSequencesBefore
    )
  )
  val staleUpdatesDoNotOverwriteNewerRows =
    not(lastOperation == Update and lastUpdateStale and
      (
        rows != lastRowsBefore or
          appendRows != lastAppendRowsBefore or
          rowCounts != lastRowCountsBefore or
          rowSequences != lastRowSequencesBefore
      ))
  val staleFlagMatchesDurableSequence =
    lastOperation != Update or
      lastUpdateStale == (lastIncomingSequence <= lastSequenceBefore)
  val updateSequenceDoesNotRegress =
    lastOperation != Update or lastSequenceAfter >= lastSequenceBefore
  val mutableIngestsRetainDurableAuthority = not(
    lastOperation == Ingest and lastTableKind == Mutable and acceptedRecord(lastRecordKind) and (
      (lastRowExistedBefore and lastSequenceAfter != lastSequenceBefore) or
        (not(lastRowExistedBefore) and lastSequenceAfter != nextDeleteSequence(lastSequenceBefore)) or
        lastIncomingSequence != lastSequenceAfter
    )
  )
  val mutableDeletesRetainDurableAuthority = not(
    lastOperation == Delete and lastTableKind == Mutable and (
      lastSequenceAfter != nextDeleteSequence(lastSequenceBefore) or
        lastIncomingSequence != lastSequenceAfter
    )
  )
  val mutableDeletesPreserveVisibilitySemantics = not(
    lastOperation == Delete and lastTableKind == Mutable and (
      rowExists(rows, lastRowId) or countAt(rowCounts, lastRowId) != 0
    )
  )
  val filterMovesRemoveMutableRows =
    not(lastOperation == Update and lastRecordKind == FilteredOut and
      not(lastUpdateStale) and lastRowExistedBefore and rowExists(rows, lastRowId))
  val filteredUpdatesRetainDurableCursor = not(
    lastOperation == Update and lastRecordKind == FilteredOut and not(lastUpdateStale) and (
      lastRowSequencesBefore.keys().contains(lastRowId) and (
        lastSequenceAfter != lastIncomingSequence or
          not(rowSequences.keys().contains(lastRowId)) or
          rowExists(rows, lastRowId) or countAt(rowCounts, lastRowId) != 0
      )
    )
  )
  val filteredUpdatesDoNotMaterializeAbsentRows = not(
    lastOperation == Update and lastRecordKind == FilteredOut and not(lastUpdateStale) and
      not(lastRowSequencesBefore.keys().contains(lastRowId)) and (
        rowSequences.keys().contains(lastRowId) or
          rows.keys().contains(lastRowId) or
          rowCounts.keys().contains(lastRowId)
    )
  )
  val appendOnlyDeletesDoNotRemoveRows =
    not(lastOperation == Delete and lastTableKind == AppendOnly and
      (
        rows != lastRowsBefore or
          appendRows != lastAppendRowsBefore or
          rowCounts != lastRowCountsBefore or
          rowSequences != lastRowSequencesBefore
      ))
  val duplicateInsertsDoNotCreateExtraRows = not(
    lastOperation == Duplicate and (
      rows != lastRowsBefore or
        appendRows != lastAppendRowsBefore or
        rowCounts != lastRowCountsBefore or
        rowSequences != lastRowSequencesBefore
    )
  )
  val duplicateOperationsRequireExistingRow =
    lastOperation != Duplicate or lastRowExistedBefore

  val inv = and {
    missingTablesPreserveAllState,
    staleUpdatesDoNotOverwriteNewerRows,
    staleFlagMatchesDurableSequence,
    updateSequenceDoesNotRegress,
    mutableIngestsRetainDurableAuthority,
    mutableDeletesRetainDurableAuthority,
    mutableDeletesPreserveVisibilitySemantics,
    filterMovesRemoveMutableRows,
    filteredUpdatesRetainDurableCursor,
    filteredUpdatesDoNotMaterializeAbsentRows,
    appendOnlyDeletesDoNotRemoveRows,
    duplicateInsertsDoNotCreateExtraRows,
    duplicateOperationsRequireExistingRow,
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
    val after = deleteForTable(before, "row-1", AppendOnly)
    all {
      assert(rowExists(before, "row-1")),
      assert(rowExists(after, "row-1")),
      assert(after == before),
    }
  }

  run missingTableRejectedTest = {
    val rejected = false or rejectsMissingTable(Missing)
    assert(rejected)
  }

  run durableTombstoneOrderingTest = {
    pure val initialSequences = Map()
    pure val afterDeleteRows = mapPut(Map(), "row-2", false)
    pure val afterDeleteCounts = intMapPut(Map(), "row-2", 0)
    pure val afterDelete = intMapPut(initialSequences, "row-2", nextDeleteSequence(sequenceAt(initialSequences, "row-2")))
    pure val sameCursor = sequenceAt(afterDelete, "row-2")
    pure val sameCursorIsStale = updateIsStale(sameCursor, sameCursor)
    pure val laterCursor = sameCursor + 1
    pure val laterRows = mapPut(afterDeleteRows, "row-2", true)
    pure val laterCounts = intMapPut(afterDeleteCounts, "row-2", 1)
    all {
      assert(sameCursor == 1),
      assert(not(rowExists(afterDeleteRows, "row-2"))),
      assert(countAt(afterDeleteCounts, "row-2") == 0),
      assert(sameCursorIsStale),
      assert(updateSequence(sameCursor, sameCursor) == sameCursor),
      assert(not(rowExists(afterDeleteRows, "row-2"))),
      assert(countAt(afterDeleteCounts, "row-2") == 0),
      assert(not(updateIsStale(sameCursor, laterCursor))),
      assert(updateSequence(sameCursor, laterCursor) == laterCursor),
      assert(nextIngestSequence(false, sameCursor) == laterCursor),
      assert(rowExists(laterRows, "row-2")),
      assert(countAt(laterCounts, "row-2") == 1),
    }
  }

  run filteredCursorOrderingTest = {
    pure val matchingRows = mapPut(Map(), "row-1", true)
    pure val matchingCounts = intMapPut(Map(), "row-1", 1)
    pure val matchingSequences = intMapPut(Map(), "row-1", 1)
    pure val filteredRows = mapPut(matchingRows, "row-1", false)
    pure val filteredCounts = intMapPut(matchingCounts, "row-1", 0)
    pure val filteredSequences = intMapPut(matchingSequences, "row-1", 2)
    pure val sameCursorIsStale = updateIsStale(sequenceAt(filteredSequences, "row-1"), 2)
    pure val sameCursorRows = if (sameCursorIsStale) filteredRows else mapPut(filteredRows, "row-1", true)
    pure val sameCursorCounts = if (sameCursorIsStale) filteredCounts else intMapPut(filteredCounts, "row-1", 1)
    pure val sameCursorSequences = if (sameCursorIsStale) filteredSequences else intMapPut(filteredSequences, "row-1", 2)
    pure val laterCursorIsStale = updateIsStale(sequenceAt(sameCursorSequences, "row-1"), 3)
    pure val laterRows = if (laterCursorIsStale) sameCursorRows else mapPut(sameCursorRows, "row-1", true)
    pure val laterCounts = if (laterCursorIsStale) sameCursorCounts else intMapPut(sameCursorCounts, "row-1", 1)
    pure val laterSequences = if (laterCursorIsStale) sameCursorSequences else intMapPut(sameCursorSequences, "row-1", 3)
    all {
      assert(rowExists(matchingRows, "row-1")),
      assert(countAt(matchingCounts, "row-1") == 1),
      assert(sequenceAt(matchingSequences, "row-1") == 1),
      assert(not(rowExists(filteredRows, "row-1"))),
      assert(countAt(filteredCounts, "row-1") == 0),
      assert(sequenceAt(filteredSequences, "row-1") == 2),
      assert(sameCursorIsStale),
      assert(not(rowExists(sameCursorRows, "row-1"))),
      assert(countAt(sameCursorCounts, "row-1") == 0),
      assert(sequenceAt(sameCursorSequences, "row-1") == 2),
      assert(not(laterCursorIsStale)),
      assert(rowExists(laterRows, "row-1")),
      assert(countAt(laterCounts, "row-1") == 1),
      assert(sequenceAt(laterSequences, "row-1") == 3),
    }
  }

  run filteredAbsentDoesNotMaterializeTest = {
    pure val existingRows = mapPut(Map(), "row-2", true)
    pure val existingCounts = intMapPut(Map(), "row-2", 1)
    pure val existingSequences = intMapPut(Map(), "row-2", 3)
    pure val filteredRows = existingRows
    pure val filteredCounts = existingCounts
    pure val filteredSequences = existingSequences
    all {
      assert(rowExists(existingRows, "row-2")),
      assert(sequenceAt(existingSequences, "row-2") == 3),
      assert(not(filteredRows.keys().contains("row-1"))),
      assert(not(filteredCounts.keys().contains("row-1"))),
      assert(not(filteredSequences.keys().contains("row-1"))),
      assert(sequenceAt(filteredSequences, "row-1") == 0),
      assert(rowExists(filteredRows, "row-2")),
      assert(countAt(filteredCounts, "row-2") == 1),
      assert(sequenceAt(filteredSequences, "row-2") == 3),
    }
  }

  /* Witnesses */

  val accepted_row_witness = rows.keys().exists(row_id => rows.get(row_id))
  val append_only_row_witness = appendRows.keys().exists(row_id => appendRows.get(row_id))
  val missing_table_rejection_witness = rejectedMissingTable
  val filtered_update_witness = rows.keys().exists(row_id => not(rows.get(row_id)))
  val append_only_delete_witness =
    lastOperation == Delete and lastTableKind == AppendOnly and lastAppendRowExistedBefore
  val duplicate_insert_witness = lastOperation == Duplicate and lastRowExistedBefore
  val stale_update_witness = lastOperation == Update and lastUpdateStale and lastRowExistedBefore
  val durable_tombstone_witness =
    lastOperation == Delete and lastTableKind == Mutable and
      not(lastRowExistedBefore) and lastSequenceAfter > lastSequenceBefore and
      not(rowExists(rows, lastRowId)) and countAt(rowCounts, lastRowId) == 0
  val filtered_cursor_tombstone_witness =
    lastOperation == Update and lastRecordKind == FilteredOut and
      not(lastUpdateStale) and lastRowExistedBefore and
      lastSequenceBefore == 1 and lastIncomingSequence == 2 and
      lastSequenceAfter == lastIncomingSequence and lastSequenceAfter > lastSequenceBefore and
      not(rowExists(rows, lastRowId)) and countAt(rowCounts, lastRowId) == 0
  val filtered_absent_no_metadata_witness =
    lastOperation == Update and lastRecordKind == FilteredOut and
      not(lastUpdateStale) and not(lastRowExistedBefore) and
      not(lastRowSequencesBefore.keys().contains(lastRowId)) and
      lastSequenceBefore == 0 and lastIncomingSequence == 1 and
      lastSequenceAfter == 0 and not(rowSequences.keys().contains(lastRowId)) and
      not(rows.keys().contains(lastRowId)) and not(rowCounts.keys().contains(lastRowId))
  val higher_cursor_matching_witness =
    lastOperation == Update and lastRecordKind == Matching and
      not(lastUpdateStale) and not(lastRowExistedBefore) and
      lastSequenceBefore == 2 and lastIncomingSequence == 3 and
      lastSequenceAfter == 3 and rowExists(rows, lastRowId) and
      countAt(rowCounts, lastRowId) == 1
}
