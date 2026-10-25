module check_mbt {
  type CheckCase =
    | Clean
    | Missing
    | Extra
    | MissingExtra
    | Mismatched
    | Stale
    | PrivacyViolation
  type CheckStrategy = CountsOnly | KeySet | ChunkHashes | FullRows
  type SampleLimit = OneSample | TwoSamples
  type CheckOutcome =
    | NotChecked
    | Complete
    | Incomplete
    | MismatchedOutcome
    | StaleOutcome
    | PrivacyViolationOutcome

  // This is the exact typed CheckReport projection returned by LocalCheckExecutor.
  // Request choices are action inputs; they are not fabricated as persisted state.
  type ReportState = {
    operationId: str,
    strategy: CheckStrategy,
    outcome: CheckOutcome,
    sourceRows: int,
    destinationRows: int,
    checkedRows: int,
    missingKeys: int,
    extraKeys: int,
    mismatchedRows: int,
    staleRows: int,
    privacyViolations: int,
    deterministicChunkHashMismatches: int,
    unsupportedComparisons: int,
    checkDurationMs: int,
    missingSamples: List[str],
    extraSamples: List[str],
    mismatchedSamples: List[str],
    staleSamples: List[str],
    privacySamples: List[str],
  }

  pure val OPERATION_ID = "check_mbt"

  var lastReport: ReportState

  pure def expectedReport(checkCase: CheckCase, strategy: CheckStrategy, sampleLimit: SampleLimit): ReportState = {
    val twoSamples = sampleLimit == TwoSamples
    if (checkCase == Clean) {
      operationId: OPERATION_ID,
      strategy: strategy,
      outcome: Complete,
      sourceRows: 2,
      destinationRows: 2,
      checkedRows: 2,
      missingKeys: 0,
      extraKeys: 0,
      mismatchedRows: 0,
      staleRows: 0,
      privacyViolations: 0,
      deterministicChunkHashMismatches: 0,
      unsupportedComparisons: 0,
      checkDurationMs: 0,
      missingSamples: List(),
      extraSamples: List(),
      mismatchedSamples: List(),
      staleSamples: List(),
      privacySamples: List(),
    }
    else if (checkCase == Missing) {
      operationId: OPERATION_ID,
      strategy: strategy,
      outcome: Incomplete,
      sourceRows: 2,
      destinationRows: 1,
      checkedRows: 2,
      missingKeys: 1,
      extraKeys: 0,
      mismatchedRows: 0,
      staleRows: 0,
      privacyViolations: 0,
      deterministicChunkHashMismatches: if (strategy == ChunkHashes) 1 else 0,
      unsupportedComparisons: 0,
      checkDurationMs: 0,
      missingSamples: if (strategy == CountsOnly) List("count_missing:0") else List("users:b"),
      extraSamples: List(),
      mismatchedSamples: List(),
      staleSamples: List(),
      privacySamples: List(),
    }
    else if (checkCase == Extra) {
      operationId: OPERATION_ID,
      strategy: strategy,
      outcome: Incomplete,
      sourceRows: 1,
      destinationRows: 2,
      checkedRows: 2,
      missingKeys: 0,
      extraKeys: 1,
      mismatchedRows: 0,
      staleRows: 0,
      privacyViolations: 0,
      deterministicChunkHashMismatches: if (strategy == ChunkHashes) 1 else 0,
      unsupportedComparisons: 0,
      checkDurationMs: 0,
      missingSamples: List(),
      extraSamples: if (strategy == CountsOnly) List("count_extra:0") else List("users:b"),
      mismatchedSamples: List(),
      staleSamples: List(),
      privacySamples: List(),
    }
    else if (checkCase == MissingExtra) {
      operationId: OPERATION_ID,
      strategy: strategy,
      outcome: if (strategy == CountsOnly) Complete else Incomplete,
      sourceRows: 1,
      destinationRows: 1,
      checkedRows: 2,
      missingKeys: if (strategy == CountsOnly) 0 else 1,
      extraKeys: if (strategy == CountsOnly) 0 else 1,
      mismatchedRows: 0,
      staleRows: 0,
      privacyViolations: 0,
      deterministicChunkHashMismatches: if (strategy == ChunkHashes) 1 else 0,
      unsupportedComparisons: 0,
      checkDurationMs: 0,
      missingSamples: if (strategy == CountsOnly) List() else List("users:a"),
      extraSamples: if (strategy == CountsOnly) List() else List("users:b"),
      mismatchedSamples: List(),
      staleSamples: List(),
      privacySamples: List(),
    }
    else if (checkCase == Mismatched and strategy == FullRows) {
      operationId: OPERATION_ID,
      strategy: strategy,
      outcome: MismatchedOutcome,
      sourceRows: 2,
      destinationRows: 2,
      checkedRows: 2,
      missingKeys: 0,
      extraKeys: 0,
      mismatchedRows: 2,
      staleRows: 0,
      privacyViolations: 0,
      deterministicChunkHashMismatches: 0,
      unsupportedComparisons: 0,
      checkDurationMs: 0,
      missingSamples: List(),
      extraSamples: List(),
      mismatchedSamples: if (twoSamples) List("users:a", "users:b") else List("users:a"),
      staleSamples: List(),
      privacySamples: List(),
    }
    else if (checkCase == Stale and strategy == FullRows) {
      operationId: OPERATION_ID,
      strategy: strategy,
      outcome: StaleOutcome,
      sourceRows: 1,
      destinationRows: 1,
      checkedRows: 1,
      missingKeys: 0,
      extraKeys: 0,
      mismatchedRows: 0,
      staleRows: 1,
      privacyViolations: 0,
      deterministicChunkHashMismatches: 0,
      unsupportedComparisons: 0,
      checkDurationMs: 0,
      missingSamples: List(),
      extraSamples: List(),
      mismatchedSamples: List(),
      staleSamples: List("users:a"),
      privacySamples: List(),
    }
    else if (checkCase == PrivacyViolation) {
      operationId: OPERATION_ID,
      strategy: strategy,
      outcome: PrivacyViolationOutcome,
      sourceRows: 1,
      destinationRows: 1,
      checkedRows: 1,
      missingKeys: 0,
      extraKeys: 0,
      mismatchedRows: 0,
      staleRows: 0,
      privacyViolations: 1,
      deterministicChunkHashMismatches: 0,
      unsupportedComparisons: 0,
      checkDurationMs: 0,
      missingSamples: List(),
      extraSamples: List(),
      mismatchedSamples: List(),
      staleSamples: List(),
      privacySamples: List("users:a"),
    }
    else {
      operationId: OPERATION_ID,
      strategy: strategy,
      outcome: if (strategy == ChunkHashes and (checkCase == Mismatched or checkCase == Stale)) MismatchedOutcome else Complete,
      sourceRows: if (checkCase == Stale) 1 else 2,
      destinationRows: if (checkCase == Stale) 1 else 2,
      checkedRows: if (checkCase == Stale) 1 else 2,
      missingKeys: 0,
      extraKeys: 0,
      mismatchedRows: 0,
      staleRows: 0,
      privacyViolations: 0,
      deterministicChunkHashMismatches: if (strategy == ChunkHashes and (checkCase == Mismatched or checkCase == Stale)) 1 else 0,
      unsupportedComparisons: 0,
      checkDurationMs: 0,
      missingSamples: List(),
      extraSamples: List(),
      mismatchedSamples: List(),
      staleSamples: List(),
      privacySamples: List(),
    }
  }

  action init = {
    lastReport' = {
      operationId: "",
      strategy: FullRows,
      outcome: NotChecked,
      sourceRows: 0,
      destinationRows: 0,
      checkedRows: 0,
      missingKeys: 0,
      extraKeys: 0,
      mismatchedRows: 0,
      staleRows: 0,
      privacyViolations: 0,
      deterministicChunkHashMismatches: 0,
      unsupportedComparisons: 0,
      checkDurationMs: 0,
      missingSamples: List(),
      extraSamples: List(),
      mismatchedSamples: List(),
      staleSamples: List(),
      privacySamples: List(),
    }
  }

  action CheckReport = {
    nondet checkCase = Set(Clean, Missing, Extra, MissingExtra, Mismatched, Stale, PrivacyViolation).oneOf()
    nondet strategy = Set(CountsOnly, KeySet, ChunkHashes, FullRows).oneOf()
    nondet sampleLimit = Set(OneSample, TwoSamples).oneOf()
    lastReport' = expectedReport(checkCase, strategy, sampleLimit)
  }

  action step = CheckReport

  val reportCountersAreNonnegative = all {
    lastReport.sourceRows >= 0,
    lastReport.destinationRows >= 0,
    lastReport.checkedRows >= 0,
    lastReport.missingKeys >= 0,
    lastReport.extraKeys >= 0,
    lastReport.mismatchedRows >= 0,
    lastReport.staleRows >= 0,
    lastReport.privacyViolations >= 0,
    lastReport.deterministicChunkHashMismatches >= 0,
    lastReport.unsupportedComparisons >= 0,
    lastReport.checkDurationMs >= 0,
  }

  val reportIdentityIsBounded =
    lastReport.operationId == "" or lastReport.operationId == OPERATION_ID

  val completeHasNoDifferences =
    if (lastReport.outcome == Complete) all {
      lastReport.missingKeys == 0,
      lastReport.extraKeys == 0,
      lastReport.mismatchedRows == 0,
      lastReport.staleRows == 0,
      lastReport.privacyViolations == 0,
      lastReport.deterministicChunkHashMismatches == 0,
      lastReport.unsupportedComparisons == 0,
    } else true

  val outcomeHasEvidence =
    if (lastReport.outcome == Incomplete) lastReport.missingKeys > 0 or lastReport.extraKeys > 0
    else if (lastReport.outcome == MismatchedOutcome) lastReport.mismatchedRows > 0 or lastReport.deterministicChunkHashMismatches > 0
    else if (lastReport.outcome == StaleOutcome) lastReport.staleRows > 0
    else if (lastReport.outcome == PrivacyViolationOutcome) lastReport.privacyViolations > 0
    else true

  val completeAndNotCheckedHaveNoSamples =
    if (lastReport.outcome == Complete or lastReport.outcome == NotChecked) all {
      lastReport.missingSamples == List(),
      lastReport.extraSamples == List(),
      lastReport.mismatchedSamples == List(),
      lastReport.staleSamples == List(),
      lastReport.privacySamples == List(),
    } else true

  val inv = all {
    reportCountersAreNonnegative,
    reportIdentityIsBounded,
    completeHasNoDifferences,
    outcomeHasEvidence,
    completeAndNotCheckedHaveNoSamples,
  }

  // Witnesses cover report outcome and strategy branches without adding request history to state.
  val complete_witness = lastReport.outcome == Complete
  val incomplete_witness = lastReport.outcome == Incomplete
  val mismatched_witness = lastReport.outcome == MismatchedOutcome
  val stale_witness = lastReport.outcome == StaleOutcome
  val privacy_violation_witness = lastReport.outcome == PrivacyViolationOutcome
  val counts_only_witness = lastReport.strategy == CountsOnly
  val key_set_witness = lastReport.strategy == KeySet
  val chunk_hashes_witness = lastReport.strategy == ChunkHashes
  val full_rows_witness = lastReport.strategy == FullRows

  run checkMbtExamples = all {
    assert(expectedReport(Clean, FullRows, OneSample).outcome == Complete),
    assert(expectedReport(Missing, CountsOnly, OneSample).missingSamples == List("count_missing:0")),
    assert(expectedReport(Missing, FullRows, OneSample).missingSamples == List("users:b")),
    assert(expectedReport(MissingExtra, FullRows, OneSample).missingSamples == List("users:a")),
    assert(expectedReport(Mismatched, FullRows, OneSample).mismatchedSamples == List("users:a")),
    assert(expectedReport(Mismatched, FullRows, TwoSamples).mismatchedSamples == List("users:a", "users:b")),
    assert(expectedReport(Mismatched, CountsOnly, OneSample).outcome == Complete),
    assert(expectedReport(Stale, FullRows, OneSample).outcome == StaleOutcome),
    assert(expectedReport(PrivacyViolation, KeySet, OneSample).outcome == PrivacyViolationOutcome),
  }
}
