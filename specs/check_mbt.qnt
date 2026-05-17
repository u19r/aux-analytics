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

  type ReportState = {
    outcome: CheckOutcome,
    checkedRows: int,
    missingKeys: int,
    extraKeys: int,
    mismatchedRows: int,
    staleRows: int,
    privacyViolations: int,
    deterministicChunkHashMismatches: int,
    missingSamples: int,
    extraSamples: int,
    mismatchedSamples: int,
    staleSamples: int,
    privacySamples: int,
  }

  var lastCase: CheckCase
  var lastStrategy: CheckStrategy
  var lastSampleLimit: SampleLimit
  var lastReport: ReportState

  pure def sampleLimitValue(sampleLimit: SampleLimit): int =
    if (sampleLimit == OneSample) 1 else 2

  pure def min(a: int, b: int): int =
    if (a < b) a else b

  pure def expectedReport(checkCase: CheckCase, strategy: CheckStrategy, sampleLimit: SampleLimit): ReportState = {
    val limit = sampleLimitValue(sampleLimit)
    if (checkCase == Clean) {
      outcome: Complete,
      checkedRows: 2,
      missingKeys: 0,
      extraKeys: 0,
      mismatchedRows: 0,
      staleRows: 0,
      privacyViolations: 0,
      deterministicChunkHashMismatches: 0,
      missingSamples: 0,
      extraSamples: 0,
      mismatchedSamples: 0,
      staleSamples: 0,
      privacySamples: 0,
    }
    else if (checkCase == Missing) {
      outcome: Incomplete,
      checkedRows: 2,
      missingKeys: 1,
      extraKeys: 0,
      mismatchedRows: 0,
      staleRows: 0,
      privacyViolations: 0,
      deterministicChunkHashMismatches: if (strategy == ChunkHashes) 1 else 0,
      missingSamples: min(1, limit),
      extraSamples: 0,
      mismatchedSamples: 0,
      staleSamples: 0,
      privacySamples: 0,
    }
    else if (checkCase == Extra) {
      outcome: Incomplete,
      checkedRows: 2,
      missingKeys: 0,
      extraKeys: 1,
      mismatchedRows: 0,
      staleRows: 0,
      privacyViolations: 0,
      deterministicChunkHashMismatches: if (strategy == ChunkHashes) 1 else 0,
      missingSamples: 0,
      extraSamples: min(1, limit),
      mismatchedSamples: 0,
      staleSamples: 0,
      privacySamples: 0,
    }
    else if (checkCase == MissingExtra) {
      outcome: if (strategy == CountsOnly) Complete else Incomplete,
      checkedRows: 2,
      missingKeys: if (strategy == CountsOnly) 0 else 1,
      extraKeys: if (strategy == CountsOnly) 0 else 1,
      mismatchedRows: 0,
      staleRows: 0,
      privacyViolations: 0,
      deterministicChunkHashMismatches: if (strategy == ChunkHashes) 1 else 0,
      missingSamples: if (strategy == CountsOnly) 0 else min(1, limit),
      extraSamples: if (strategy == CountsOnly) 0 else min(1, limit),
      mismatchedSamples: 0,
      staleSamples: 0,
      privacySamples: 0,
    }
    else if (checkCase == Mismatched and strategy == FullRows) {
      outcome: MismatchedOutcome,
      checkedRows: 2,
      missingKeys: 0,
      extraKeys: 0,
      mismatchedRows: 2,
      staleRows: 0,
      privacyViolations: 0,
      deterministicChunkHashMismatches: 0,
      missingSamples: 0,
      extraSamples: 0,
      mismatchedSamples: min(2, limit),
      staleSamples: 0,
      privacySamples: 0,
    }
    else if (checkCase == Stale and strategy == FullRows) {
      outcome: StaleOutcome,
      checkedRows: 1,
      missingKeys: 0,
      extraKeys: 0,
      mismatchedRows: 0,
      staleRows: 1,
      privacyViolations: 0,
      deterministicChunkHashMismatches: 0,
      missingSamples: 0,
      extraSamples: 0,
      mismatchedSamples: 0,
      staleSamples: min(1, limit),
      privacySamples: 0,
    }
    else if (checkCase == PrivacyViolation) {
      outcome: PrivacyViolationOutcome,
      checkedRows: 1,
      missingKeys: 0,
      extraKeys: 0,
      mismatchedRows: 0,
      staleRows: 0,
      privacyViolations: 1,
      deterministicChunkHashMismatches: 0,
      missingSamples: 0,
      extraSamples: 0,
      mismatchedSamples: 0,
      staleSamples: 0,
      privacySamples: min(1, limit),
    }
    else {
      outcome: if (strategy == ChunkHashes and (checkCase == Mismatched or checkCase == Stale)) MismatchedOutcome else Complete,
      checkedRows: if (checkCase == Stale) 1 else 2,
      missingKeys: 0,
      extraKeys: 0,
      mismatchedRows: 0,
      staleRows: 0,
      privacyViolations: 0,
      deterministicChunkHashMismatches: if (strategy == ChunkHashes and (checkCase == Mismatched or checkCase == Stale)) 1 else 0,
      missingSamples: 0,
      extraSamples: 0,
      mismatchedSamples: 0,
      staleSamples: 0,
      privacySamples: 0,
    }
  }

  action init = all {
    lastCase' = Clean,
    lastStrategy' = FullRows,
    lastSampleLimit' = OneSample,
    lastReport' = {
      outcome: NotChecked,
      checkedRows: 0,
      missingKeys: 0,
      extraKeys: 0,
      mismatchedRows: 0,
      staleRows: 0,
      privacyViolations: 0,
      deterministicChunkHashMismatches: 0,
      missingSamples: 0,
      extraSamples: 0,
      mismatchedSamples: 0,
      staleSamples: 0,
      privacySamples: 0,
    },
  }

  action CheckReport = {
    nondet checkCase = Set(Clean, Missing, Extra, MissingExtra, Mismatched, Stale, PrivacyViolation).oneOf()
    nondet strategy = Set(CountsOnly, KeySet, ChunkHashes, FullRows).oneOf()
    nondet sampleLimit = Set(OneSample, TwoSamples).oneOf()
    all {
      lastCase' = checkCase,
      lastStrategy' = strategy,
      lastSampleLimit' = sampleLimit,
      lastReport' = expectedReport(checkCase, strategy, sampleLimit),
    }
  }

  action step = CheckReport

  val boundedSamples = all {
    lastReport.missingSamples <= sampleLimitValue(lastSampleLimit),
    lastReport.extraSamples <= sampleLimitValue(lastSampleLimit),
    lastReport.mismatchedSamples <= sampleLimitValue(lastSampleLimit),
    lastReport.staleSamples <= sampleLimitValue(lastSampleLimit),
    lastReport.privacySamples <= sampleLimitValue(lastSampleLimit),
  }
  val completeHasNoDifferences =
    if (lastReport.outcome == Complete) all {
      lastReport.missingKeys == 0,
      lastReport.extraKeys == 0,
      lastReport.mismatchedRows == 0,
      lastReport.staleRows == 0,
      lastReport.privacyViolations == 0,
      lastReport.deterministicChunkHashMismatches == 0,
    } else true
  val privacyTakesPrecedence =
    if (lastReport.outcome == PrivacyViolationOutcome) lastReport.privacyViolations > 0 else true
  val inv = all {
    boundedSamples,
    completeHasNoDifferences,
    privacyTakesPrecedence,
  }

  run checkMbtExamples = all {
    assert(expectedReport(Clean, FullRows, OneSample).outcome == Complete),
    assert(expectedReport(MissingExtra, FullRows, OneSample).missingSamples == 1),
    assert(expectedReport(Mismatched, FullRows, OneSample).mismatchedSamples == 1),
    assert(expectedReport(Mismatched, CountsOnly, OneSample).outcome == Complete),
    assert(expectedReport(Stale, FullRows, OneSample).outcome == StaleOutcome),
    assert(expectedReport(PrivacyViolation, KeySet, OneSample).outcome == PrivacyViolationOutcome),
  }
}
