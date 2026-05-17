module check {
  type CheckState =
    | Planned
    | ReadingSource
    | ReadingDestination
    | Comparing
    | Complete
    | Incomplete
    | Mismatched
    | Stale
    | PrivacyViolation
    | Failed

  var state: CheckState
  var destinationMutated: bool
  var checkedRows: int
  var missingRows: int
  var extraRows: int
  var mismatchedRows: int
  var staleRows: int
  var privacyViolations: int
  var sampleLimit: int
  var evidenceSamples: int
  var reportPersisted: bool

  action init = all {
    state' = Planned,
    destinationMutated' = false,
    checkedRows' = 0,
    missingRows' = 0,
    extraRows' = 0,
    mismatchedRows' = 0,
    staleRows' = 0,
    privacyViolations' = 0,
    sampleLimit' = 2,
    evidenceSamples' = 0,
    reportPersisted' = false,
  }

  action ReadSource = all {
    state == Planned,
    state' = ReadingSource,
    destinationMutated' = destinationMutated,
    checkedRows' = checkedRows,
    missingRows' = missingRows,
    extraRows' = extraRows,
    mismatchedRows' = mismatchedRows,
    staleRows' = staleRows,
    privacyViolations' = privacyViolations,
    sampleLimit' = sampleLimit,
    evidenceSamples' = evidenceSamples,
    reportPersisted' = reportPersisted,
  }

  action ReadDestination = all {
    state == ReadingSource,
    state' = ReadingDestination,
    destinationMutated' = destinationMutated,
    checkedRows' = checkedRows,
    missingRows' = missingRows,
    extraRows' = extraRows,
    mismatchedRows' = mismatchedRows,
    staleRows' = staleRows,
    privacyViolations' = privacyViolations,
    sampleLimit' = sampleLimit,
    evidenceSamples' = evidenceSamples,
    reportPersisted' = reportPersisted,
  }

  action CompareClean = all {
    state == ReadingDestination,
    state' = Complete,
    destinationMutated' = false,
    checkedRows' = checkedRows + 1,
    missingRows' = missingRows,
    extraRows' = extraRows,
    mismatchedRows' = mismatchedRows,
    staleRows' = staleRows,
    privacyViolations' = privacyViolations,
    sampleLimit' = sampleLimit,
    evidenceSamples' = evidenceSamples,
    reportPersisted' = true,
  }

  action CompareMissing = all {
    state == ReadingDestination,
    state' = Incomplete,
    destinationMutated' = false,
    checkedRows' = checkedRows + 1,
    missingRows' = missingRows + 1,
    extraRows' = extraRows,
    mismatchedRows' = mismatchedRows,
    staleRows' = staleRows,
    privacyViolations' = privacyViolations,
    sampleLimit' = sampleLimit,
    evidenceSamples' = if (evidenceSamples < sampleLimit) evidenceSamples + 1 else evidenceSamples,
    reportPersisted' = true,
  }

  action CompareExtra = all {
    state == ReadingDestination,
    state' = Incomplete,
    destinationMutated' = false,
    checkedRows' = checkedRows + 1,
    missingRows' = missingRows,
    extraRows' = extraRows + 1,
    mismatchedRows' = mismatchedRows,
    staleRows' = staleRows,
    privacyViolations' = privacyViolations,
    sampleLimit' = sampleLimit,
    evidenceSamples' = if (evidenceSamples < sampleLimit) evidenceSamples + 1 else evidenceSamples,
    reportPersisted' = true,
  }

  action CompareMismatch = all {
    state == ReadingDestination,
    state' = Mismatched,
    destinationMutated' = false,
    checkedRows' = checkedRows + 1,
    missingRows' = missingRows,
    extraRows' = extraRows,
    mismatchedRows' = mismatchedRows + 1,
    staleRows' = staleRows,
    privacyViolations' = privacyViolations,
    sampleLimit' = sampleLimit,
    evidenceSamples' = if (evidenceSamples < sampleLimit) evidenceSamples + 1 else evidenceSamples,
    reportPersisted' = true,
  }

  action CompareStale = all {
    state == ReadingDestination,
    state' = Stale,
    destinationMutated' = false,
    checkedRows' = checkedRows + 1,
    missingRows' = missingRows,
    extraRows' = extraRows,
    mismatchedRows' = mismatchedRows,
    staleRows' = staleRows + 1,
    privacyViolations' = privacyViolations,
    sampleLimit' = sampleLimit,
    evidenceSamples' = if (evidenceSamples < sampleLimit) evidenceSamples + 1 else evidenceSamples,
    reportPersisted' = true,
  }

  action ComparePrivacyViolation = all {
    state == ReadingDestination,
    state' = PrivacyViolation,
    destinationMutated' = false,
    checkedRows' = checkedRows + 1,
    missingRows' = missingRows,
    extraRows' = extraRows,
    mismatchedRows' = mismatchedRows,
    staleRows' = staleRows,
    privacyViolations' = privacyViolations + 1,
    sampleLimit' = sampleLimit,
    evidenceSamples' = if (evidenceSamples < sampleLimit) evidenceSamples + 1 else evidenceSamples,
    reportPersisted' = true,
  }

  action FailRead = all {
    state == ReadingSource or state == ReadingDestination,
    state' = Failed,
    destinationMutated' = false,
    checkedRows' = checkedRows,
    missingRows' = missingRows,
    extraRows' = extraRows,
    mismatchedRows' = mismatchedRows,
    staleRows' = staleRows,
    privacyViolations' = privacyViolations,
    sampleLimit' = sampleLimit,
    evidenceSamples' = evidenceSamples,
    reportPersisted' = reportPersisted,
  }

  action step = any {
    ReadSource,
    ReadDestination,
    CompareClean,
    CompareMissing,
    CompareExtra,
    CompareMismatch,
    CompareStale,
    ComparePrivacyViolation,
    FailRead,
  }

  val noDestinationMutation = not(destinationMutated)
  val boundedEvidence = evidenceSamples <= sampleLimit
  val reportForTerminalCheck = if (
    state == Complete or state == Incomplete or state == Mismatched or state == Stale or state == PrivacyViolation
  ) reportPersisted else true
  val privacyViolationWins = if (state == PrivacyViolation) privacyViolations > 0 else true
  val staleRowsAreReported = if (state == Stale) staleRows > 0 else true
  val mismatchRowsAreReported = if (state == Mismatched) mismatchedRows > 0 else true
  val incompleteRowsAreReported = if (state == Incomplete) missingRows + extraRows > 0 else true
  val completeMeansNoDifferences = if (state == Complete) all {
    missingRows == 0,
    extraRows == 0,
    mismatchedRows == 0,
    staleRows == 0,
    privacyViolations == 0,
  } else true

  val inv = all {
    noDestinationMutation,
    boundedEvidence,
    reportForTerminalCheck,
    privacyViolationWins,
    staleRowsAreReported,
    mismatchRowsAreReported,
    incompleteRowsAreReported,
    completeMeansNoDifferences,
  }

  run cleanCheck = all {
    assert(if (state == Complete) reportPersisted else true),
    assert(if (state == Complete) checkedRows >= 0 else true),
  }

  run missingRowCheck = all {
    assert(if (state == Incomplete) missingRows + extraRows > 0 else true),
    assert(evidenceSamples <= sampleLimit),
  }

  run privacyViolationCheck = all {
    assert(if (state == PrivacyViolation) privacyViolations > 0 else true),
    assert(not(destinationMutated)),
  }

  val complete_witness = state == Complete
  val incomplete_witness = state == Incomplete
  val mismatched_witness = state == Mismatched
  val stale_witness = state == Stale
  val privacy_violation_witness = state == PrivacyViolation
  val failed_read_witness = state == Failed
}
