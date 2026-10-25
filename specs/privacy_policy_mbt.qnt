// Compact Rust-conformance model for privacy policy output behavior.
// Production boundaries sanitize each valid record independently: both output
// boundaries receive the record, denied data is absent, allowed data remains,
// and the dropped-field count and policy version are observable. Invalid
// policies fail before either boundary can persist anything.
module privacy_policy_mbt {
  type PrivacyCase =
    | NotChecked
    | PublicRecord
    | DeniedKey
    | DeniedPath
    | DeniedValue
    | MalformedPolicy
    | OversizedPolicy

  type PrivacyResult =
    | NotRun
    | Allowed
    | Denied
    | InvalidPolicyRejected

  type PrivacyReport = {
    result: PrivacyResult,
    reachedRawBackup: bool,
    reachedProjection: bool,
    rawBackupDeniedDataAbsent: bool,
    projectionDeniedDataAbsent: bool,
    rawBackupAllowedDataPreserved: bool,
    projectionAllowedDataPreserved: bool,
    droppedFields: int,
    policyVersionRecorded: bool,
  }

  pure def defaultReport: PrivacyReport = {
    result: NotRun,
    reachedRawBackup: false,
    reachedProjection: false,
    rawBackupDeniedDataAbsent: true,
    projectionDeniedDataAbsent: true,
    rawBackupAllowedDataPreserved: false,
    projectionAllowedDataPreserved: false,
    droppedFields: 0,
    policyVersionRecorded: false,
  }

  pure def reportFor(privacyCase: PrivacyCase): PrivacyReport =
    match privacyCase {
      | NotChecked => defaultReport
      | PublicRecord => {
          result: Allowed,
          reachedRawBackup: true,
          reachedProjection: true,
          rawBackupDeniedDataAbsent: true,
          projectionDeniedDataAbsent: true,
          rawBackupAllowedDataPreserved: true,
          projectionAllowedDataPreserved: true,
          droppedFields: 0,
          policyVersionRecorded: true,
        }
      | DeniedKey => {
          result: Denied,
          reachedRawBackup: true,
          reachedProjection: true,
          rawBackupDeniedDataAbsent: true,
          projectionDeniedDataAbsent: true,
          rawBackupAllowedDataPreserved: true,
          projectionAllowedDataPreserved: true,
          droppedFields: 1,
          policyVersionRecorded: true,
        }
      | DeniedPath => {
          result: Denied,
          reachedRawBackup: true,
          reachedProjection: true,
          rawBackupDeniedDataAbsent: true,
          projectionDeniedDataAbsent: true,
          rawBackupAllowedDataPreserved: true,
          projectionAllowedDataPreserved: true,
          droppedFields: 1,
          policyVersionRecorded: true,
        }
      | DeniedValue => {
          result: Denied,
          reachedRawBackup: true,
          reachedProjection: true,
          rawBackupDeniedDataAbsent: true,
          projectionDeniedDataAbsent: true,
          rawBackupAllowedDataPreserved: true,
          projectionAllowedDataPreserved: true,
          droppedFields: 1,
          policyVersionRecorded: true,
        }
      | MalformedPolicy => {
          result: InvalidPolicyRejected,
          reachedRawBackup: false,
          reachedProjection: false,
          rawBackupDeniedDataAbsent: true,
          projectionDeniedDataAbsent: true,
          rawBackupAllowedDataPreserved: false,
          projectionAllowedDataPreserved: false,
          droppedFields: 0,
          policyVersionRecorded: false,
        }
      | OversizedPolicy => {
          result: InvalidPolicyRejected,
          reachedRawBackup: false,
          reachedProjection: false,
          rawBackupDeniedDataAbsent: true,
          projectionDeniedDataAbsent: true,
          rawBackupAllowedDataPreserved: false,
          projectionAllowedDataPreserved: false,
          droppedFields: 0,
          policyVersionRecorded: false,
        }
    }

  var lastReport: PrivacyReport
  var lastCase: PrivacyCase

  action init = all {
    lastReport' = defaultReport,
    lastCase' = NotChecked,
  }

  action PrivacyScenario = {
    nondet privacyCase = Set(
      PublicRecord,
      DeniedKey,
      DeniedPath,
      DeniedValue,
      MalformedPolicy,
      OversizedPolicy,
    ).oneOf()
    all {
      lastReport' = reportFor(privacyCase),
      lastCase' = privacyCase,
    }
  }

  action step = PrivacyScenario

  val deniedRecordsAreSanitizedAtBothOutputs =
    if (lastReport.result == Denied)
      lastReport.reachedRawBackup and
      lastReport.reachedProjection and
      lastReport.rawBackupDeniedDataAbsent and
      lastReport.projectionDeniedDataAbsent and
      lastReport.rawBackupAllowedDataPreserved and
      lastReport.projectionAllowedDataPreserved
    else true
  val invalidPolicyDoesNotReachOutputs =
    if (lastReport.result == InvalidPolicyRejected)
      not(lastReport.reachedRawBackup) and
      not(lastReport.reachedProjection) and
      not(lastReport.rawBackupAllowedDataPreserved) and
      not(lastReport.projectionAllowedDataPreserved) and
      lastReport.droppedFields == 0 and
      not(lastReport.policyVersionRecorded)
    else true
  val processedRecordsRecordVersion =
    if (lastReport.result == Allowed or lastReport.result == Denied)
      lastReport.policyVersionRecorded
    else true
  val processedRecordsReportDropCount =
    if (lastReport.result == Allowed) lastReport.droppedFields == 0
    else if (lastReport.result == Denied) lastReport.droppedFields > 0
    else true
  val reportMatchesCanonicalCaseReducer =
    lastReport == reportFor(lastCase)
  val validRecordsReachBothOutputs =
    if (lastReport.result == Allowed or lastReport.result == Denied)
      lastReport.reachedRawBackup and lastReport.reachedProjection
    else true

  val inv = all {
    reportMatchesCanonicalCaseReducer,
    deniedRecordsAreSanitizedAtBothOutputs,
    invalidPolicyDoesNotReachOutputs,
    processedRecordsRecordVersion,
    processedRecordsReportDropCount,
    validRecordsReachBothOutputs,
  }

  val public_witness =
    lastReport.result == Allowed and
    lastReport.reachedRawBackup and
    lastReport.reachedProjection and
    lastReport.rawBackupAllowedDataPreserved and
    lastReport.projectionAllowedDataPreserved
  val denied_sanitized_witness =
    lastReport.result == Denied and
    lastReport.reachedRawBackup and
    lastReport.reachedProjection and
    lastReport.rawBackupDeniedDataAbsent and
    lastReport.projectionDeniedDataAbsent and
    lastReport.droppedFields > 0
  val invalid_witness =
    lastReport.result == InvalidPolicyRejected and
    not(lastReport.reachedRawBackup) and
    not(lastReport.reachedProjection)
  val malformed_policy_witness =
    lastCase == MalformedPolicy and
    lastReport.result == InvalidPolicyRejected and
    not(lastReport.reachedRawBackup) and
    not(lastReport.reachedProjection)
  val oversized_policy_witness =
    lastCase == OversizedPolicy and
    lastReport.result == InvalidPolicyRejected and
    not(lastReport.reachedRawBackup) and
    not(lastReport.reachedProjection)

  run publicRecordAllowed = assert(reportFor(PublicRecord).reachedProjection)
  run deniedKeySanitized = assert(
    reportFor(DeniedKey).reachedRawBackup and
    reportFor(DeniedKey).rawBackupDeniedDataAbsent and
    reportFor(DeniedKey).droppedFields == 1
  )
  run malformedPolicyRejected = assert(
    reportFor(MalformedPolicy).result == InvalidPolicyRejected
  )
  run oversizedPolicyRejected = assert(
    reportFor(OversizedPolicy).result == InvalidPolicyRejected
  )
}
