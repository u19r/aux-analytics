// Privacy policy contract at the shared analytics-contract boundary.
// Every valid record is sanitized before it is persisted by each output
// boundary. The output state records both reachability and exact sanitized
// content evidence; invalid policies fail before either boundary persists.
module privacy_policy {
  type DenialScope = NoDeniedScope | KeyNameDenied | NestedPathDenied | ValueDenied

  type PrivacyCase =
    | NotCheckedCase
    | PublicRecordCase
    | DeniedKeyCase
    | DeniedPathCase
    | DeniedValueCase
    | MalformedPolicyCase
    | OversizedPolicyCase

  type PrivacyResult =
    | NotChecked
    | Allowed
    | Denied
    | InvalidPolicyRejected

  var result: PrivacyResult
  var reachedRawBackup: bool
  var reachedProjection: bool
  var rawBackupDeniedDataAbsent: bool
  var projectionDeniedDataAbsent: bool
  var rawBackupAllowedDataPreserved: bool
  var projectionAllowedDataPreserved: bool
  var droppedFields: int
  var policyVersionRecorded: bool
  var lastCase: PrivacyCase
  var denialScope: DenialScope
  var policyVersion: int

  pure def privacyResultFor(privacyCase: PrivacyCase): PrivacyResult =
    match privacyCase {
      | NotCheckedCase => NotChecked
      | PublicRecordCase => Allowed
      | DeniedKeyCase => Denied
      | DeniedPathCase => Denied
      | DeniedValueCase => Denied
      | MalformedPolicyCase => InvalidPolicyRejected
      | OversizedPolicyCase => InvalidPolicyRejected
    }

  action init = all {
    result' = NotChecked,
    reachedRawBackup' = false,
    reachedProjection' = false,
    rawBackupDeniedDataAbsent' = true,
    projectionDeniedDataAbsent' = true,
    rawBackupAllowedDataPreserved' = false,
    projectionAllowedDataPreserved' = false,
    droppedFields' = 0,
    policyVersionRecorded' = false,
    lastCase' = NotCheckedCase,
    denialScope' = NoDeniedScope,
    policyVersion' = 0,
  }

  action AllowPublicRecord = all {
    result' = privacyResultFor(PublicRecordCase),
    reachedRawBackup' = true,
    reachedProjection' = true,
    rawBackupDeniedDataAbsent' = true,
    projectionDeniedDataAbsent' = true,
    rawBackupAllowedDataPreserved' = true,
    projectionAllowedDataPreserved' = true,
    droppedFields' = 0,
    policyVersionRecorded' = true,
    lastCase' = PublicRecordCase,
    denialScope' = NoDeniedScope,
    policyVersion' = 1,
  }

  action SanitizeDeniedKeyRecord = all {
    result' = privacyResultFor(DeniedKeyCase),
    reachedRawBackup' = true,
    reachedProjection' = true,
    rawBackupDeniedDataAbsent' = true,
    projectionDeniedDataAbsent' = true,
    rawBackupAllowedDataPreserved' = true,
    projectionAllowedDataPreserved' = true,
    droppedFields' = 1,
    policyVersionRecorded' = true,
    lastCase' = DeniedKeyCase,
    denialScope' = KeyNameDenied,
    policyVersion' = 1,
  }

  action SanitizeDeniedPathRecord = all {
    result' = privacyResultFor(DeniedPathCase),
    reachedRawBackup' = true,
    reachedProjection' = true,
    rawBackupDeniedDataAbsent' = true,
    projectionDeniedDataAbsent' = true,
    rawBackupAllowedDataPreserved' = true,
    projectionAllowedDataPreserved' = true,
    droppedFields' = 1,
    policyVersionRecorded' = true,
    lastCase' = DeniedPathCase,
    denialScope' = NestedPathDenied,
    policyVersion' = 1,
  }

  action SanitizeDeniedValueRecord = all {
    result' = privacyResultFor(DeniedValueCase),
    reachedRawBackup' = true,
    reachedProjection' = true,
    rawBackupDeniedDataAbsent' = true,
    projectionDeniedDataAbsent' = true,
    rawBackupAllowedDataPreserved' = true,
    projectionAllowedDataPreserved' = true,
    droppedFields' = 1,
    policyVersionRecorded' = true,
    lastCase' = DeniedValueCase,
    denialScope' = ValueDenied,
    policyVersion' = 1,
  }

  action RejectInvalidPolicy = all {
    result' = privacyResultFor(MalformedPolicyCase),
    reachedRawBackup' = false,
    reachedProjection' = false,
    rawBackupDeniedDataAbsent' = true,
    projectionDeniedDataAbsent' = true,
    rawBackupAllowedDataPreserved' = false,
    projectionAllowedDataPreserved' = false,
    droppedFields' = 0,
    policyVersionRecorded' = false,
    lastCase' = MalformedPolicyCase,
    denialScope' = NoDeniedScope,
    policyVersion' = 0,
  }

  action RejectOversizedPolicy = all {
    result' = privacyResultFor(OversizedPolicyCase),
    reachedRawBackup' = false,
    reachedProjection' = false,
    rawBackupDeniedDataAbsent' = true,
    projectionDeniedDataAbsent' = true,
    rawBackupAllowedDataPreserved' = false,
    projectionAllowedDataPreserved' = false,
    droppedFields' = 0,
    policyVersionRecorded' = false,
    lastCase' = OversizedPolicyCase,
    denialScope' = NoDeniedScope,
    policyVersion' = 0,
  }

  action step = any {
    AllowPublicRecord,
    SanitizeDeniedKeyRecord,
    SanitizeDeniedPathRecord,
    SanitizeDeniedValueRecord,
    RejectInvalidPolicy,
    RejectOversizedPolicy,
  }

  val lastReportRawBackupSanitized =
    reachedRawBackup and rawBackupDeniedDataAbsent and rawBackupAllowedDataPreserved
  val lastReportProjectionSanitized =
    reachedProjection and projectionDeniedDataAbsent and projectionAllowedDataPreserved
  val deniedFieldsAbsentFromSanitizedRawBackup =
    if (lastCase == DeniedKeyCase or
        lastCase == DeniedPathCase or
        lastCase == DeniedValueCase)
      lastReportRawBackupSanitized
    else true
  val deniedFieldsAbsentFromSanitizedProjection =
    if (lastCase == DeniedKeyCase or
        lastCase == DeniedPathCase or
        lastCase == DeniedValueCase)
      lastReportProjectionSanitized
    else true
  val acceptedRecordsReachBothOutputs =
    if (lastCase == PublicRecordCase or
        lastCase == DeniedKeyCase or
        lastCase == DeniedPathCase or
        lastCase == DeniedValueCase)
      reachedRawBackup and reachedProjection
    else true
  val acceptedRecordsRecordPolicyVersion =
    if (lastCase == PublicRecordCase or
        lastCase == DeniedKeyCase or
        lastCase == DeniedPathCase or
        lastCase == DeniedValueCase)
      policyVersionRecorded and policyVersion == 1
    else true
  val acceptedRecordsReportDropCount =
    if (lastCase == PublicRecordCase) droppedFields == 0
    else if (lastCase == DeniedKeyCase or
             lastCase == DeniedPathCase or
             lastCase == DeniedValueCase)
      droppedFields == 1
    else true
  val invalidPolicyDoesNotProcessData =
    if (lastCase == MalformedPolicyCase or lastCase == OversizedPolicyCase)
      not(reachedRawBackup) and
      not(reachedProjection) and
      not(rawBackupAllowedDataPreserved) and
      not(projectionAllowedDataPreserved) and
      droppedFields == 0 and
      not(policyVersionRecorded) and
      policyVersion == 0
    else true
  val deniedCaseHasScope =
    if (lastCase == DeniedKeyCase or
        lastCase == DeniedPathCase or
        lastCase == DeniedValueCase)
      denialScope != NoDeniedScope and policyVersion > 0
    else if (lastCase == PublicRecordCase)
      denialScope == NoDeniedScope and policyVersion > 0
    else true
  val resultMatchesCanonicalCaseReducer =
    result == privacyResultFor(lastCase)

  val inv = all {
    resultMatchesCanonicalCaseReducer,
    deniedFieldsAbsentFromSanitizedRawBackup,
    deniedFieldsAbsentFromSanitizedProjection,
    acceptedRecordsReachBothOutputs,
    acceptedRecordsRecordPolicyVersion,
    acceptedRecordsReportDropCount,
    invalidPolicyDoesNotProcessData,
    deniedCaseHasScope,
  }

  run deniedRecordIsSanitized = assert(
    if (lastCase == DeniedKeyCase or
        lastCase == DeniedPathCase or
        lastCase == DeniedValueCase)
      reachedRawBackup and reachedProjection and
      rawBackupDeniedDataAbsent and projectionDeniedDataAbsent
    else true
  )
  run invalidPolicyStopsProcessing = assert(
    if (lastCase == MalformedPolicyCase or lastCase == OversizedPolicyCase)
      not(policyVersionRecorded) and policyVersion == 0 and droppedFields == 0
    else true
  )

  val allowed_witness =
    lastCase == PublicRecordCase and
    result == Allowed and
    reachedRawBackup and reachedProjection and
    rawBackupAllowedDataPreserved and projectionAllowedDataPreserved
  val raw_backup_sanitized_witness =
    lastCase == DeniedKeyCase and
    result == Denied and
    reachedRawBackup and rawBackupDeniedDataAbsent and droppedFields == 1
  val projection_sanitized_witness =
    lastCase == DeniedPathCase and
    result == Denied and
    reachedProjection and projectionDeniedDataAbsent and droppedFields == 1
  val value_sanitized_witness =
    lastCase == DeniedValueCase and
    result == Denied and
    reachedRawBackup and rawBackupDeniedDataAbsent and droppedFields == 1
  val invalid_policy_witness =
    (lastCase == MalformedPolicyCase or lastCase == OversizedPolicyCase) and
    result == InvalidPolicyRejected and
    not(reachedRawBackup) and not(reachedProjection)
  val malformed_policy_witness =
    lastCase == MalformedPolicyCase and
    result == InvalidPolicyRejected and
    not(reachedRawBackup) and not(reachedProjection)
  val oversized_policy_witness =
    lastCase == OversizedPolicyCase and
    result == InvalidPolicyRejected and
    not(reachedRawBackup) and not(reachedProjection)
}
