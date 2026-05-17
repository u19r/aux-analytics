module privacy_policy {
  type PrivacyResult =
    | NotChecked
    | Allowed
    | DeniedBeforeRawBackup
    | DeniedBeforeProjection
    | InvalidPolicyRejected

  var result: PrivacyResult
  var deniedKeyPresent: bool
  var reachedRawBackup: bool
  var reachedProjection: bool
  var policyVersionRecorded: bool

  action init = all {
    result' = NotChecked,
    deniedKeyPresent' = false,
    reachedRawBackup' = false,
    reachedProjection' = false,
    policyVersionRecorded' = false,
  }

  action AllowPublicRecord = all {
    result' = Allowed,
    deniedKeyPresent' = false,
    reachedRawBackup' = true,
    reachedProjection' = true,
    policyVersionRecorded' = true,
  }

  action DenyBeforeRawBackup = all {
    result' = DeniedBeforeRawBackup,
    deniedKeyPresent' = true,
    reachedRawBackup' = false,
    reachedProjection' = false,
    policyVersionRecorded' = true,
  }

  action DenyBeforeProjection = all {
    result' = DeniedBeforeProjection,
    deniedKeyPresent' = true,
    reachedRawBackup' = false,
    reachedProjection' = false,
    policyVersionRecorded' = true,
  }

  action RejectInvalidPolicy = all {
    result' = InvalidPolicyRejected,
    deniedKeyPresent' = false,
    reachedRawBackup' = false,
    reachedProjection' = false,
    policyVersionRecorded' = false,
  }

  action step = any {
    AllowPublicRecord,
    DenyBeforeRawBackup,
    DenyBeforeProjection,
    RejectInvalidPolicy,
  }

  val deniedDataNeverReachesRawBackup = if (deniedKeyPresent) not(reachedRawBackup) else true
  val deniedDataNeverReachesProjection = if (deniedKeyPresent) not(reachedProjection) else true
  val acceptedRecordsRecordPolicyVersion =
    if (result == Allowed or result == DeniedBeforeRawBackup or result == DeniedBeforeProjection)
      policyVersionRecorded
    else true
  val invalidPolicyDoesNotProcessData =
    if (result == InvalidPolicyRejected) not(reachedRawBackup) and not(reachedProjection) else true

  val inv = all {
    deniedDataNeverReachesRawBackup,
    deniedDataNeverReachesProjection,
    acceptedRecordsRecordPolicyVersion,
    invalidPolicyDoesNotProcessData,
  }

  run deniedRecordIsBlocked = assert(if (deniedKeyPresent) not(reachedRawBackup) and not(reachedProjection) else true)
  run invalidPolicyStopsProcessing = assert(if (result == InvalidPolicyRejected) not(policyVersionRecorded) else true)

  val allowed_witness = result == Allowed and reachedRawBackup and reachedProjection
  val raw_backup_denial_witness = result == DeniedBeforeRawBackup and not(reachedRawBackup)
  val projection_denial_witness = result == DeniedBeforeProjection and not(reachedProjection)
  val invalid_policy_witness = result == InvalidPolicyRejected
}
