module privacy_policy_mbt {
  type PrivacyCase =
    | PublicRecord
    | DeniedKey
    | DeniedValue
    | InvalidRegex

  type PrivacyResult =
    | NotRun
    | Allowed
    | Denied
    | InvalidPolicyRejected

  type PrivacyReport = {
    result: PrivacyResult,
    reachedRawBackup: bool,
    reachedProjection: bool,
    policyVersionRecorded: bool,
  }

  pure def defaultReport: PrivacyReport = {
    result: NotRun,
    reachedRawBackup: false,
    reachedProjection: false,
    policyVersionRecorded: false,
  }

  pure def reportFor(privacyCase: PrivacyCase): PrivacyReport =
    match privacyCase {
      | PublicRecord => {
          result: Allowed,
          reachedRawBackup: true,
          reachedProjection: true,
          policyVersionRecorded: true,
        }
      | DeniedKey => {
          result: Denied,
          reachedRawBackup: false,
          reachedProjection: false,
          policyVersionRecorded: true,
        }
      | DeniedValue => {
          result: Denied,
          reachedRawBackup: false,
          reachedProjection: false,
          policyVersionRecorded: true,
        }
      | InvalidRegex => {
          result: InvalidPolicyRejected,
          reachedRawBackup: false,
          reachedProjection: false,
          policyVersionRecorded: false,
        }
    }

  var lastReport: PrivacyReport

  action init = lastReport' = defaultReport

  action PrivacyScenario = {
    nondet privacyCase = Set(PublicRecord, DeniedKey, DeniedValue, InvalidRegex).oneOf()
    all {
      lastReport' = reportFor(privacyCase),
    }
  }

  action step = PrivacyScenario

  val deniedRecordsDoNotReachOutputs =
    if (lastReport.result == Denied) not(lastReport.reachedRawBackup) and not(lastReport.reachedProjection) else true
  val invalidPolicyDoesNotReachOutputs =
    if (lastReport.result == InvalidPolicyRejected) not(lastReport.reachedRawBackup) and not(lastReport.reachedProjection) else true
  val processedRecordsRecordVersion =
    if (lastReport.result == Allowed or lastReport.result == Denied) lastReport.policyVersionRecorded else true

  val inv = all {
    deniedRecordsDoNotReachOutputs,
    invalidPolicyDoesNotReachOutputs,
    processedRecordsRecordVersion,
  }

  run publicRecordAllowed = assert(reportFor(PublicRecord).reachedProjection)
  run deniedKeyBlocked = assert(not(reportFor(DeniedKey).reachedRawBackup))
  run invalidRegexRejected = assert(reportFor(InvalidRegex).result == InvalidPolicyRejected)
}
