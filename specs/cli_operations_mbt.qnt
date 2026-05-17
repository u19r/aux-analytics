module cli_operations_mbt {
  type CliCase =
    | OperationsStatus
    | OperationsAudit
    | OperationsCancel
    | BackfillRun
    | BackfillResume
    | TrimApplyMissingConfirmation
    | PrivacyDeleteMissingConfirmation

  type CliResult =
    | NotRun
    | ParsedReadOnly
    | ParsedRun
    | ParsedResume
    | ParsedCancel
    | RejectedMissingConfirmation

  pure def resultFor(cliCase: CliCase): CliResult =
    match cliCase {
      | OperationsStatus => ParsedReadOnly
      | OperationsAudit => ParsedReadOnly
      | OperationsCancel => ParsedCancel
      | BackfillRun => ParsedRun
      | BackfillResume => ParsedResume
      | TrimApplyMissingConfirmation => RejectedMissingConfirmation
      | PrivacyDeleteMissingConfirmation => RejectedMissingConfirmation
    }

  var lastCase: CliCase
  var lastResult: CliResult

  action init = all {
    lastCase' = OperationsStatus,
    lastResult' = NotRun,
  }

  action CliScenario = {
    nondet cliCase = Set(
      OperationsStatus,
      OperationsAudit,
      OperationsCancel,
      BackfillRun,
      BackfillResume,
      TrimApplyMissingConfirmation,
      PrivacyDeleteMissingConfirmation,
    ).oneOf()
    all {
      lastCase' = cliCase,
      lastResult' = resultFor(cliCase),
    }
  }

  action step = CliScenario

  val readOnlyDoesNotCancel =
    if (lastResult == ParsedReadOnly) lastCase != OperationsCancel else true
  val cancelIsExplicit =
    if (lastResult == ParsedCancel) lastCase == OperationsCancel else true
  val resumeIsDistinct =
    if (lastResult == ParsedResume) lastCase == BackfillResume else true
  val destructiveWithoutConfirmationRejected =
    if (
      lastCase == TrimApplyMissingConfirmation
        or lastCase == PrivacyDeleteMissingConfirmation
    ) lastResult == RejectedMissingConfirmation else true

  val inv = all {
    readOnlyDoesNotCancel,
    cancelIsExplicit,
    resumeIsDistinct,
    destructiveWithoutConfirmationRejected,
  }

  run statusParses = assert(resultFor(OperationsStatus) == ParsedReadOnly)
  run cancelParses = assert(resultFor(OperationsCancel) == ParsedCancel)
  run resumeParses = assert(resultFor(BackfillResume) == ParsedResume)
  run trimMissingConfirmationRejected =
    assert(resultFor(TrimApplyMissingConfirmation) == RejectedMissingConfirmation)
}
