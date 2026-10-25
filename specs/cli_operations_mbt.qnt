module cli_operations_mbt {
  type CliCase =
    | NotStarted
    | OperationsStatus
    | OperationsAudit
    | OperationsCancel
    | BackfillRun
    | BackfillResume
    | TrimDryRun
    | TrimApplyMissingConfirmation
    | PrivacyDeleteMissingConfirmation

  type CliResult =
    | NotRun
    | ParsedStatus
    | ParsedAudit
    | ParsedCancel
    | ParsedRun
    | ParsedResume
    | ParsedDryRun
    | RejectedTrimMissingConfirmation
    | RejectedPrivacyMissingConfirmation

  type CliState = {
    lastCase: CliCase,
    lastResult: CliResult,
    operationPersisted: bool,
    reportPersisted: bool,
    effectEvidence: bool,
    auditEvidence: bool,
    sentinelUnchanged: bool,
  }

  // The model describes the observable contract of each production command.
  // The adapter obtains these fields by rereading the operation store and the
  // command's real destination; it never manufactures a report for an error.
  pure def stateFor(cliCase: CliCase): CliState =
    match cliCase {
      | NotStarted => {
          lastCase: NotStarted,
          lastResult: NotRun,
          operationPersisted: false,
          reportPersisted: false,
          effectEvidence: false,
          auditEvidence: false,
          sentinelUnchanged: true,
        }
      | OperationsStatus => {
          lastCase: OperationsStatus,
          lastResult: ParsedStatus,
          operationPersisted: true,
          reportPersisted: false,
          effectEvidence: false,
          auditEvidence: true,
          sentinelUnchanged: true,
        }
      | OperationsAudit => {
          lastCase: OperationsAudit,
          lastResult: ParsedAudit,
          operationPersisted: true,
          reportPersisted: false,
          effectEvidence: false,
          auditEvidence: true,
          sentinelUnchanged: true,
        }
      | OperationsCancel => {
          lastCase: OperationsCancel,
          lastResult: ParsedCancel,
          operationPersisted: true,
          reportPersisted: false,
          effectEvidence: false,
          auditEvidence: true,
          sentinelUnchanged: true,
        }
      | BackfillRun => {
          lastCase: BackfillRun,
          lastResult: ParsedRun,
          operationPersisted: true,
          reportPersisted: true,
          effectEvidence: true,
          auditEvidence: true,
          sentinelUnchanged: true,
        }
      | BackfillResume => {
          lastCase: BackfillResume,
          lastResult: ParsedResume,
          operationPersisted: true,
          reportPersisted: true,
          effectEvidence: true,
          auditEvidence: true,
          sentinelUnchanged: true,
        }
      | TrimDryRun => {
          lastCase: TrimDryRun,
          lastResult: ParsedDryRun,
          operationPersisted: true,
          reportPersisted: true,
          effectEvidence: true,
          auditEvidence: true,
          sentinelUnchanged: true,
        }
      | TrimApplyMissingConfirmation => {
          lastCase: TrimApplyMissingConfirmation,
          lastResult: RejectedTrimMissingConfirmation,
          operationPersisted: false,
          reportPersisted: false,
          effectEvidence: false,
          auditEvidence: false,
          sentinelUnchanged: true,
        }
      | PrivacyDeleteMissingConfirmation => {
          lastCase: PrivacyDeleteMissingConfirmation,
          lastResult: RejectedPrivacyMissingConfirmation,
          operationPersisted: false,
          reportPersisted: false,
          effectEvidence: false,
          auditEvidence: false,
          sentinelUnchanged: true,
        }
    }

  var lastCase: CliCase
  var lastResult: CliResult
  var operationPersisted: bool
  var reportPersisted: bool
  var effectEvidence: bool
  var auditEvidence: bool
  var sentinelUnchanged: bool

  action init = all {
    lastCase' = NotStarted,
    lastResult' = NotRun,
    operationPersisted' = false,
    reportPersisted' = false,
    effectEvidence' = false,
    auditEvidence' = false,
    sentinelUnchanged' = true,
  }

  action CliScenario = {
    nondet cliCase = Set(
      OperationsStatus,
      OperationsAudit,
      OperationsCancel,
      BackfillRun,
      BackfillResume,
      TrimDryRun,
      TrimApplyMissingConfirmation,
      PrivacyDeleteMissingConfirmation,
    ).oneOf()
    val observed = stateFor(cliCase)
    all {
      lastCase' = observed.lastCase,
      lastResult' = observed.lastResult,
      operationPersisted' = observed.operationPersisted,
      reportPersisted' = observed.reportPersisted,
      effectEvidence' = observed.effectEvidence,
      auditEvidence' = observed.auditEvidence,
      sentinelUnchanged' = observed.sentinelUnchanged,
    }
  }

  action step = CliScenario

  val statusReadWitness =
    lastCase == OperationsStatus and lastResult == ParsedStatus
  val auditReadWitness =
    lastCase == OperationsAudit and lastResult == ParsedAudit
  val cancelWitness =
    lastCase == OperationsCancel
      and lastResult == ParsedCancel
      and operationPersisted
      and auditEvidence
  val runWitness =
    lastCase == BackfillRun
      and lastResult == ParsedRun
      and operationPersisted
      and reportPersisted
      and effectEvidence
      and auditEvidence
  val resumeWitness =
    lastCase == BackfillResume
      and lastResult == ParsedResume
      and operationPersisted
      and reportPersisted
      and effectEvidence
      and auditEvidence
  val dryRunWitness =
    lastCase == TrimDryRun
      and lastResult == ParsedDryRun
      and operationPersisted
      and reportPersisted
      and effectEvidence
      and sentinelUnchanged
      and auditEvidence
  val trimMissingConfirmationWitness =
    lastCase == TrimApplyMissingConfirmation
      and lastResult == RejectedTrimMissingConfirmation
      and not(operationPersisted)
      and not(reportPersisted)
      and not(effectEvidence)
      and not(auditEvidence)
      and sentinelUnchanged
  val privacyMissingConfirmationWitness =
    lastCase == PrivacyDeleteMissingConfirmation
      and lastResult == RejectedPrivacyMissingConfirmation
      and not(operationPersisted)
      and not(reportPersisted)
      and not(effectEvidence)
      and not(auditEvidence)
      and sentinelUnchanged
  val destructiveRejectionWitness =
    trimMissingConfirmationWitness or privacyMissingConfirmationWitness

  // Stable aliases retained for the inventory's conformance receipt.
  val read_only_witness = statusReadWitness or auditReadWitness
  val run_witness = runWitness
  val resume_witness = resumeWitness
  val destructive_rejection_witness = destructiveRejectionWitness

  // This is deliberately keyed only by the last command case. Every reachable
  // case has one exact result/evidence contract, including initialization and
  // destructive rejection; no witness is needed to activate the invariant.
  val caseContract =
    match lastCase {
      | NotStarted =>
          lastResult == NotRun
            and not(operationPersisted)
            and not(reportPersisted)
            and not(effectEvidence)
            and not(auditEvidence)
            and sentinelUnchanged
      | OperationsStatus =>
          lastResult == ParsedStatus
            and operationPersisted
            and not(reportPersisted)
            and not(effectEvidence)
            and auditEvidence
            and sentinelUnchanged
      | OperationsAudit =>
          lastResult == ParsedAudit
            and operationPersisted
            and not(reportPersisted)
            and not(effectEvidence)
            and auditEvidence
            and sentinelUnchanged
      | OperationsCancel =>
          lastResult == ParsedCancel
            and operationPersisted
            and not(reportPersisted)
            and not(effectEvidence)
            and auditEvidence
            and sentinelUnchanged
      | BackfillRun =>
          lastResult == ParsedRun
            and operationPersisted
            and reportPersisted
            and effectEvidence
            and auditEvidence
            and sentinelUnchanged
      | BackfillResume =>
          lastResult == ParsedResume
            and operationPersisted
            and reportPersisted
            and effectEvidence
            and auditEvidence
            and sentinelUnchanged
      | TrimDryRun =>
          lastResult == ParsedDryRun
            and operationPersisted
            and reportPersisted
            and effectEvidence
            and auditEvidence
            and sentinelUnchanged
      | TrimApplyMissingConfirmation =>
          lastResult == RejectedTrimMissingConfirmation
            and not(operationPersisted)
            and not(reportPersisted)
            and not(effectEvidence)
            and not(auditEvidence)
            and sentinelUnchanged
      | PrivacyDeleteMissingConfirmation =>
          lastResult == RejectedPrivacyMissingConfirmation
            and not(operationPersisted)
            and not(reportPersisted)
            and not(effectEvidence)
            and not(auditEvidence)
            and sentinelUnchanged
    }

  val inv = caseContract

  run statusParses = assert(stateFor(OperationsStatus).lastResult == ParsedStatus)
  run auditParses = assert(stateFor(OperationsAudit).lastResult == ParsedAudit)
  run cancelParses = assert(stateFor(OperationsCancel).lastResult == ParsedCancel)
  run resumeParses = assert(stateFor(BackfillResume).lastResult == ParsedResume)
  run cancelPersists =
    assert(
      stateFor(OperationsCancel).operationPersisted
        and stateFor(OperationsCancel).auditEvidence
    )
  run runPersistsReport =
    assert(
      stateFor(BackfillRun).reportPersisted
        and stateFor(BackfillRun).effectEvidence
    )
  run resumePersistsReport =
    assert(
      stateFor(BackfillResume).reportPersisted
        and stateFor(BackfillResume).effectEvidence
    )
  run trimMissingConfirmationRejected =
    assert(
      stateFor(TrimApplyMissingConfirmation).lastResult
        == RejectedTrimMissingConfirmation
    )
  run privacyMissingConfirmationRejected =
    assert(
      stateFor(PrivacyDeleteMissingConfirmation).lastResult
        == RejectedPrivacyMissingConfirmation
    )
}
