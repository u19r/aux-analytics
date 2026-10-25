use crate::quint_contract_checker::mutation::{MutationContract, MutationDefinition};
// Inventory layout and ordered audit phases.

pub(crate) const EXPECTED_HEADER: &str = concat!(
    "rule_id\tfamily\trule_summary\treadable_model\tinvariant\t",
    "witnesses\thostile_trace\tmutation_id\tmbt_model\trust_owner\t",
    "rust_adapter\tbdd_tags\tperformance_gate\tstatus"
);
pub(crate) const PHASES: &[&str] = &[
    "F-A1", "I-A1", "U-A1", "B-A1", "R-A1", "P-A1", "A-A1", "F-A2", "I-A2", "U-A2", "B-A2", "R-A2",
    "P-A2", "A-A2", "F-A3", "I-A3", "U-A3", "B-A3", "R-A3", "P-A3", "A-A3",
];

// Formal plan budgets and deferred adapter scheduling.
pub(crate) const MAX_STANDARD_SAMPLES: usize = 4_000;
pub(crate) const MAX_STANDARD_STEPS: usize = 16;
pub(crate) const DEEP_SAMPLE_MULTIPLIER: usize = 5;
pub(crate) const DEEP_STEP_MULTIPLIER: usize = 2;
pub(crate) const MAX_DEEP_SAMPLES: usize = MAX_STANDARD_SAMPLES * DEEP_SAMPLE_MULTIPLIER;
pub(crate) const MAX_DEEP_STEPS: usize = MAX_STANDARD_STEPS * DEEP_STEP_MULTIPLIER;
pub(crate) const MAX_PLAN_RUNS: usize = 256;
pub(crate) const MAX_PLAN_WORK: usize = 50_000_000;
pub(crate) const EXPLICIT_DEFERRED_ADAPTER_SKIPS: &[(&str, &str)] = &[(
    "crates/analytics-engine/src/quint_privacy_policy_tests.rs",
    "given_quint_privacy_policy_model_when_replayed_then_full_state_matches",
)];

// Source and counterexample resource bounds.
pub(crate) const MAX_MODEL_BYTES: u64 = 1_048_576;
pub(crate) const MAX_COUNTEREXAMPLE_BYTES: u64 = 8 * 1024 * 1024;
pub(crate) const MAX_INDEPENDENT_EVALUATION_OUTPUT_BYTES: u64 = 64 * 1024;
pub(crate) const MAX_INDEPENDENT_REPLAY_ARGUMENT_BYTES: usize = 1024 * 1024;

// Executable mutation registry; expected inventory and witnesses remain
// separate test oracles.
pub(crate) const MUTATIONS: &[MutationDefinition] = &[
    MutationDefinition {
        id: "M-A1-MAN-001",
        model: "specs/manifest.qnt",
        needle: "      lastCase' = ValidSimpleManifest,",
        replacement: "      lastCase' = NotCheckedManifest,",
        contract: None,
    },
    MutationDefinition {
        id: "M-A1-MAN-002",
        model: "specs/manifest.qnt",
        needle: "      lastCase' = UnsupportedVersionManifest,",
        replacement: "      lastCase' = ValidSimpleManifest,",
        contract: None,
    },
    MutationDefinition {
        id: "M-A1-MPX-001",
        model: "specs/manifest_projection_bughunt.qnt",
        needle: "      reservedColumnProjected' = column_is_reserved,",
        replacement: "      reservedColumnProjected' = not(column_is_reserved),",
        contract: None,
    },
    MutationDefinition {
        id: "M-A1-MPX-002",
        model: "specs/manifest_projection_bughunt.qnt",
        needle: "      outputSourcesUnique' = outputSourcesAreUnique(has_projection_column, \
                 has_document_column),",
        replacement: "      outputSourcesUnique' = \
                      not(outputSourcesAreUnique(has_projection_column, has_document_column)),",
        contract: None,
    },
    MutationDefinition {
        id: "M-A1-SP-004",
        model: "specs/source_position_mbt.qnt",
        needle: "existingPosition' = existing,",
        replacement: "existingPosition' = NoExisting,",
        contract: None,
    },
    MutationDefinition {
        id: "M-A1-ING-001",
        model: "specs/ingestion.qnt",
        needle: "    nondet user_id = userIds.oneOf()",
        replacement: "    nondet user_id = Set(\"unregistered-user\").oneOf()",
        contract: None,
    },
    MutationDefinition {
        id: "M-A1-ING-002",
        model: "specs/ingestion_lifecycle_bughunt.qnt",
        needle: "      lastOperation' = Ingest,",
        replacement: "      lastOperation' = Delete,",
        contract: None,
    },
    MutationDefinition {
        id: "M-A1-ING-003",
        model: "specs/ingestion_lifecycle_bughunt.qnt",
        needle: "val nextRowSequences = if (stale) rowSequences else if (acceptedRecord(new_kind) \
                 or durable_sequence_known) intMapPut(rowSequences, row_id, \
                 updateSequence(durable_seq, new_seq)) else rowSequences",
        replacement: "val nextRowSequences = if (stale) rowSequences else intMapPut(rowSequences, \
                      row_id, updateSequence(durable_seq, new_seq))",
        contract: None,
    },
    MutationDefinition {
        id: "M-A1-PROJ-001",
        model: "specs/projection.qnt",
        needle: "      lastCase' = AliasPathProjection,",
        replacement: "      lastCase' = NotCheckedProjection,",
        contract: None,
    },
    MutationDefinition {
        id: "M-A1-QRY-001",
        model: "specs/query.qnt",
        needle: "    rawReadOnlyPrefix' = isReadOnlyPrefix(prefix),",
        replacement: "    rawReadOnlyPrefix' = false,",
        contract: None,
    },
    MutationDefinition {
        id: "M-A1-QRY-002",
        model: "specs/query.qnt",
        needle: "    structuredHasTableName' = tableNamePresent,",
        replacement: "    structuredHasTableName' = false,",
        contract: None,
    },
    MutationDefinition {
        id: "M-A1-QSEC-001",
        model: "specs/query_security_bughunt.qnt",
        needle: "modifiesData' = observedDurableEffect(attack, security_result),",
        replacement: "modifiesData' = attackAttemptsMutation(attack),",
        contract: None,
    },
    MutationDefinition {
        id: "M-A1-QSEC-002",
        model: "specs/query_security_bughunt.qnt",
        needle: "action step = any {",
        replacement: "action step = any { UnsafeCrossTenantExecution,",
        contract: None,
    },
    MutationDefinition {
        id: "M-A1-PRIV-001",
        model: "specs/privacy_policy.qnt",
        needle: "  action SanitizeDeniedKeyRecord = all {\n    result' = \
                 privacyResultFor(DeniedKeyCase),\n    reachedRawBackup' = true,\n    \
                 reachedProjection' = true,\n    rawBackupDeniedDataAbsent' = true,",
        replacement: "  action SanitizeDeniedKeyRecord = all {\n    result' = \
                      privacyResultFor(DeniedKeyCase),\n    reachedRawBackup' = true,\n    \
                      reachedProjection' = true,\n    rawBackupDeniedDataAbsent' = false,",
        contract: None,
    },
    MutationDefinition {
        id: "M-A1-PRIV-002",
        model: "specs/privacy_policy_mbt.qnt",
        needle: "      lastReport' = reportFor(privacyCase),",
        replacement: "      lastReport' = defaultReport,",
        contract: None,
    },
    MutationDefinition {
        id: "M-A2-POLL-001",
        model: "specs/polling_api.qnt",
        needle: "durableCheckpointCount' = durableCheckpointCount + 2,",
        replacement: "durableCheckpointCount' = durableCheckpointCount,",
        contract: None,
    },
    MutationDefinition {
        id: "M-A2-POLL-004",
        model: "specs/polling_restart_resume.qnt",
        needle: "lastResult' = AwsWrongTableIgnored,",
        replacement: "lastResult' = AwsRecordCheckpointSelected,",
        contract: None,
    },
    MutationDefinition {
        id: "M-A2-POLL-005",
        model: "specs/polling_api.qnt",
        needle: "lastHealthDurableCheckpointCount' = durableCheckpointCount + 2,",
        replacement: "lastHealthDurableCheckpointCount' = durableCheckpointCount,",
        contract: None,
    },
    MutationDefinition {
        id: "M-A2-POLL-006",
        model: "specs/polling_restart_resume.qnt",
        needle: "resumedPosition' = \"seq-42\",",
        replacement: "resumedPosition' = \"\",",
        contract: None,
    },
    MutationDefinition {
        id: "M-A2-POLL-007",
        model: "specs/polling_storage.qnt",
        needle: "auxCursorAfter' = CursorValue(\"prior-page\"),",
        replacement: "auxCursorAfter' = NoCursor,",
        contract: None,
    },
    MutationDefinition {
        id: "M-A2-POLL-008",
        model: "specs/polling_storage.qnt",
        needle: "iteratorCheckpointDurable' = durableIteratorCheckpointPolicy,",
        replacement: "iteratorCheckpointDurable' = true,",
        contract: None,
    },
    MutationDefinition {
        id: "M-A2-CHECK-002",
        model: "specs/check_mbt.qnt",
        needle: "      operationId: \"\",",
        replacement: "      operationId: \"forged\",",
        contract: None,
    },
    MutationDefinition {
        id: "M-A2-POLL-010",
        model: "specs/polling_api.qnt",
        needle: "lastPersistableCheckpointCount' = 0,\n      lastBatchClean' = true,\n      \
                 lastCheckpointSaveSucceeded' = true,\n      lastLeasePermitted' = \
                 leasePermitted,\n      lastCommitted' = false,\n      lastCurrentBatchHasErrors' \
                 = false,",
        replacement: "lastPersistableCheckpointCount' = 0,\n      lastBatchClean' = true,\n      \
                      lastCheckpointSaveSucceeded' = true,\n      lastLeasePermitted' = \
                      leasePermitted,\n      lastCommitted' = true,\n      \
                      lastCurrentBatchHasErrors' = false,",
        contract: None,
    },
    MutationDefinition {
        id: "M-A2-RET-001",
        model: "specs/retention_mbt.qnt",
        needle: "lastResult' = StaticExpiryResult,",
        replacement: "lastResult' = MissingMarkerResult,",
        contract: None,
    },
    MutationDefinition {
        id: "M-A2-RET-002",
        model: "specs/retention_mbt.qnt",
        needle: "lastResult' = MissingMarkerResult,\n    rowCount' = 1,\n    missingCount' = 1,",
        replacement: "lastResult' = MissingMarkerResult,\n    rowCount' = 1,\n    missingCount' = \
                      2,",
        contract: None,
    },
    MutationDefinition {
        id: "M-A2-RET-003",
        model: "specs/retention_mbt.qnt",
        needle: "lastResult' = SweepIncompleteResult,\n    rowCount' = 1,\n    missingCount' = \
                 0,\n    staticExpiryExact' = false,\n    deletedCount' = 1,\n    \
                 remainingExpiredCount' = 1,\n    receiptCount' = 1,\n    complete' = false,",
        replacement: "lastResult' = SweepIncompleteResult,\n    rowCount' = 1,\n    missingCount' \
                      = 0,\n    staticExpiryExact' = false,\n    deletedCount' = 1,\n    \
                      remainingExpiredCount' = 1,\n    receiptCount' = 1,\n    complete' = true,",
        contract: None,
    },
    MutationDefinition {
        id: "M-A2-RET-004",
        model: "specs/retention_mbt.qnt",
        needle:
            "lastResult' = SweepCompleteResult,\n    rowCount' = 0,\n    missingCount' = 0,\n    \
             staticExpiryExact' = false,\n    deletedCount' = 1,",
        replacement: "lastResult' = SweepCompleteResult,\n    rowCount' = 0,\n    missingCount' = \
                      0,\n    staticExpiryExact' = false,\n    deletedCount' = 2,",
        contract: None,
    },
    MutationDefinition {
        id: "M-A2-RET-005",
        model: "specs/retention_mbt.qnt",
        needle:
            "lastResult' = SweepCompleteResult,\n    rowCount' = 0,\n    missingCount' = 0,\n    \
             staticExpiryExact' = false,\n    deletedCount' = 1,\n    remainingExpiredCount' = 0,",
        replacement: "lastResult' = SweepCompleteResult,\n    rowCount' = 0,\n    missingCount' = \
                      0,\n    staticExpiryExact' = false,\n    deletedCount' = 1,\n    \
                      remainingExpiredCount' = 1,",
        contract: None,
    },
    MutationDefinition {
        id: "M-A2-SB-001",
        model: "specs/source_batch_integration.qnt",
        needle: "durableCheckpointCount' = durableCheckpointCount + persistable,",
        replacement: "durableCheckpointCount' = durableCheckpointCount,",
        contract: None,
    },
    MutationDefinition {
        id: "M-A2-SB-002",
        model: "specs/source_batch_integration.qnt",
        needle: "lastIteratorPersisted' = iteratorCheckpointDurablePolicy,",
        replacement: "lastIteratorPersisted' = true,",
        contract: None,
    },
    MutationDefinition {
        id: "M-A2-SB-003",
        model: "specs/source_batch_integration.qnt",
        needle: "sourceBlocked' = true,",
        replacement: "sourceBlocked' = false,",
        contract: None,
    },
    MutationDefinition {
        id: "M-A3-BACK-003",
        model: "specs/backfill_capability_discovery_mbt.qnt",
        needle: "if (not(discoveryCase.metadataReachable)) RejectedPartialUnsupported",
        replacement: "if (not(discoveryCase.metadataReachable)) RejectedUnsupported",
        contract: Some(MutationContract {
            invariant: "inv",
            witness: "unsupported_witness",
        }),
    },
    MutationDefinition {
        id: "M-A3-BACK-004",
        model: "specs/backfill_planning.qnt",
        needle: "identity and metadata and destination and method != StreamOnly",
        replacement: "identity and metadata and destination",
        contract: Some(MutationContract {
            invariant: "inv",
            witness: "strategy_chosen_witness",
        }),
    },
    MutationDefinition {
        id: "M-A3-BACK-005",
        model: "specs/backfill_planning_bughunt.qnt",
        needle: "deleteProofRecorded' = canProveDeletes(sourceKind),",
        replacement: "deleteProofRecorded' = not(canProveDeletes(sourceKind)),",
        contract: Some(MutationContract {
            invariant: "inv",
            witness: "rejected_unsupported_witness",
        }),
    },
    MutationDefinition {
        id: "M-A3-BACK-006",
        model: "specs/backfill_planning_mbt.qnt",
        needle: "else if (planCase.requestedMethod == RequestedAuxStorageEnumeration) \
                 PlannedAuxStorageEnumeration",
        replacement: "else if (planCase.requestedMethod == RequestedAuxStorageEnumeration) \
                      PlannedRawBackupReplay",
        contract: Some(MutationContract {
            invariant: "inv",
            witness: "planned_witness",
        }),
    },
    MutationDefinition {
        id: "M-A3-CLI-001",
        model: "specs/cli_operations_mbt.qnt",
        needle: "          lastResult: ParsedRun,",
        replacement: "          lastResult: ParsedResume,",
        contract: Some(MutationContract {
            invariant: "inv",
            witness: "resume_witness",
        }),
    },
    MutationDefinition {
        id: "M-A3-OPS-003",
        model: "specs/operations_mbt.qnt",
        needle: "else if (opAction == AcceptCancel) Cancelled",
        replacement: "else if (opAction == AcceptCancel) Succeeded",
        contract: Some(MutationContract {
            invariant: "inv",
            witness: "cancellation_witness",
        }),
    },
    MutationDefinition {
        id: "M-A3-PRIVFIX-002",
        model: "specs/privacy_fix_mbt.qnt",
        needle: "result: DestinationFenceUnavailable,\n    scannedObjects: 0,\n    \
                 taintedObjects: 0,\n    rewrittenObjects: 0,\n    destinationMutated: false,",
        replacement: "result: DestinationFenceUnavailable,\n    scannedObjects: 0,\n    \
                      taintedObjects: 0,\n    rewrittenObjects: 0,\n    destinationMutated: true,",
        contract: Some(MutationContract {
            invariant: "inv",
            witness: "apply_fence_rejection_witness",
        }),
    },
    MutationDefinition {
        id: "M-A3-RATE-001",
        model: "specs/rate_limit_mbt.qnt",
        needle: "else if (rateLimitCase == OverRowsBudget) {\n      allowed: false,\n      \
                 waitMs: 2500,\n      retryDelayMs: 0,\n      batchMayStart: false,\n      \
                 policyRowsLimit: 100,\n    }",
        replacement: "else if (rateLimitCase == OverRowsBudget) {\n      allowed: false,\n      \
                      waitMs: 2500,\n      retryDelayMs: 0,\n      batchMayStart: true,\n      \
                      policyRowsLimit: 100,\n    }",
        contract: Some(MutationContract {
            invariant: "inv",
            witness: "within_budget_witness",
        }),
    },
    MutationDefinition {
        id: "M-A3-BACKUP-002",
        model: "specs/raw_backup_mbt.qnt",
        needle: "result: ReplayComplete,\n      recordCount: 1,\n      checksumRejected: false,",
        replacement: "result: ReplayComplete,\n      recordCount: 1,\n      checksumRejected: \
                      true,",
        contract: Some(MutationContract {
            invariant: "inv",
            witness: "replay_complete_witness",
        }),
    },
    MutationDefinition {
        id: "M-A3-TFIX-002",
        model: "specs/table_fix_mbt.qnt",
        needle: "| ApplyTwoCurrentSource => {\n          lastOutcome: Applied({\n            \
                 selectedRows: 2,\n            repairedRows: 2,\n            destinationMutated: \
                 true,\n          }),",
        replacement: "| ApplyTwoCurrentSource => {\n          lastOutcome: Applied({\n            \
                      selectedRows: 2,\n            repairedRows: 2,\n            \
                      destinationMutated: false,\n          }),",
        contract: Some(MutationContract {
            invariant: "inv",
            witness: "applied_witness",
        }),
    },
    MutationDefinition {
        id: "M-A3-TRIM-002",
        model: "specs/trim_mbt.qnt",
        needle:
            "| ApplyRowKeysWithConfirmation => {\n          lastOutcome: Applied({\n            \
             candidateRows: 1,\n            rowsDeleted: 1,\n            destinationMutated: \
             true,\n          }),",
        replacement:
            "| ApplyRowKeysWithConfirmation => {\n          lastOutcome: Applied({\n            \
             candidateRows: 1,\n            rowsDeleted: 1,\n            destinationMutated: \
             false,\n          }),",
        contract: Some(MutationContract {
            invariant: "inv",
            witness: "applied_witness",
        }),
    },
    MutationDefinition {
        id: "M-A3-CONFIG-001",
        model: "specs/config_api.qnt",
        needle: "if (input.manifestArgumentPresent) ExplicitArgument",
        replacement: "if (input.manifestArgumentPresent) Missing",
        contract: Some(MutationContract {
            invariant: "inv",
            witness: "lambda_argument_precedence_witness",
        }),
    },
    MutationDefinition {
        id: "M-A3-CONFIG-002",
        model: "specs/config_api.qnt",
        needle: "ingestRouteEnabled: routeEnabled(input, expectedResult(input)),",
        replacement: "ingestRouteEnabled: not(routeEnabled(input, expectedResult(input))),",
        contract: Some(MutationContract {
            invariant: "inv",
            witness: "explicit_query_limit_witness",
        }),
    },
];

// Deterministic test fixtures and performance probe settings.

#[cfg(test)]
pub(crate) const FRESH_TRACE_SPEC: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../specs/cli_operations_mbt.qnt"
);
#[cfg(test)]
pub(crate) const PLANNER_PAIRED_SAMPLES: usize = 3;
#[cfg(test)]
pub(crate) const PLANNER_SIZES: [usize; 4] = [1, 25, 100, 500];
#[cfg(test)]
pub(crate) const PLANNER_WARMUP_RUNS: usize = 2;
#[cfg(test)]
pub(crate) const PLANNER_MEASURE_REPETITIONS: usize = 32;
#[cfg(test)]
pub(crate) const PLANNER_TIMING_TRIALS: usize = 3;
#[cfg(test)]
pub(crate) const PLANNER_MIN_ALLOCATION_CPU_DELTA_PERCENT: f64 = 0.0;
#[cfg(test)]
pub(crate) const INVENTORY_DEFERRED_MARKERS: [&str; 6] = [
    "pending F-A2",
    "pending F-A3",
    "placeholder F-A2",
    "placeholder F-A3",
    "deferred F-A2",
    "deferred F-A3",
];
#[cfg(test)]
pub(crate) const INVENTORY_STATUS_VARIANTS: [&str; 15] = [
    "model-only",
    "not-applicable",
    "unknown",
    "covered ",
    "Covered",
    "COVERED",
    "covered?",
    "-",
    "",
    "pending",
    "deferred",
    "model_only",
    "model-only ",
    "not-applicable ",
    "N/A",
];
