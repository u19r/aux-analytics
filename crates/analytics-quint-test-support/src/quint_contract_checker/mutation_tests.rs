use std::{
    collections::BTreeSet,
    fs,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::json;

use super::{
    mutation::{
        MutationDefinition, SourceTransform, TemporaryModel, apply_transform,
        counterexample::{read_bounded_bytes, read_bounded_json, validate_itf},
        definition,
        execution::{CounterexampleRun, ensure_temporary_model_unchanged, run_one},
        replay::independently_verify_invariant,
        validate_registered_transform, validate_typed_contract,
    },
    symbols::{DeclarationRole, declaration_at},
};
use crate::{
    constants::MUTATIONS,
    quint_contract_checker::{
        evidence::{CoverageEvidence, NotApplicableEvidence},
        inventory::Rule,
        plan::{Gate, PerformanceGate},
    },
};

fn fixture_root(label: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let root = std::env::temp_dir().join(format!("aux-quint-mutation-{label}-{nonce}"));
    fs::create_dir_all(root.join("specs")).expect("specs");
    fs::write(
        root.join("specs/model.qnt"),
        "module model {\n  var state: int\n  action init = state' = 0\n  action step = state' = \
         0\n  val inv = true\n}\n",
    )
    .expect("model");
    root
}

fn rule() -> Rule {
    Rule {
        rule_id: "R-MUTATION".to_owned(),
        family: "test".to_owned(),
        rule_summary: "mutation output".to_owned(),
        readable_model: "specs/model.qnt".to_owned(),
        invariant: "inv".to_owned(),
        witnesses: "-".to_owned(),
        hostile_trace: "HT-MUTATION".to_owned(),
        mutation_id: "M-TEST".to_owned(),
        mbt_model: "-".to_owned(),
        rust_owner: "test-owner".to_owned(),
        bdd_tags: "-".to_owned(),
        performance_gate: Gate::Fixed(PerformanceGate {
            seed: "0x1".to_owned(),
            samples: 1,
            steps: 1,
            description: "focused".to_owned(),
        }),
        evidence: CoverageEvidence::NotApplicable(NotApplicableEvidence {
            reason: "not-applicable".to_owned(),
            proof: "type=fixture".to_owned(),
        }),
    }
}

fn cli_rule() -> Rule {
    Rule {
        rule_id: "ANA-CLI-001".to_owned(),
        family: "interfaces".to_owned(),
        rule_summary: "CLI operation mapping".to_owned(),
        readable_model: "specs/cli_operations_mbt.qnt".to_owned(),
        invariant: "inv".to_owned(),
        witnesses: "read_only_witness run_witness resume_witness destructive_rejection_witness"
            .to_owned(),
        hostile_trace: "HT-A3-CLI-001".to_owned(),
        mutation_id: "M-A3-CLI-001".to_owned(),
        mbt_model: "specs/cli_operations_mbt.qnt".to_owned(),
        rust_owner: "analytics-cli".to_owned(),
        bdd_tags: "-".to_owned(),
        performance_gate: Gate::Fixed(PerformanceGate {
            seed: "0xA315".to_owned(),
            samples: 400,
            steps: 12,
            description: "CLI operation mapping".to_owned(),
        }),
        evidence: CoverageEvidence::NotApplicable(NotApplicableEvidence {
            reason: "focused mutation fixture".to_owned(),
            proof: "typed CLI mutation".to_owned(),
        }),
    }
}

fn source_position_rule() -> Rule {
    Rule {
        rule_id: "ANA-SP-004".to_owned(),
        family: "source-position".to_owned(),
        rule_summary: "source-position merge decisions".to_owned(),
        readable_model: "specs/source_position_mbt.qnt".to_owned(),
        invariant: "inv".to_owned(),
        witnesses: "insert_witness replace_witness delete_witness duplicate_witness stale_witness \
                    append_witness validation_witness"
            .to_owned(),
        hostile_trace: "HT-A1-SP-004".to_owned(),
        mutation_id: "M-A1-SP-004".to_owned(),
        mbt_model: "specs/source_position_mbt.qnt".to_owned(),
        rust_owner: "analytics-contract".to_owned(),
        bdd_tags: "-".to_owned(),
        performance_gate: Gate::Fixed(PerformanceGate {
            seed: "0xA105".to_owned(),
            samples: 4000,
            steps: 8,
            description: "source-position merge decisions".to_owned(),
        }),
        evidence: CoverageEvidence::NotApplicable(NotApplicableEvidence {
            reason: "focused mutation fixture".to_owned(),
            proof: "source-position action mutation".to_owned(),
        }),
    }
}

fn mutation(replacement: &'static str) -> MutationDefinition {
    MutationDefinition {
        id: "M-TEST",
        model: "specs/model.qnt",
        needle: "val inv = true",
        replacement,
        contract: None,
    }
}

fn trace_for(source: &Path, state: i64) -> serde_json::Value {
    json!({
        "#meta": {
            "format": "ITF",
            "source": source.display().to_string(),
            "status": "violation"
        },
        "vars": ["state"],
        "states": [{"state": {"#bigint": state.to_string()}}]
    })
}

#[test]
fn given_generic_marker_output_when_mutation_runs_then_evidence_is_rejected() {
    let repo = fixture_root("generic-output");
    let output = repo.join("counterexample.itf.json");
    fs::write(&output, "counterexample").expect("marker");

    let result = read_bounded_json(&output, 8 * 1024 * 1024);

    assert!(
        result
            .expect_err("a text marker must not count as a counterexample")
            .contains("JSON is invalid")
    );
    let _ = fs::remove_dir_all(repo);
}

#[test]
fn given_structured_violation_trace_when_mutation_runs_then_evidence_is_accepted() {
    let repo = fixture_root("structured-output");

    run_one(&repo, &rule(), &mutation("val inv = false")).expect("structured evidence");
    assert!(repo.join("specs/model.qnt").is_file());
    let _ = fs::remove_dir_all(repo);
}

#[test]
fn given_violation_marker_without_trace_when_mutation_runs_then_evidence_is_rejected() {
    let repo = fixture_root("incomplete-output");
    let model = repo.join("specs/model.qnt");
    let document = json!({
        "#meta": {
            "format": "ITF",
            "source": model.display().to_string(),
            "status": "violation"
        },
        "vars": [],
        "states": []
    });

    let result = validate_itf(&document, &model, 1);

    assert!(
        result
            .expect_err("incomplete ITF must not count as a counterexample")
            .contains("no variables")
    );
    let _ = fs::remove_dir_all(repo);
}

#[test]
fn given_runner_ignores_requested_invariant_when_mutation_runs_then_evidence_is_rejected() {
    let repo = fixture_root("invariant-ignored");
    let model = repo.join("specs/model.qnt");
    fs::write(
        &model,
        "module model {\n  var state: int\n  action init = state' = 0\n  action step = state' = \
         1\n  val inv = state == 1\n}\n",
    )
    .expect("model");
    let command = super::mutation::QuintCommand::discover_trusted().expect("installed quint");
    let document = trace_for(&model, 1);

    let result = independently_verify_invariant(&command, &model, &document, "inv");

    assert!(
        result
            .expect_err("a runner that ignores the requested invariant must fail")
            .contains("holds for every")
    );
    let _ = fs::remove_dir_all(repo);
}

#[test]
fn given_trace_replayed_from_other_invariant_when_mutation_runs_then_evidence_is_rejected() {
    let repo = fixture_root("other-invariant");
    let model = repo.join("specs/model.qnt");
    fs::write(
        &model,
        "module model {\n  var state: int\n  action init = state' = 0\n  action step = state' = \
         1\n  val inv = state == 1\n  val other = state == 0\n}\n",
    )
    .expect("model");
    let command = super::mutation::QuintCommand::discover_trusted().expect("installed quint");
    let document = trace_for(&model, 1);

    let result = independently_verify_invariant(&command, &model, &document, "inv");

    assert!(
        result
            .expect_err("a trace satisfying another invariant must fail")
            .contains("holds for every")
    );
    let _ = fs::remove_dir_all(repo);
}

#[test]
fn given_runner_tampers_with_temporary_model_when_mutation_runs_then_evidence_is_rejected() {
    let repo = fixture_root("temporary-tamper");
    let model = repo.join("specs/model.qnt");
    let expected_path = model.canonicalize().expect("model path");
    let expected_bytes = read_bounded_bytes(&model, crate::constants::MAX_COUNTEREXAMPLE_BYTES)
        .expect("model bytes");
    fs::write(&model, "tampered model\n").expect("tamper");

    let result = ensure_temporary_model_unchanged(
        &model,
        &expected_path,
        &expected_bytes,
        "temporary mutation model after run",
    );

    assert!(
        result
            .expect_err("a runner that tampers with its input must fail")
            .contains("temporary mutation model changed")
    );
    let _ = fs::remove_dir_all(repo);
}

#[test]
fn given_fabricated_unreachable_state_when_trace_is_replayed_then_evidence_is_rejected() {
    let repo = fixture_root("unreachable-state");
    let model = repo.join("specs/model.qnt");
    fs::write(
        &model,
        "module model {\n  var state: int\n  action init = state' = 0\n  action step = state' = \
         0\n  val inv = state == 1\n}\n",
    )
    .expect("model");
    let command = super::mutation::QuintCommand::discover_trusted().expect("installed quint");
    let rule = rule();
    let Gate::Fixed(gate) = &rule.performance_gate else {
        panic!("fixture gate");
    };
    let forged = trace_for(&model, 1);
    let replayed_path = repo.join("reachable.itf.json");

    let result = CounterexampleRun {
        model_path: &model,
        invariant: &rule.invariant,
        gate,
    }
    .verify_reachable_trace(&command, &forged, &replayed_path);

    let error = result.expect_err("an unreachable fabricated state must fail");
    assert!(
        error.contains("does not match the trusted reachable trace"),
        "unexpected error: {error}"
    );
    let _ = fs::remove_dir_all(repo);
}

#[test]
fn given_a3_registry_when_compared_then_independent_expected_contracts_match() {
    let expected = [
        (
            "M-A3-BACK-003",
            "specs/backfill_capability_discovery_mbt.qnt",
            "unsupported_witness",
        ),
        (
            "M-A3-BACK-004",
            "specs/backfill_planning.qnt",
            "strategy_chosen_witness",
        ),
        (
            "M-A3-BACK-005",
            "specs/backfill_planning_bughunt.qnt",
            "rejected_unsupported_witness",
        ),
        (
            "M-A3-BACK-006",
            "specs/backfill_planning_mbt.qnt",
            "planned_witness",
        ),
        (
            "M-A3-CLI-001",
            "specs/cli_operations_mbt.qnt",
            "resume_witness",
        ),
        (
            "M-A3-OPS-003",
            "specs/operations_mbt.qnt",
            "cancellation_witness",
        ),
        (
            "M-A3-PRIVFIX-002",
            "specs/privacy_fix_mbt.qnt",
            "apply_fence_rejection_witness",
        ),
        (
            "M-A3-RATE-001",
            "specs/rate_limit_mbt.qnt",
            "within_budget_witness",
        ),
        (
            "M-A3-BACKUP-002",
            "specs/raw_backup_mbt.qnt",
            "replay_complete_witness",
        ),
        (
            "M-A3-TFIX-002",
            "specs/table_fix_mbt.qnt",
            "applied_witness",
        ),
        ("M-A3-TRIM-002", "specs/trim_mbt.qnt", "applied_witness"),
        (
            "M-A3-CONFIG-001",
            "specs/config_api.qnt",
            "lambda_argument_precedence_witness",
        ),
        (
            "M-A3-CONFIG-002",
            "specs/config_api.qnt",
            "explicit_query_limit_witness",
        ),
    ];

    let registered = MUTATIONS
        .iter()
        .filter(|mutation| mutation.id.starts_with("M-A3-"))
        .collect::<Vec<_>>();
    assert_eq!(registered.len(), expected.len());

    let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    for (id, model, witness) in expected {
        let mutation = definition(id).expect("indexed A3 mutation");
        assert_eq!(mutation.model, model);
        let contract = mutation.contract.expect("typed A3 contract");
        assert_eq!(contract.invariant, "inv");
        assert_eq!(contract.witness, witness);

        let source = fs::read_to_string(repository.join(model)).expect("model source");
        let transformed = apply_transform(&source, SourceTransform::from(mutation), id)
            .expect("typed transform should match exactly once");
        assert_ne!(source, transformed, "{id} must change source bytes");
    }
}

#[test]
fn given_current_registry_when_compared_then_inventory_matches_and_targets_are_unique() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let inventory = fs::read_to_string(repository.join("specs/model-inventory.tsv"))
        .expect("current inventory");
    let inventory_ids = inventory
        .lines()
        .skip(1)
        .filter_map(|line| line.split('\t').nth(7))
        .filter(|mutation| mutation.starts_with("M-"))
        .collect::<BTreeSet<_>>();
    let registry_ids = MUTATIONS
        .iter()
        .map(|mutation| mutation.id)
        .collect::<BTreeSet<_>>();

    assert_eq!(inventory_ids.len(), 44);
    assert_eq!(registry_ids.len(), MUTATIONS.len());
    assert_eq!(registry_ids, inventory_ids);
    for mutation in MUTATIONS {
        let source = fs::read_to_string(repository.join(mutation.model)).expect("model source");
        validate_registered_transform(&source, mutation)
            .expect("registered mutations must target behavior declarations");
        let transformed = apply_transform(&source, SourceTransform::from(mutation), mutation.id)
            .expect("every registered mutation must target one source expression");
        assert_ne!(
            source,
            transformed,
            "{id} must change source bytes",
            id = mutation.id
        );
    }

    assert!(definition("M-A2-CHECK-002").is_some());
    assert!(definition("M-A2-POLL-009").is_none());
    assert!(definition("M-A2-POLL-010").is_some());
}

#[test]
fn given_legacy_mutation_when_bound_then_target_is_named_action_state() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let expected = [
        ("M-A1-MAN-001", "CheckValidSimpleManifest"),
        ("M-A1-MAN-002", "CheckUnsupportedVersion"),
        ("M-A1-MPX-001", "GenerateManifestProjectionCombination"),
        ("M-A1-MPX-002", "GenerateManifestProjectionCombination"),
        ("M-A1-ING-001", "InsertUser"),
        ("M-A1-ING-002", "IngestRecord"),
        ("M-A1-PROJ-001", "ProjectAliasPath"),
        ("M-A1-QRY-001", "GenerateRawQueryCombination"),
        ("M-A1-QRY-002", "GenerateStructuredQueryCombination"),
        ("M-A1-PRIV-001", "SanitizeDeniedKeyRecord"),
        ("M-A1-PRIV-002", "PrivacyScenario"),
    ];

    for (id, action_name) in expected {
        let mutation = definition(id).expect("legacy mutation is registered");
        let source = fs::read_to_string(repository.join(mutation.model)).expect("model source");
        let offset = source
            .find(SourceTransform::from(mutation).needle)
            .expect("legacy transform target");
        let declaration = declaration_at(&source, offset).expect("action declaration");
        let target_tail = source.get(offset..).expect("target offset boundary");
        let target_line_end = target_tail
            .find('\n')
            .map_or(source.len(), |relative| offset + relative);
        let target_line = source
            .get(offset..target_line_end)
            .expect("target line boundaries");
        let action_start = target_line.find("action ").map_or_else(
            || {
                source
                    .get(..offset)
                    .and_then(|prefix| prefix.rfind("action "))
                    .expect("action header")
            },
            |relative| offset + relative,
        );
        let action_tail = source.get(action_start..).expect("action offset boundary");
        let action_end = action_tail
            .find('\n')
            .map_or(source.len(), |relative| action_start + relative);
        let action_header = source
            .get(action_start..action_end)
            .expect("action header boundaries");

        assert_eq!(declaration.role, DeclarationRole::Behavior, "{id}");
        assert!(
            action_header.contains(action_name),
            "{id} must target action {action_name}, found {action_header:?}"
        );
        assert!(
            SourceTransform::from(mutation).replacement != SourceTransform::from(mutation).needle,
            "{id} must change action state behavior"
        );
    }
}

#[test]
fn given_non_ascii_comment_when_action_binding_is_parsed_then_declaration_is_preserved() {
    let source = "module model {\n  // café keeps byte traversal honest\n  var state: int\n  \
                  action step = {\n    state' = 1\n  }\n  val inv = state >= 0\n}\n";
    let mutation = MutationDefinition {
        id: "M-UNICODE-ACTION",
        model: "specs/model.qnt",
        needle: "state' = 1",
        replacement: "state' = 0",
        contract: None,
    };
    let offset = source.find(mutation.needle).expect("action state target");
    let declaration = declaration_at(source, offset).expect("action declaration");

    assert_eq!(declaration.role, DeclarationRole::Behavior);
    assert_eq!(declaration.name, "step");
}

#[test]
fn given_cli_result_mutation_when_contract_runs_then_invariant_fails_with_resume_witness() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mutation = definition("M-A3-CLI-001").expect("CLI mutation");
    let rule = cli_rule();
    let source_path = repository.join(mutation.model);
    let source = fs::read_to_string(&source_path).expect("CLI model source");
    let transformed = apply_transform(&source, SourceTransform::from(mutation), mutation.id)
        .expect("CLI transform should match exactly once");
    let temporary = TemporaryModel::try_new(mutation.model).expect("temporary CLI model");
    fs::write(&temporary.path, transformed).expect("mutated CLI model");

    let command = super::mutation::QuintCommand::discover_trusted().expect("installed quint");
    let model_path = temporary.path.to_string_lossy().into_owned();
    let arguments = vec![
        "run".to_owned(),
        model_path,
        "--witnesses=resume_witness".to_owned(),
        "--max-samples=400".to_owned(),
        "--max-steps=12".to_owned(),
        "--seed=0xA315".to_owned(),
    ];
    let witness_output = command.run_output(&arguments).expect("run CLI witness");
    assert!(
        witness_output.status.success(),
        "resume witness run failed: {}",
        String::from_utf8_lossy(&witness_output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&witness_output.stdout).contains("resume_witness was witnessed"),
        "mutated CLI model did not report a reachable resume witness: {}",
        String::from_utf8_lossy(&witness_output.stdout)
    );

    run_one(&repository, &rule, mutation)
        .expect("the exact result mutation must produce an invariant counterexample");
}

#[test]
fn given_source_position_action_mutation_when_contract_runs_then_invariant_fails() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mutation = definition("M-A1-SP-004").expect("source-position mutation");
    let transform = SourceTransform::from(mutation);
    assert!(transform.needle.contains("existingPosition' = existing"));
    assert!(!transform.needle.contains("resultMatchesExpected"));
    assert!(!transform.needle.contains("expected"));
    assert!(!transform.replacement.contains("resultMatchesExpected"));
    assert!(!transform.replacement.contains("expected"));

    let source_path = repository.join(mutation.model);
    let source = fs::read_to_string(&source_path).expect("source-position model");
    let transformed = apply_transform(&source, transform, mutation.id)
        .expect("source-position action target should match exactly once");
    assert_ne!(source, transformed);
    assert!(!transformed.contains("not(resultMatchesExpected)"));

    run_one(&repository, &source_position_rule(), mutation)
        .expect("the action mutation must produce an invariant counterexample");
}

#[test]
fn given_two_row_table_fix_mutation_when_bound_then_only_two_row_target_changes() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mutation = definition("M-A3-TFIX-002").expect("two-row table-fix mutation");
    let transform = SourceTransform::from(mutation);
    assert!(transform.needle.contains("ApplyTwoCurrentSource"));
    assert!(!transform.needle.contains("ApplyCurrentSource =>"));

    let source = fs::read_to_string(repository.join(mutation.model)).expect("table-fix model");
    let transformed = apply_transform(&source, transform, mutation.id)
        .expect("two-row table-fix target should match exactly once");
    assert_ne!(source, transformed);
    assert!(transformed.contains(
        "| ApplyCurrentSource => {\n          lastOutcome: Applied({\n            selectedRows: \
         1,\n            repairedRows: 1,\n            destinationMutated: true,\n          }),"
    ));
    assert!(
        transformed.contains(
            "| ApplyTwoCurrentSource => {\n          lastOutcome: Applied({\n            \
             selectedRows: 2,\n            repairedRows: 2,\n            destinationMutated: \
             false,\n          }),"
        )
    );
}

#[test]
fn given_unindexed_witness_when_contract_is_validated_then_binding_is_rejected() {
    let mutation = definition("M-A3-BACK-003").expect("A3 mutation");
    let mut rule = rule();
    rule.witnesses = "some_other_witness".to_owned();
    rule.mutation_id = mutation.id.to_owned();
    rule.readable_model = mutation.model.to_owned();

    let error = validate_typed_contract(&rule, mutation)
        .expect_err("a mutation must not run against an unrelated witness");
    assert!(error.contains("unsupported_witness"));
}

#[test]
fn given_wrong_inventory_binding_when_contract_is_validated_then_execution_is_rejected() {
    let mutation = definition("M-A3-BACK-003").expect("A3 mutation");
    let mut rule = rule();

    let error = validate_typed_contract(&rule, mutation)
        .expect_err("a mutation must match the inventory mutation id");
    assert!(error.contains("not bound to inventory mutation M-TEST"));

    rule.mutation_id = mutation.id.to_owned();
    rule.readable_model = "specs/other.qnt".to_owned();
    rule.witnesses = "unsupported_witness".to_owned();
    let error = validate_typed_contract(&rule, mutation)
        .expect_err("a mutation must match the inventory model");
    assert!(error.contains("inventory model specs/other.qnt"));
}

#[test]
fn given_typed_mutation_with_wrong_source_model_when_validated_then_binding_is_rejected() {
    let mutation = definition("M-A3-BACK-003").expect("A3 mutation");
    let wrong_model = MutationDefinition {
        id: mutation.id,
        model: "specs/other.qnt",
        needle: mutation.needle,
        replacement: mutation.replacement,
        contract: mutation.contract,
    };
    let mut rule = rule();
    rule.mutation_id = mutation.id.to_owned();
    rule.readable_model = mutation.model.to_owned();
    rule.invariant = "inv".to_owned();
    rule.witnesses = "unsupported_witness".to_owned();

    let error = validate_typed_contract(&rule, &wrong_model)
        .expect_err("typed mutation must bind to its declared source model");
    assert!(error.contains("typed contract requires specs/other.qnt"));
}

#[test]
fn given_missing_or_ambiguous_target_when_transformed_then_source_is_rejected() {
    let transform = SourceTransform {
        needle: "target",
        replacement: "replacement",
    };
    let missing = apply_transform("unrelated", transform, "M-A3-TEST");
    assert_eq!(
        missing.expect_err("missing target must be rejected"),
        "M-A3-TEST expected one guard target, found 0"
    );

    let ambiguous = apply_transform("target target", transform, "M-A3-TEST");
    assert_eq!(
        ambiguous.expect_err("ambiguous target must be rejected"),
        "M-A3-TEST expected one guard target, found 2"
    );
}

#[test]
fn given_registered_mutation_targeting_oracle_when_validated_then_it_is_rejected() {
    let source = "module model {\n  var state: int\n  val inv = state >= 0\n}\n";
    let mutation = MutationDefinition {
        id: "M-TEST-ORACLE",
        model: "specs/model.qnt",
        needle: "state >= 0",
        replacement: "state < 0",
        contract: None,
    };

    let error = validate_registered_transform(source, &mutation)
        .expect_err("oracle mutations must not be accepted");
    assert!(error.contains("invariant/oracle declaration inv"));
}

#[test]
fn given_registered_mutation_targeting_action_when_validated_then_it_is_accepted() {
    let source = "module model {\n  var state: int\n  action step = {\n    state' = 1\n  }\n  val \
                  inv = state >= 0\n}\n";
    let mutation = MutationDefinition {
        id: "M-TEST-ACTION",
        model: "specs/model.qnt",
        needle: "state' = 1",
        replacement: "state' = 0",
        contract: None,
    };

    validate_registered_transform(source, &mutation).expect("action mutations should be accepted");
}

#[test]
fn given_mutation_failure_when_executed_then_repository_source_is_unchanged() {
    let repo = fixture_root("source-cleanup");
    let model = repo.join("specs/model.qnt");
    let before = fs::read(&model).expect("source before mutation");

    run_one(&repo, &rule(), &mutation("val inv = false")).expect("structured evidence");

    assert_eq!(
        fs::read(&model).expect("source after mutation"),
        before,
        "mutation execution must never rewrite the repository model"
    );
    let _ = fs::remove_dir_all(repo);
}

#[test]
fn given_temporary_model_when_dropped_then_mutated_source_is_removed() {
    let (directory, path) = {
        let temporary = TemporaryModel::try_new("specs/model.qnt").expect("temporary model");
        fs::write(&temporary.path, "mutated").expect("mutated source");
        (temporary.directory.clone(), temporary.path.clone())
    };

    assert!(!path.exists());
    assert!(!directory.exists());
}
