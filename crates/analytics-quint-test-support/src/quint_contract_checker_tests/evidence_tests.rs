use std::path::Path;

use super::inventory_fixture::InventoryFixture;
use crate::quint_contract_checker::{
    CheckReport, check_repository, identifier_present, model_symbol_present, parse_evidence,
    parse_iso_date, utc_date_with_offset,
};

#[test]
fn given_current_inventory_when_checked_then_contract_is_valid() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    assert_eq!(
        check_repository(&repository, "A-A3").expect("inventory should pass"),
        CheckReport {
            rules: 44,
            readable_models: 28,
        }
    );
}

#[test]
fn given_unknown_bdd_tag_when_checker_runs_then_mapping_is_rejected() {
    let fixture = InventoryFixture::new("bdd-unknown");
    fixture.write_model("manifest", "inv", "witness");
    fixture.write_feature("@F101\nFeature: analytics\n\n@F101_S001\nScenario: valid\n");
    fixture.write_inventory(&[InventoryFixture::with_bdd_tags(
        fixture.row(
            "manifest",
            "M-A1-MAN-001",
            "not-applicable; proof=type=fixture",
            "HT-BDD-UNKNOWN",
            "fixed seed 0x1; 1 samples; 1 steps; focused",
            "not-applicable",
        ),
        "@F101_S999",
    )]);

    let failure = check_repository(&fixture.root, "U-A1")
        .expect_err("unknown BDD tags must fail closed")
        .to_string();
    assert!(failure.contains("BDD tag @F101_S999 is not declared"));
}

#[test]
fn given_duplicate_feature_tag_when_checker_runs_then_mapping_is_rejected() {
    let fixture = InventoryFixture::new("bdd-duplicate");
    fixture.write_model("manifest", "inv", "witness");
    fixture.write_feature(
        "@F101\nFeature: analytics\n\n@F101_S001\nScenario: first\n\n@F101_S001\nScenario: \
         second\n",
    );
    fixture.write_inventory(&[InventoryFixture::with_bdd_tags(
        fixture.row(
            "manifest",
            "M-A1-MAN-001",
            "not-applicable; proof=type=fixture",
            "HT-BDD-DUPLICATE",
            "fixed seed 0x1; 1 samples; 1 steps; focused",
            "not-applicable",
        ),
        "@F101_S001",
    )]);

    let failure = check_repository(&fixture.root, "U-A1")
        .expect_err("duplicate BDD declarations must fail closed")
        .to_string();
    assert!(failure.contains("BDD tag @F101_S001 is declared 2 times"));
}

#[test]
fn given_feature_level_bdd_tag_when_checker_runs_then_mapping_is_rejected() {
    let fixture = InventoryFixture::new("bdd-feature-level");
    fixture.write_model("manifest", "inv", "witness");
    fixture.write_feature("@F101\nFeature: analytics\n");
    fixture.write_inventory(&[InventoryFixture::with_bdd_tags(
        fixture.row(
            "manifest",
            "M-A1-MAN-001",
            "not-applicable; proof=type=fixture",
            "HT-BDD-FEATURE-LEVEL",
            "fixed seed 0x1; 1 samples; 1 steps; focused",
            "not-applicable",
        ),
        "@F101",
    )]);

    let failure = check_repository(&fixture.root, "U-A1")
        .expect_err("feature-level BDD tags must fail closed")
        .to_string();
    assert!(failure.contains("BDD tag @F101 is not attached to a Scenario"));
}

#[test]
fn given_malformed_bdd_tag_list_when_checker_runs_then_mapping_is_rejected() {
    for (label, tags, expected) in [
        ("missing-prefix", "F101_S001", "malformed BDD tag"),
        ("empty-element", "@F101_S001,", "empty comma element"),
    ] {
        let fixture = InventoryFixture::new(&format!("bdd-{label}"));
        fixture.write_model("manifest", "inv", "witness");
        fixture.write_feature("@F101\nFeature: analytics\n\n@F101_S001\nScenario: valid\n");
        fixture.write_inventory(&[InventoryFixture::with_bdd_tags(
            fixture.row(
                "manifest",
                "M-A1-MAN-001",
                "not-applicable; proof=type=fixture",
                "HT-BDD-MALFORMED",
                "fixed seed 0x1; 1 samples; 1 steps; focused",
                "not-applicable",
            ),
            tags,
        )]);

        let failure = check_repository(&fixture.root, "U-A1")
            .expect_err("malformed BDD tag lists must fail closed")
            .to_string();
        assert!(
            failure.contains(expected),
            "unexpected diagnostic: {failure}"
        );
    }
}

#[test]
fn given_known_scenario_tags_when_checker_runs_then_mapping_is_accepted() {
    let fixture = InventoryFixture::new("bdd-positive");
    fixture.write_model("manifest", "inv", "witness");
    fixture.write_feature(
        "@F101\nFeature: analytics\n\n@F101_S001\nScenario: first\n\n@F101_S002\nScenario: \
         second\n",
    );
    fixture.write_inventory(&[InventoryFixture::with_bdd_tags(
        fixture.row(
            "manifest",
            "PENDING-F-A2",
            "pending F-A2 BDD mapping; expiry before I-A2",
            "HT-BDD-POSITIVE",
            "pending F-A2 fixed-seed receipt",
            "model-only",
        ),
        "@F101_S001, @F101_S002",
    )]);

    check_repository(&fixture.root, "U-A1").expect("known scenario tags should map");
}

#[test]
fn given_declared_scenario_without_inventory_mapping_when_checker_runs_then_it_is_rejected() {
    let fixture = InventoryFixture::new("bdd-coverage");
    fixture.write_model("manifest", "inv", "witness");
    fixture.write_feature(
        "@F101\nFeature: analytics\n\n@F101_S001\nScenario: first\n\n@F101_S002\nScenario: \
         second\n",
    );
    fixture.write_inventory(&[InventoryFixture::with_bdd_tags(
        fixture.row(
            "manifest",
            "PENDING-F-A2",
            "pending F-A2 BDD mapping; expiry before I-A2",
            "HT-BDD-COVERAGE",
            "pending F-A2 fixed-seed receipt",
            "model-only",
        ),
        "@F101_S001",
    )]);

    let failure = check_repository(&fixture.root, "U-A1")
        .expect_err("unmapped structured scenario tags must fail closed")
        .to_string();
    assert!(failure.contains("BDD tag @F101_S002 has no inventory mapping"));
}

#[test]
fn given_malformed_invariant_name_when_checker_runs_then_it_is_rejected() {
    let fixture = InventoryFixture::new("invariant-name");
    fixture.write_model("manifest", "inv", "witness");
    for invalid in [
        "inv value",
        "inv || true",
        "--invariant",
        "1inv",
        "inv\rname",
    ] {
        let mut fields = fixture
            .row(
                "manifest",
                "PENDING-F-A2",
                "pending F-A2 invariant validation; expiry before I-A2",
                "HT-INVARIANT-NAME",
                "fixed seed 0x1; 1 samples; 1 steps; focused",
                "model-only",
            )
            .split('\t')
            .map(str::to_owned)
            .collect::<Vec<_>>();
        fields[4] = invalid.to_owned();
        fixture.write_inventory(&[fields.join("\t")]);

        let failure = check_repository(&fixture.root, "U-A1")
            .expect_err("malformed invariant names must fail closed")
            .to_string();
        assert!(
            failure.contains("strict Quint identifier"),
            "unexpected diagnostic for {invalid:?}: {failure}"
        );
    }
}

#[test]
fn given_exact_invariant_declaration_when_checker_runs_then_only_val_forms_are_accepted() {
    let fixture = InventoryFixture::new("invariant-declaration");
    fixture.write_model("manifest", "inv", "witness");
    let row = fixture.row(
        "manifest",
        "PENDING-F-A2",
        "pending F-A2 invariant validation; expiry before I-A2",
        "HT-INVARIANT-DECLARATION",
        "fixed seed 0x1; 1 samples; 1 steps; focused",
        "model-only",
    );

    fixture.write_inventory(std::slice::from_ref(&row));
    check_repository(&fixture.root, "U-A1").expect("ordinary val declaration should pass");

    std::fs::write(
        fixture.root.join("specs/manifest.qnt"),
        "module manifest\nvar state = 0\naction init = state' = 0\npure val inv = true\nval \
         witness = true\n",
    )
    .expect("pure val model");
    fixture.write_inventory(std::slice::from_ref(&row));
    check_repository(&fixture.root, "U-A1").expect("pure val declaration should pass");

    std::fs::write(
        fixture.root.join("specs/manifest.qnt"),
        "module manifest\nvar state = 0\naction init = state' = 0\nval invAlias = true\nval \
         witness = true\n",
    )
    .expect("mismatched val model");
    fixture.write_inventory(std::slice::from_ref(&row));
    let failure = check_repository(&fixture.root, "U-A1")
        .expect_err("only the exact invariant declaration should pass")
        .to_string();
    assert!(failure.contains("invariant inv is not declared"));
}

#[test]
fn given_covered_and_deferred_rows_when_checked_then_evidence_is_accepted() {
    assert!(
        parse_evidence(
            "covered",
            "adapter=crates/test-owner/src/test_adapter.rs; actions=init; state=state; \
             proof=type=fixture",
            "M-A1-MAN-001"
        )
        .is_ok()
    );
    assert!(
        parse_evidence(
            "model-only",
            "pending F-A2 audit remains; expiry before I-A2",
            "PENDING-F-A2"
        )
        .is_ok()
    );
}

#[test]
fn given_covered_row_without_witness_when_checker_runs_then_it_is_rejected() {
    let fixture = InventoryFixture::new("covered-witnesses");
    fixture.write_model("manifest", "inv", "witness");
    fixture.write_adapter();
    let row = fixture.row(
        "manifest",
        "M-A1-MAN-001",
        "adapter=crates/test-owner/src/test_adapter.rs; actions=init; state=state; \
         proof=type=fixture",
        "HT-COVERED-WITNESSES",
        "fixed seed 0x1; 1 samples; 1 steps; focused",
        "covered",
    );
    let mut fields = row.split('\t').map(str::to_owned).collect::<Vec<_>>();
    fields[5] = "-".to_owned();
    fixture.write_inventory(&[fields.join("\t")]);

    let failure = check_repository(&fixture.root, "U-A1")
        .expect_err("covered rows need at least one witness")
        .to_string();
    assert!(failure.contains("covered status must declare at least one model witness"));
}

#[test]
fn given_current_and_named_exception_rows_when_checked_then_evidence_is_accepted() {
    assert!(
        parse_evidence(
            "model-only",
            "pending U-A1 current handoff; adapter=crates/test-owner/src/test_adapter.rs; \
             actions=init; state=state; proof=type=fixture; expiry before I-A2",
            "M-A1-MAN-001"
        )
        .is_ok()
    );
    assert!(
        parse_evidence(
            "not-applicable",
            "not-applicable; proof=type=fixture",
            "M-A1-MAN-002"
        )
        .is_ok()
    );
    let failure = parse_evidence(
        "not-applicable",
        "not-applicable; proof=type",
        "M-A1-MAN-002",
    )
    .expect_err("an unnamed proof must fail");
    assert!(failure.iter().any(|error| error.contains("not named")));
}

#[test]
fn given_invalid_model_only_expiry_when_checked_then_evidence_is_rejected() {
    let fixture = InventoryFixture::new("expiry");
    fixture.write_model("manifest", "inv", "witness");
    fixture.write_inventory(&[fixture.row(
        "manifest",
        "PENDING-F-A2",
        "pending F-A2 audit; expiry before 2026-02-30",
        "HT expiry before 2026-02-30",
        "pending F-A2 fixed-seed receipt",
        "model-only",
    )]);
    let malformed = check_repository(&fixture.root, "U-A1")
        .expect_err("invalid calendar date must fail")
        .to_string();
    assert!(malformed.contains("malformed expiry condition"));

    fixture.replace_expiry("expiry before U-A1");
    let expired = check_repository(&fixture.root, "U-A1")
        .expect_err("current phase expiry must fail")
        .to_string();
    assert!(expired.contains("current or past"));
}

#[test]
fn given_reserved_markers_or_empty_lists_when_checked_then_evidence_is_rejected() {
    let fixture = InventoryFixture::new("negative");
    fixture.write_model("manifest", "inv", "witness");
    fixture.write_adapter();
    fixture.write_inventory(&[fixture.row(
        "manifest",
        "M-A1-MAN-001",
        "adapter=crates/test-owner/src/test_adapter.rs; actions=init,; state=state; \
         proof=type=fixture",
        "HT-NEGATIVE",
        "fixed seed 0x1; 1 samples; 1 steps; pending marker",
        "covered",
    )]);
    let failure = check_repository(&fixture.root, "U-A1")
        .expect_err("covered reserved marker and empty list must fail")
        .to_string();
    assert!(failure.contains("empty comma element"));
}

#[test]
fn given_unknown_phase_when_checked_then_execution_is_rejected() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let failure = check_repository(&repository, "UNKNOWN").expect_err("phase must be ordered");
    assert!(
        failure
            .to_string()
            .contains("unknown QUINT_CURRENT_PHASE UNKNOWN")
    );
}

#[test]
fn given_empty_identifier_or_missing_owner_when_checked_then_rule_is_rejected() {
    let fixture = InventoryFixture::new("identity");
    fixture.write_model("manifest", "inv", "witness");
    let row = fixture.row(
        "manifest",
        "M-A1-MAN-001",
        "not-applicable; proof=type=fixture",
        "HT-IDENTITY",
        "fixed seed 0x1; 1 samples; 1 steps; focused",
        "not-applicable",
    );
    let row = row
        .split_once('\t')
        .expect("identifier column")
        .1
        .to_owned();
    fixture.write_inventory(&[format!("\t{row}")]);
    let failure = check_repository(&fixture.root, "U-A1")
        .expect_err("empty rule identifiers must not be skipped")
        .to_string();
    assert!(failure.contains("empty rule_id"));

    let row = fixture
        .row(
            "manifest",
            "M-A1-MAN-002",
            "not-applicable; proof=type=fixture",
            "HT-IDENTITY",
            "fixed seed 0x1; 1 samples; 1 steps; focused",
            "not-applicable",
        )
        .replace("\ttest-owner\t", "\tmissing-owner\t");
    fixture.write_inventory(&[row]);
    let failure = check_repository(&fixture.root, "U-A1")
        .expect_err("owners must resolve to an in-repository crate")
        .to_string();
    assert!(failure.contains("missing in-repository rust_owner crate"));
}

#[test]
fn given_empty_covered_evidence_when_checked_then_rule_is_rejected() {
    let fixture = InventoryFixture::new("covered-evidence");
    fixture.write_model("manifest", "inv", "witness");
    fixture.write_adapter();
    fixture.write_inventory(&[fixture.row(
        "manifest",
        "M-A1-MAN-001",
        "adapter=; actions=; state=; proof=type=fixture",
        "HT-COVERED",
        "fixed seed 0x1; 1 samples; 1 steps; focused",
        "covered",
    )]);
    let failure = check_repository(&fixture.root, "U-A1")
        .expect_err("covered evidence must name real path/actions/state")
        .to_string();
    assert!(failure.contains("adapter marker has no path"));
    assert!(failure.contains("actions has an empty comma element"));
    assert!(failure.contains("state has an empty comma element"));

    fixture.write_inventory(&[fixture.row(
        "manifest",
        "M-A1-MAN-001",
        "adapter=crates/test-owner/src/test_adapter.rs,; actions=init; state=state; \
         proof=type=fixture",
        "HT-COVERED",
        "fixed seed 0x1; 1 samples; 1 steps; focused",
        "covered",
    )]);
    let failure = check_repository(&fixture.root, "U-A1")
        .expect_err("adapter comma elements must not be silently discarded")
        .to_string();
    assert!(failure.contains("adapter marker has an empty comma element"));

    fixture.write_inventory(&[fixture.row(
        "manifest",
        "PENDING-F-A2",
        "NONE; actions=init; state=state; expiry before 2026-02-30",
        "HT-UNAVAILABLE",
        "pending F-A2 fixed-seed receipt",
        "model-only",
    )]);
    let failure = check_repository(&fixture.root, "U-A1")
        .expect_err("an expired unavailable row must fail")
        .to_string();
    assert!(
        failure.contains("malformed expiry condition") || failure.contains("today or in the past")
    );

    fixture.write_inventory(&[fixture.row(
        "manifest",
        "PENDING-F-A2",
        "pending F-A2 adapter audit; adapter=; actions=init; state=state; expiry before I-A2",
        "HT-DEFERRED expiry before I-A2",
        "pending F-A2 fixed-seed receipt",
        "model-only",
    )]);
    let failure = check_repository(&fixture.root, "U-A1")
        .expect_err("a deferred empty adapter path must fail")
        .to_string();
    assert!(failure.contains("adapter marker has no path"));
}

#[test]
fn given_adapter_path_traversal_when_checked_then_path_is_rejected_before_lookup() {
    let fixture = InventoryFixture::new("adapter-traversal");
    fixture.write_model("manifest", "inv", "witness");
    fixture.write_inventory(&[fixture.row(
        "manifest",
        "M-A1-MAN-001",
        "adapter=crates/analytics-contract/../../../external; actions=init; state=state; \
         proof=type=fixture",
        "HT-TRAVERSAL",
        "fixed seed 0x1; 1 samples; 1 steps; focused",
        "covered",
    )]);
    let failure = check_repository(&fixture.root, "U-A1")
        .expect_err("traversal must be rejected")
        .to_string();
    assert!(failure.contains("non-normal adapter path"));
}

#[cfg(unix)]
#[test]
fn given_adapter_symlink_escape_when_checked_then_path_is_rejected() {
    let fixture = InventoryFixture::new("adapter-symlink");
    fixture.write_model("manifest", "inv", "witness");
    fixture.write_symlink_escape();
    fixture.write_inventory(&[fixture.row(
        "manifest",
        "M-A1-MAN-001",
        "adapter=crates/test-owner/src/escape.rs; actions=init; state=state; proof=type=fixture",
        "HT-SYMLINK",
        "fixed seed 0x1; 1 samples; 1 steps; focused",
        "covered",
    )]);
    let failure = check_repository(&fixture.root, "U-A1")
        .expect_err("symlink escape must be rejected")
        .to_string();
    assert!(failure.contains("adapter path escapes declared crate"));
}

#[test]
fn given_utc_expiry_boundaries_when_parsed_then_leap_dates_are_checked() {
    assert!(parse_iso_date("2024-02-29").is_some());
    assert!(parse_iso_date("2023-02-29").is_none());
    assert!(parse_iso_date("2024-02-30").is_none());

    let yesterday = utc_date_with_offset(-1);
    let today = utc_date_with_offset(0);
    let tomorrow = utc_date_with_offset(1);
    for (label, date, expected_success) in [
        ("yesterday", yesterday, false),
        ("today", today, false),
        ("tomorrow", tomorrow, true),
    ] {
        let fixture = InventoryFixture::new(label);
        fixture.write_model("manifest", "inv", "witness");
        fixture.write_inventory(&[fixture.row(
            "manifest",
            "PENDING-F-A2",
            &format!("pending F-A2 audit; expiry before {date}"),
            &format!("HT-EXPIRY expiry before {date}"),
            "pending F-A2 fixed-seed receipt",
            "model-only",
        )]);
        let result = check_repository(&fixture.root, "U-A1");
        assert_eq!(result.is_ok(), expected_success, "expiry boundary {label}");
        if !expected_success {
            assert!(
                result
                    .expect_err("boundary must be rejected")
                    .to_string()
                    .contains("today or in the past")
            );
        }
    }
}

#[test]
fn given_comments_and_literals_when_symbols_are_checked_then_fake_code_is_ignored() {
    let rust = r####"
        // fn fake_comment() {}
        /* fake_block_state and fake_block_action */
        let normal = "fake_string_state";
        let raw = r##"fake_raw_state"##;
        let bytes = b"fake_bytes_state";
        let character = 'x';
        fn real_action() {}
        let real_state = 1;
    "####;
    assert!(identifier_present(rust, "real_action"));
    assert!(identifier_present(rust, "real_state"));
    for fake in [
        "fake_comment",
        "fake_block_state",
        "fake_block_action",
        "fake_string_state",
        "fake_raw_state",
        "fake_bytes_state",
    ] {
        assert!(
            !identifier_present(rust, fake),
            "literal/comment symbol {fake}"
        );
    }

    let model = r#"
        // val fake_model = true
        /* val fake_block_model = true */
        val real_model = true
    "#;
    assert!(model_symbol_present(model, "val", "real_model"));
    assert!(!model_symbol_present(model, "val", "fake_model"));
    assert!(!model_symbol_present(model, "val", "fake_block_model"));

    let character_literals = r#"b'f' '\u{66}' '\x66'"#;
    for fake in ["f", "u", "x"] {
        assert!(
            !identifier_present(character_literals, fake),
            "character literal symbol {fake}"
        );
    }
}
