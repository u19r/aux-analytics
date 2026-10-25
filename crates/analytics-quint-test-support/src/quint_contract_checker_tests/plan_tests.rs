use std::{fs, path::Path};

use super::inventory_fixture::InventoryFixture;
use crate::quint_contract_checker::{
    check_repository, mutation_models, reset_validation_runs, run_plan, validation_runs,
};

#[test]
fn given_shared_models_and_adapters_when_plan_is_built_then_runs_are_unique() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let plan = run_plan(
        &repository,
        "U-A1",
        crate::quint_contract_checker::RunPlanOptions {
            family: "all",
            mode: "standard",
            skip_adapters: false,
            skip_mutations: true,
        },
    )
    .expect("repository plan should validate");
    assert_eq!(
        plan.records
            .iter()
            .filter(|record| record.starts_with("typecheck\t"))
            .count(),
        28
    );
    assert_eq!(
        plan.records
            .iter()
            .filter(|record| record.starts_with("test\t"))
            .count(),
        28
    );
    let adapters = plan
        .records
        .iter()
        .filter_map(|record| record.strip_prefix("adapter\t"))
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(adapters.len(), 8);
    assert_eq!(
        adapters.len(),
        plan.records
            .iter()
            .filter(|record| record.starts_with("adapter\t"))
            .count()
    );
    assert!(
        plan.records
            .iter()
            .filter(|record| record.starts_with("run\t"))
            .all(|record| record.contains("\t--max-steps=") && !record.contains("t--max-steps="))
    );
    assert!(
        plan.records
            .iter()
            .all(|record| !record.starts_with("mutations\t")),
        "mutation execution must not be transported as a second command"
    );
    assert!(plan.mutation_receipts.is_empty());
    assert!(
        run_plan(
            &repository,
            "U-A1",
            crate::quint_contract_checker::RunPlanOptions {
                family: "unknown-family",
                mode: "standard",
                skip_adapters: true,
                skip_mutations: true
            }
        )
        .is_err()
    );
}

#[test]
fn given_valid_repository_when_combined_plan_runs_then_checker_validates_once() {
    let fixture = InventoryFixture::new("combined-plan");
    let mut rows = Vec::new();
    for (mutation, model) in mutation_models() {
        let model_path = Path::new(model);
        let module = model_path
            .file_stem()
            .expect("mutation model file name")
            .to_string_lossy();
        fs::write(
            fixture.root.join(model),
            format!("module {module}\nval inv = true\nval witness = true\n"),
        )
        .expect("mutation model");
        rows.push(format!(
            concat!(
                "R-{}\ttest\tfixture rule\t{}\tinv\twitness\tHT-COMBINED\t",
                "{}\t-\ttest-owner\tnot-applicable; proof=type=fixture\t-\t",
                "fixed seed 0x1; 1 samples; 1 steps; focused\tnot-applicable"
            ),
            mutation, model, mutation
        ));
    }
    fixture.write_inventory(&rows);
    reset_validation_runs();

    let plan = run_plan(
        &fixture.root,
        "U-A1",
        crate::quint_contract_checker::RunPlanOptions {
            family: "all",
            mode: "standard",
            skip_adapters: true,
            skip_mutations: true,
        },
    )
    .expect("combined plan should validate");

    assert!(!plan.records.is_empty());
    assert_eq!(validation_runs(), 1);
}

#[test]
fn given_current_inventory_when_all_mode_plan_is_built_then_each_mode_stays_bounded() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let plan = run_plan(
        &repository,
        "U-A1",
        crate::quint_contract_checker::RunPlanOptions {
            family: "all",
            mode: "all",
            skip_adapters: true,
            skip_mutations: true,
        },
    )
    .expect("all-mode plan should fit its bounded mode budgets");
    let standard = run_plan(
        &repository,
        "U-A1",
        crate::quint_contract_checker::RunPlanOptions {
            family: "all",
            mode: "standard",
            skip_adapters: true,
            skip_mutations: true,
        },
    )
    .expect("standard-mode plan should fit its bounded budget");
    let deep = run_plan(
        &repository,
        "U-A1",
        crate::quint_contract_checker::RunPlanOptions {
            family: "all",
            mode: "deep",
            skip_adapters: true,
            skip_mutations: true,
        },
    )
    .expect("deep-mode plan should fit its bounded budget");
    let run_count = |plan: &crate::quint_contract_checker::RunPlan| {
        plan.records
            .iter()
            .filter(|record| record.starts_with("run\t"))
            .count()
    };
    assert_eq!(
        run_count(&plan),
        run_count(&standard) + run_count(&deep),
        "all mode should compose independently bounded mode plans"
    );
    assert_eq!(
        run_count(&standard),
        run_count(&deep),
        "both modes should retain the same grouped command structure"
    );

    let runs = plan
        .records
        .iter()
        .filter(|record| record.starts_with("run\t"))
        .collect::<Vec<_>>();

    let ingestion = runs
        .iter()
        .find(|record| record.contains("specs/ingestion.qnt\t--invariant=inv"))
        .expect("ingestion invariant command");
    assert!(ingestion.contains("--max-samples=2000"));
    let ingestion_witnesses = runs
        .iter()
        .find(|record| {
            record.contains("specs/ingestion.qnt\t--witnesses=")
                && record.contains("\t--witnesses=user_row_exists")
        })
        .expect("grouped ingestion witness command");
    for witness in [
        "user_row_exists",
        "user_row_can_be_updated",
        "audit_row_exists",
    ] {
        assert!(
            ingestion_witnesses.contains(&format!("\t--witnesses={witness}")),
            "grouped command should retain {witness}"
        );
    }
}

#[test]
fn given_mismatched_mutation_model_when_checked_then_binding_is_rejected() {
    let fixture = InventoryFixture::new("mutation-mapping");
    fixture.write_model("manifest", "inv", "witness");
    fixture.write_model("query", "inv", "witness");
    let row = fixture.row(
        "manifest",
        "M-A1-MAN-001",
        "not-applicable; proof=type=fixture",
        "HT-MAPPING",
        "fixed seed 0x1; 1 samples; 1 steps; focused",
        "not-applicable",
    );
    fixture.write_inventory(&[row.replace("specs/manifest.qnt", "specs/query.qnt")]);
    let failure = check_repository(&fixture.root, "U-A1")
        .expect_err("a mutation must map to its exact model")
        .to_string();
    assert!(failure.contains("mutation M-A1-MAN-001 maps to specs/manifest.qnt"));
    assert!(failure.contains("inventory says specs/query.qnt"));
}

#[cfg(unix)]
#[test]
fn given_model_symlink_escape_when_checked_then_source_is_rejected() {
    use std::os::unix::fs::symlink;

    let fixture = InventoryFixture::new("model-symlink");
    let external = fixture.root.join("external.qnt");
    fs::write(
        &external,
        "module external\nvar state = 0\naction init = state' = 0\nval inv = true\nval witness = \
         true\n",
    )
    .expect("external model");
    symlink(&external, fixture.root.join("specs/escape.qnt")).expect("model symlink");
    let row = fixture
        .row(
            "manifest",
            "M-A1-MAN-001",
            "not-applicable; proof=type=fixture",
            "HT-SYMLINK-MODEL",
            "fixed seed 0x1; 1 samples; 1 steps; focused",
            "not-applicable",
        )
        .replace("specs/manifest.qnt", "specs/escape.qnt");
    fixture.write_inventory(&[row]);

    let failure = check_repository(&fixture.root, "U-A1")
        .expect_err("readable model symlinks must remain confined to specs")
        .to_string();
    assert!(
        failure.contains("escapes specs") || failure.contains("regular file"),
        "unexpected symlink diagnostic: {failure}"
    );
}

#[test]
fn given_unconfined_or_oversized_model_when_checked_then_source_is_rejected() {
    let traversal = InventoryFixture::new("model-traversal");
    fs::write(
        traversal.root.join("external.qnt"),
        "module external\nvar state = 0\naction init = state' = 0\nval inv = true\nval witness = \
         true\n",
    )
    .expect("external model");
    let row = traversal
        .row(
            "manifest",
            "M-A1-MAN-001",
            "not-applicable; proof=type=fixture",
            "HT-TRAVERSAL-MODEL",
            "fixed seed 0x1; 1 samples; 1 steps; focused",
            "not-applicable",
        )
        .replace("specs/manifest.qnt", "../external.qnt");
    traversal.write_inventory(&[row]);
    let failure = check_repository(&traversal.root, "U-A1")
        .expect_err("readable models must not traverse outside specs")
        .to_string();
    assert!(
        failure.contains("under specs")
            || failure.contains("escapes specs")
            || failure.contains("non-normal readable model path"),
        "unexpected traversal diagnostic: {failure}"
    );

    let oversized = InventoryFixture::new("model-size");
    oversized.write_model("manifest", "inv", "witness");
    fs::write(
        oversized.root.join("specs/manifest.qnt"),
        format!(
            "module manifest\nvar state = 0\naction init = state' = 0\nval inv = true\nval \
             witness = true\n{}\n",
            "x".repeat(2 * 1024 * 1024)
        ),
    )
    .expect("oversized model");
    oversized.write_inventory(&[oversized.row(
        "manifest",
        "M-A1-MAN-001",
        "not-applicable; proof=type=fixture",
        "HT-OVERSIZED-MODEL",
        "fixed seed 0x1; 1 samples; 1 steps; focused",
        "not-applicable",
    )]);
    let failure = check_repository(&oversized.root, "U-A1")
        .expect_err("readable model source must have a finite byte bound")
        .to_string();
    assert!(
        failure.contains("byte limit"),
        "unexpected oversized model diagnostic: {failure}"
    );
}

#[test]
fn given_non_dash_mbt_reference_outside_specs_when_checker_runs_then_it_is_rejected() {
    let fixture = InventoryFixture::new("mbt-path");
    fixture.write_model("manifest", "inv", "witness");
    let external = fixture.root.join("external.qnt");
    std::fs::write(
        &external,
        "module external\nval inv = true\nval witness = true\n",
    )
    .expect("external MBT model");
    let row = fixture.row(
        "manifest",
        "PENDING-F-A2",
        "pending F-A2 MBT path validation; expiry before I-A2",
        "HT-MBT-PATH",
        "fixed seed 0x1; 1 samples; 1 steps; focused",
        "model-only",
    );

    for reference in [
        external.to_string_lossy().into_owned(),
        "../external.qnt".to_owned(),
        "specs/manifest.txt".to_owned(),
        "crates/test-owner/src/adapter.rs".to_owned(),
    ] {
        let mut fields = row.split('\t').map(str::to_owned).collect::<Vec<_>>();
        fields[8] = reference.clone();
        fixture.write_inventory(&[fields.join("\t")]);
        let failure = check_repository(&fixture.root, "U-A1")
            .expect_err("non-dash MBT references must remain in specs/*.qnt")
            .to_string();
        assert!(
            failure.contains("MBT model") || failure.contains("under specs"),
            "unexpected diagnostic for {reference}: {failure}"
        );
    }
}

#[cfg(unix)]
#[test]
fn given_mbt_symlink_escape_when_checker_runs_then_it_is_rejected() {
    use std::os::unix::fs::symlink;

    let fixture = InventoryFixture::new("mbt-symlink");
    fixture.write_model("manifest", "inv", "witness");
    let external = fixture.root.join("external.qnt");
    std::fs::write(
        &external,
        "module external\nval inv = true\nval witness = true\n",
    )
    .expect("external MBT model");
    symlink(&external, fixture.root.join("specs/escape_mbt.qnt")).expect("MBT symlink");
    let mut fields = fixture
        .row(
            "manifest",
            "PENDING-F-A2",
            "pending F-A2 MBT path validation; expiry before I-A2",
            "HT-MBT-SYMLINK",
            "fixed seed 0x1; 1 samples; 1 steps; focused",
            "model-only",
        )
        .split('\t')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    fields[8] = "specs/escape_mbt.qnt".to_owned();
    fixture.write_inventory(&[fields.join("\t")]);

    let failure = check_repository(&fixture.root, "U-A1")
        .expect_err("MBT symlink escapes must fail closed")
        .to_string();
    assert!(
        failure.contains("escapes specs") || failure.contains("regular file"),
        "unexpected symlink diagnostic: {failure}"
    );
}
