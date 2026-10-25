use super::inventory_fixture::InventoryFixture;
use crate::{
    constants::{INVENTORY_DEFERRED_MARKERS, INVENTORY_STATUS_VARIANTS},
    quint_contract_checker::{check_repository, utc_date_with_offset},
};

#[test]
fn given_invalid_inventory_matrix_when_checked_then_every_distinct_case_is_rejected() {
    let fixture = InventoryFixture::new("inventory-matrix");
    fixture.write_model("model", "inv", "witness");
    fixture.write_adapter();
    let covered = fixture.row(
        "model",
        "M-BASE",
        "crates/test-owner/src/test_adapter.rs; actions=init; state=state",
        "HT-BASE",
        "fixed seed 0x1; 1 samples; 1 steps; focused",
        "covered",
    );
    let not_applicable = fixture.row(
        "model",
        "M-BASE",
        "not-applicable; proof=type=fixture",
        "HT-BASE",
        "fixed seed 0x1; 1 samples; 1 steps; focused",
        "not-applicable",
    );
    let mut rejected = 0;
    for field in 0..14 {
        for marker in INVENTORY_DEFERRED_MARKERS {
            assert_matrix_rejected(&fixture, mutate_field(&covered, field, marker));
            rejected += 1;
        }
    }
    for field in 0..10 {
        for marker in INVENTORY_DEFERRED_MARKERS {
            assert_matrix_rejected(&fixture, mutate_field(&not_applicable, field, marker));
            rejected += 1;
        }
    }

    for field in 10..14 {
        for marker in INVENTORY_DEFERRED_MARKERS {
            assert_matrix_rejected(&fixture, mutate_field(&not_applicable, field, marker));
            rejected += 1;
        }
    }

    let adapter = "crates/test-owner/src/test_adapter.rs";
    let current_variants = vec![
        "pending U-A1".to_owned(),
        format!("pending U-A1; adapter={adapter}"),
        format!("pending U-A1; adapter={adapter}; actions=init,step; proof=adapter"),
        format!(
            "pending U-A1; adapter={adapter}; actions=init,step; state=state; actions=step; \
             proof=adapter"
        ),
        format!(
            "pending U-A1; adapter={adapter}; actions=init,step; state=state; state=last; \
             proof=adapter"
        ),
        format!(
            "pending U-A1; adapter={adapter}; adapter={adapter}; actions=init; state=state; \
             proof=adapter"
        ),
        "pending U-A1; adapter=; actions=init; state=state; proof=adapter".to_owned(),
        format!("pending U-A1; adapter={adapter}; actions=; state=state; proof=adapter"),
        format!("pending U-A1; adapter={adapter}; actions=init,,step; state=state; proof=adapter"),
        format!("pending U-A1; adapter={adapter}; actions=init; state=; proof=adapter"),
        format!("pending U-A1; adapter={adapter}; actions=init; state=state,,last; proof=adapter"),
        format!(
            "pending U-A1; adapter={adapter}; actions=init,NoSuchAction; state=state; \
             proof=adapter"
        ),
        format!("pending U-A1; adapter={adapter}; actions=init; state=noSuchState; proof=adapter"),
        "pending U-A1; adapter=crates/test-owner/src/missing.rs; actions=init; state=state; \
         proof=adapter"
            .to_owned(),
        format!(
            "pending U-A1; adapter={adapter}; bogus=value; actions=init; state=state; \
             proof=adapter"
        ),
        format!(
            "pending U-A1; =value; adapter={adapter}; actions=init; state=state; proof=adapter"
        ),
        format!("pending U-A10; adapter={adapter}; actions=init; state=state; proof=adapter"),
        format!("pending U-A9; adapter={adapter}; actions=init; state=state; proof=adapter"),
        format!(
            "pending U-A1; adapter={adapter}; actions=init; state=state; proof=adapter; expiry \
             before U-A1"
        ),
        format!(
            "pending U-A1; adapter={adapter}; actions=init; state=state; proof=adapter; expiry \
             before F-A1"
        ),
        format!(
            "pending U-A1; adapter={adapter}; actions=init; state=state; proof=adapter; expiry \
             before 2020-01-01"
        ),
        format!(
            "pending U-A1; adapter={adapter}; actions=init; state=state; proof=adapter; expiry \
             before 2026-02-30"
        ),
        format!(
            "pending U-A1; adapter={adapter}; actions=init; state=state; proof=adapter; expiry \
             before 2026-8-26"
        ),
        format!(
            "pending U-A1; adapter={adapter}; actions=init; state=state; proof=adapter; expiry \
             before R-A1; expiry before P-A1"
        ),
        format!(
            "pending U-A1; adapter={adapter}; actions=init; state=state; pending F-A2; \
             proof=adapter; expiry before R-A1"
        ),
        format!(
            "pending U-A1; adapter={adapter}; actions=init; state=state; proof=pending F-A2; \
             expiry before R-A1"
        ),
        format!(
            "pending U-A1; adapter={adapter}; actions=init; state=state; proof=foo=bar; expiry \
             before R-A1"
        ),
        format!(
            "pending U-A1; adapter={adapter}; actions=init; state=state; proof=type=; expiry \
             before R-A1"
        ),
        format!(
            "pending U-A1; adapter={adapter}; actions=init; state=state; proof=adapter; foo; \
             expiry before R-A1"
        ),
        format!(
            "pending U-A1; adapter={adapter}; actions=init; state=state; proof=adapter; expiry \
             today"
        ),
        format!(
            "pending U-A1; adapter={adapter}; actions=init; state=state; proof=adapter; expiry \
             before R-A1; crates/test-owner/src/missing.rs"
        ),
    ];
    let current_expiry = utc_date_with_offset(1);
    for value in current_variants {
        let row = fixture.row(
            "model",
            "M-BASE",
            &format!(
                "pending U-A1 current audit; adapter={adapter}; actions=init; state=state; \
                 proof=type=fixture; expiry before {current_expiry}"
            ),
            "HT-CURRENT",
            "fixed seed 0x1; 1 samples; 1 steps; focused",
            "model-only",
        );
        assert_matrix_rejected(&fixture, mutate_field(&row, 10, &value));
        rejected += 1;
    }

    for value in INVENTORY_STATUS_VARIANTS {
        assert_matrix_rejected(&fixture, mutate_field(&covered, 13, value));
        rejected += 1;
    }
    for value in [
        format!(
            "{adapter}; actions=init; state=state; expiry before {}",
            utc_date_with_offset(1)
        ),
        format!("pending U-A1; adapter={adapter}; actions=init; state=state"),
        format!("{adapter}; actions=init; state=state; placeholder F-A2"),
        format!("{adapter}; actions=init; state=state; deferred F-A3"),
    ] {
        assert_matrix_rejected(&fixture, mutate_field(&covered, 10, &value));
        rejected += 1;
    }

    let deferred = fixture.row(
        "model",
        "PENDING-F-A2",
        "pending F-A2 audit; expiry before I-A2",
        "HT-DEFERRED expiry before I-A2",
        "pending F-A2 fixed-seed receipt",
        "model-only",
    );
    for value in [
        "covered".to_owned(),
        "not-applicable".to_owned(),
        "unknown".to_owned(),
        "pending F-A2 audit; expiry before I-A2; expiry before I-A3".to_owned(),
        "pending F-A2 audit; expiry before I-A1".to_owned(),
        "pending F-A2 audit; expiry before 2020-02-30".to_owned(),
        format!(
            "pending F-A2 audit; expiry before {}",
            utc_date_with_offset(0)
        ),
        "pending F-A2 audit; expiry ??? I-A2".to_owned(),
    ] {
        let row = if value == "covered" || value == "not-applicable" || value == "unknown" {
            mutate_field(&deferred, 13, &value)
        } else {
            mutate_field(&deferred, 10, &value)
        };
        assert_matrix_rejected(&fixture, row);
        rejected += 1;
    }

    for value in [
        "not-applicable",
        "not-applicable; proof=",
        "not-applicable; proof=foo",
        "not-applicable; proof=type",
        "not-applicable; proof=type=",
        "not-applicable; proof=bad=proof",
        "not-applicable; proof=type=fixture; proof=parser=fixture",
        "pending F-A2; proof=type=fixture",
        "not-applicable; proof=type=fixture; bogus=x",
        "not-applicable; proof=parser;",
        "not-applicable; proof=unit",
        "not-applicable; proof=adapter",
        "not-applicable; proof=parser=",
        "not-applicable; proof=type=fixture; pending F-A3",
        "not-applicable; proof=type=fixture; proof=unit=fixture",
    ] {
        assert_matrix_rejected(&fixture, mutate_field(&covered, 10, value));
        rejected += 1;
    }

    for field in 2..=11 {
        assert_matrix_rejected(&fixture, mutate_field(&covered, field, ""));
        rejected += 1;
    }
    fixture.write_inventory(&[covered.clone(), covered]);
    assert!(check_repository(&fixture.root, "U-A1").is_err());
    rejected += 1;

    assert_eq!(rejected, 252);
}

fn mutate_field(row: &str, field: usize, value: &str) -> String {
    let mut fields = row.split('\t').map(str::to_owned).collect::<Vec<_>>();
    assert_eq!(fields.len(), 14);
    fields[field] = value.to_owned();
    fields.join("\t")
}

fn assert_matrix_rejected(fixture: &InventoryFixture, row: String) {
    fixture.write_inventory(&[row]);
    assert!(
        check_repository(&fixture.root, "U-A1").is_err(),
        "negative matrix case unexpectedly passed"
    );
}
