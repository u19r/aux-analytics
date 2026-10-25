use super::verification::{projected_rows_revision, projected_values_to_storage_item};
use crate::ProjectedCurrentRow;

#[test]
fn given_projected_values_when_serialized_then_order_and_revision_remain_canonical() {
    let row = ProjectedCurrentRow::new(
        "u1",
        [
            ("org_id".to_string(), Some("org-a".to_string())),
            ("email".to_string(), Some("a@example.test".to_string())),
        ]
        .into_iter()
        .collect(),
        crate::check_test_support::backup_position(9),
    );

    assert_eq!(
        serde_json::to_string(&row).unwrap(),
        r#"{"key":"u1","values":{"email":"a@example.test","org_id":"org-a"},"source_position":{"type":"raw_backup_object_offset","object_id":"check-fixture","offset":9},"contains_private_data":false}"#
    );
    assert_eq!(
        projected_rows_revision(std::slice::from_ref(&row)),
        "sha256:cba272b927785f8aae2bfdc261de085b9abbb3342236a1bbf52dc9503fa98b80"
    );
    assert_eq!(projected_values_to_storage_item(&row).len(), 3);
}
