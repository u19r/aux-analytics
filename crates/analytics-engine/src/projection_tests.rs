use std::collections::HashMap;

use analytics_contract::{ProjectionColumn, StorageValue};

use crate::projection::project_item;

#[test]
fn given_projection_path_with_alias_when_projected_then_expression_name_is_resolved() {
    let item = HashMap::from([(
        "profile".to_string(),
        StorageValue::M(HashMap::from([(
            "email-address".to_string(),
            StorageValue::S("a@example.com".to_string()),
        )])),
    )]);
    let names = HashMap::from([("#email".to_string(), "email-address".to_string())]);

    let projected = project_item(
        &item,
        &[ProjectionColumn {
            column_name: "email".to_string(),
            attribute_path: "profile.#email".to_string(),
            column_type: None,
        }],
        Some(&names),
    )
    .unwrap();

    assert_eq!(
        projected["email"],
        StorageValue::S("a@example.com".to_string())
    );
}

#[test]
fn given_projection_path_that_expands_list_when_projected_then_present_children_are_collected() {
    let item = HashMap::from([(
        "orders".to_string(),
        StorageValue::L(vec![
            StorageValue::M(HashMap::from([(
                "total".to_string(),
                StorageValue::N("12.50".to_string()),
            )])),
            StorageValue::M(HashMap::from([(
                "status".to_string(),
                StorageValue::S("cancelled".to_string()),
            )])),
            StorageValue::M(HashMap::from([(
                "total".to_string(),
                StorageValue::N("99.00".to_string()),
            )])),
        ]),
    )]);

    let projected = project_item(
        &item,
        &[ProjectionColumn {
            column_name: "order_totals".to_string(),
            attribute_path: "orders[].total".to_string(),
            column_type: None,
        }],
        None,
    )
    .unwrap();

    assert_eq!(
        projected["order_totals"],
        StorageValue::L(vec![
            StorageValue::N("12.50".to_string()),
            StorageValue::N("99.00".to_string())
        ])
    );
}

#[test]
fn given_projection_alias_without_name_map_when_projected_then_error_names_missing_map() {
    let error = project_item(
        &HashMap::new(),
        &[ProjectionColumn {
            column_name: "email".to_string(),
            attribute_path: "#email".to_string(),
            column_type: None,
        }],
        None,
    )
    .unwrap_err();

    assert_eq!(
        error.to_string(),
        "projection attribute name requires expression attribute names: attribute_name=#email"
    );
}

#[test]
fn given_invalid_projection_path_when_projected_then_error_names_path() {
    let error = project_item(
        &HashMap::new(),
        &[ProjectionColumn {
            column_name: "email".to_string(),
            attribute_path: "profile..email".to_string(),
            column_type: None,
        }],
        None,
    )
    .unwrap_err();

    assert_eq!(
        error.to_string(),
        "projection attribute path is invalid: path=profile..email"
    );
}
