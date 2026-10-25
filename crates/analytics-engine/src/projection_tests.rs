use analytics_contract::StorageValue;
use storage_types::VecMap;

use crate::projection::{extract_from_item, parse_attribute_path};

#[test]
fn given_projection_path_with_alias_when_projected_then_expression_name_is_resolved() {
    let item = VecMap::from_iter([(
        "profile".to_string(),
        StorageValue::M(VecMap::from_iter([(
            "email-address".to_string(),
            StorageValue::S("a@example.com".to_string()),
        )])),
    )]);
    let names = VecMap::from_iter([("#email".to_string(), "email-address".to_string())]);

    let path = parse_attribute_path("profile.#email", Some(&names)).unwrap();
    let projected = extract_from_item(&item, &path).unwrap();
    assert!(matches!(projected, std::borrow::Cow::Borrowed(_)));
    assert_eq!(
        projected.as_ref(),
        &StorageValue::S("a@example.com".to_string())
    );
}

#[test]
fn given_projection_path_that_expands_list_when_projected_then_present_children_are_collected() {
    let item = VecMap::from_iter([(
        "orders".to_string(),
        StorageValue::L(vec![
            StorageValue::M(VecMap::from_iter([(
                "total".to_string(),
                StorageValue::N("12.50".to_string()),
            )])),
            StorageValue::M(VecMap::from_iter([(
                "status".to_string(),
                StorageValue::S("cancelled".to_string()),
            )])),
            StorageValue::M(VecMap::from_iter([(
                "total".to_string(),
                StorageValue::N("99.00".to_string()),
            )])),
        ]),
    )]);

    let path = parse_attribute_path("orders[].total", None).unwrap();
    let projected = extract_from_item(&item, &path).unwrap();
    assert_eq!(
        projected.as_ref(),
        &StorageValue::L(vec![
            StorageValue::N("12.50".to_string()),
            StorageValue::N("99.00".to_string()),
        ])
    );
}

#[test]
fn given_projection_alias_without_name_map_when_projected_then_error_names_missing_map() {
    let error = parse_attribute_path("#email", None).unwrap_err();

    assert_eq!(
        error.to_string(),
        "projection attribute name requires expression attribute names: attribute_name=#email"
    );
}

#[test]
fn given_invalid_projection_path_when_projected_then_error_names_path() {
    let error = parse_attribute_path("profile..email", None).unwrap_err();

    assert_eq!(
        error.to_string(),
        "projection attribute path is invalid: path=profile..email"
    );
}
