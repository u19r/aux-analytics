use analytics_operations::CurrentRowPageRequest;

use super::{projection::*, test_support::projection};

#[test]
fn given_reserved_projection_names_when_scan_requests_are_built_then_all_columns_are_aliased() {
    let mut projection = projection();
    projection.key_column = "status".into();
    projection.hash_columns = vec!["value".into(), "status".into()];
    projection.source_position_column = None;
    projection.private_data_column = None;
    let request = CurrentRowPageRequest {
        projection,
        cursor: None,
        page_size: 10,
    };
    let aux = aux_storage_scan_request(&request, None).unwrap();
    let dynamo = dynamodb_scan_request(&request, None).unwrap();
    assert_eq!(aux.projection_expression, "#c0, #c1");
    assert_eq!(dynamo.projection_expression, aux.projection_expression);
    assert_eq!(
        aux.expression_attribute_names
            .get("#c0")
            .map(String::as_str),
        Some("status")
    );
    assert_eq!(
        aux.expression_attribute_names
            .get("#c1")
            .map(String::as_str),
        Some("value")
    );
    assert_eq!(
        dynamo.expression_attribute_names,
        aux.expression_attribute_names
    );
}
