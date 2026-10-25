use axum::http::Method;

use crate::request_validation::RequestValidators;

#[test]
fn given_compiled_routes_when_paths_matched_then_literals_and_parameters_match_exactly() {
    let validators = RequestValidators::compile();
    assert!(validators.schema(&Method::POST, "/ingest/users").is_some());
    assert!(
        validators
            .schema(&Method::POST, "/ingest/users/extra")
            .is_none()
    );
    assert!(validators.schema(&Method::GET, "/ingest/users").is_none());
    assert!(
        validators
            .schema(&Method::POST, "/operations/op-1/cancel")
            .is_none()
    );
}
