use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{FromRequest, Request},
    http::{HeaderMap, Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

const VALIDATION_ERROR_CODE: &str = "invalid_request";
const APPLICATION_JSON: &str = "application/json";

#[derive(Clone)]
pub(crate) struct RequestValidators(Arc<[RouteValidator]>);

#[derive(Clone)]
struct RouteValidator {
    method: Method,
    path: Box<[PathSegment]>,
    schema: jsonschema::Validator,
}

#[derive(Clone)]
enum PathSegment {
    Literal(Box<str>),
    Parameter,
}

impl RequestValidators {
    pub(crate) fn compile() -> Self {
        let spec = crate::openapi::build_json();
        let Some(paths) = spec.get("paths").and_then(Value::as_object) else {
            panic!("generated OpenAPI document has no paths object");
        };
        let mut validators = Vec::new();
        for (template, item) in paths {
            let Some(operations) = item.as_object() else {
                continue;
            };
            for (method_name, operation) in operations {
                let method_name = method_name.to_ascii_uppercase();
                let Ok(method) = Method::from_bytes(method_name.as_bytes()) else {
                    continue;
                };
                let Some(schema) = operation
                    .pointer("/requestBody/content/application~1json/schema")
                    .cloned()
                else {
                    continue;
                };
                let schema = resolve_schema_refs(schema, &spec);
                let schema = jsonschema::validator_for(&schema).unwrap_or_else(|err| {
                    panic!("generated request schema for {method} {template} is invalid: {err}")
                });
                validators.push(RouteValidator {
                    method,
                    path: compile_path(template),
                    schema,
                });
            }
        }
        Self(validators.into())
    }

    pub(crate) fn schema(&self, method: &Method, path: &str) -> Option<&jsonschema::Validator> {
        self.0
            .iter()
            .find(|route| route.method == *method && route.path_matches(path))
            .map(|route| &route.schema)
    }
}

impl RouteValidator {
    fn path_matches(&self, path: &str) -> bool {
        let mut actual = path.trim_matches('/').split('/');
        for expected in &self.path {
            let Some(actual) = actual.next() else {
                return false;
            };
            if let PathSegment::Literal(expected) = expected
                && expected.as_ref() != actual
            {
                return false;
            }
        }
        actual.next().is_none()
    }
}

fn compile_path(template: &str) -> Box<[PathSegment]> {
    template
        .trim_matches('/')
        .split('/')
        .map(|segment| {
            if segment.starts_with('{') && segment.ends_with('}') {
                PathSegment::Parameter
            } else {
                PathSegment::Literal(segment.into())
            }
        })
        .collect()
}

pub(crate) fn resolve_schema_refs(value: Value, spec: &Value) -> Value {
    match value {
        Value::Object(mut object) => {
            if let Some(Value::String(reference)) = object.get("$ref")
                && let Some(schema_name) = reference.strip_prefix("#/components/schemas/")
                && let Some(schema) = spec.pointer(&format!("/components/schemas/{schema_name}"))
            {
                return resolve_schema_refs(schema.clone(), spec);
            }
            for child in object.values_mut() {
                *child = resolve_schema_refs(std::mem::take(child), spec);
            }
            Value::Object(object)
        }
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(|item| resolve_schema_refs(item, spec))
                .collect(),
        ),
        other => other,
    }
}

pub(crate) struct ValidatedJson<T>(pub(crate) T);

impl<T, S> FromRequest<S> for ValidatedJson<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        let validators = request
            .extensions()
            .get::<RequestValidators>()
            .cloned()
            .expect("request validator registry layer is installed");
        let schema = validators
            .schema(request.method(), request.uri().path())
            .expect("validated JSON extractor is used only on registered request-body routes")
            .clone();

        if !is_json_request(request.headers()) {
            return Err(validation_error(
                "request content-type must be application/json".to_string(),
            ));
        }

        let bytes = Bytes::from_request(request, state)
            .await
            .map_err(IntoResponse::into_response)?;
        let payload = serde_json::from_slice::<Value>(&bytes)
            .map_err(|err| validation_error(format!("request body must be valid JSON: {err}")))?;
        let errors = schema
            .iter_errors(&payload)
            .map(|err| format!("{}: {}", err.instance_path(), err))
            .collect::<Vec<_>>();
        if !errors.is_empty() {
            return Err(validation_error(format!(
                "request body failed schema validation: {}",
                errors.join("; ")
            )));
        }

        serde_json::from_value(payload)
            .map(Self)
            .map_err(|err| typed_validation_error(err.to_string()))
    }
}

fn is_json_request(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(';')
                .next()
                .is_some_and(|mime| mime.trim() == APPLICATION_JSON)
        })
}

fn validation_error(message: impl Into<String>) -> Response {
    (
        StatusCode::BAD_REQUEST,
        [(header::CONTENT_TYPE, APPLICATION_JSON)],
        axum::Json(json!({ "code": VALIDATION_ERROR_CODE, "message": message.into() })),
    )
        .into_response()
}

fn typed_validation_error(message: String) -> Response {
    (
        StatusCode::BAD_REQUEST,
        axum::Json(json!({ "error": message })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compiled_routes_match_literals_and_parameters_exactly() {
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
}
