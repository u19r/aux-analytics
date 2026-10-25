use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{FromRequest, Request},
    http::{HeaderMap, Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::constants::{APPLICATION_JSON, VALIDATION_ERROR_CODE};

#[derive(Clone)]
pub(crate) struct RequestValidators(Arc<[RouteValidator]>);

struct RouteValidator {
    method: Method,
    path: Box<[PathSegment]>,
    schema: jsonschema::Validator,
}

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
                let schema = bundle_schema_refs(schema, &spec);
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

fn bundle_schema_refs(mut schema: Value, spec: &Value) -> Value {
    let mut definitions = spec
        .pointer("/components/schemas")
        .cloned()
        .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
    rewrite_component_refs(&mut schema);
    rewrite_component_refs(&mut definitions);
    let Value::Object(definitions) = definitions else {
        panic!("generated OpenAPI components.schemas is not an object");
    };
    let Value::Object(mut schema) = schema else {
        return json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$defs": definitions,
            "allOf": [schema],
        });
    };
    schema.insert(
        "$schema".to_string(),
        Value::String("https://json-schema.org/draft/2020-12/schema".to_string()),
    );
    schema.insert("$defs".to_string(), Value::Object(definitions));
    Value::Object(schema)
}

fn rewrite_component_refs(value: &mut Value) {
    match value {
        Value::Object(object) => {
            if let Some(Value::String(reference)) = object.get_mut("$ref")
                && let Some(schema_name) = reference.strip_prefix("#/components/schemas/")
            {
                *reference = format!("#/$defs/{schema_name}");
            }
            for child in object.values_mut() {
                rewrite_component_refs(child);
            }
        }
        Value::Array(items) => {
            for item in items {
                rewrite_component_refs(item);
            }
        }
        _ => {}
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
            .expect("validated JSON extractor is used only on registered request-body routes");

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
