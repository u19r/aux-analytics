use std::sync::LazyLock;

use axum::{
    body::Body,
    http::{Method, Request, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};

static OPENAPI_SPEC: LazyLock<Value> = LazyLock::new(crate::openapi::build_json);

const VALIDATION_ERROR_CODE: &str = "invalid_request";
const APPLICATION_JSON: &str = "application/json";

pub(crate) async fn openapi_request_validator(request: Request<Body>, next: Next) -> Response {
    if request.method() != Method::POST
        && request.method() != Method::PUT
        && request.method() != Method::PATCH
    {
        return next.run(request).await;
    }

    let Some(schema) = request_schema(request.method(), request.uri().path()) else {
        return next.run(request).await;
    };

    let (parts, body) = request.into_parts();
    let bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(err) => return validation_error(format!("failed to read request body: {err}")),
    };

    if !is_json_request(&parts.headers) {
        return validation_error("request content-type must be application/json".to_string());
    }

    let payload = match serde_json::from_slice::<Value>(&bytes) {
        Ok(payload) => payload,
        Err(err) => return validation_error(format!("request body must be valid JSON: {err}")),
    };

    let validator = match jsonschema::validator_for(&schema) {
        Ok(validator) => validator,
        Err(err) => return validation_error(format!("request schema is invalid: {err}")),
    };
    let errors = validator
        .iter_errors(&payload)
        .map(|err| format!("{}: {}", err.instance_path(), err))
        .collect::<Vec<_>>();
    if !errors.is_empty() {
        return validation_error(format!(
            "request body failed schema validation: {}",
            errors.join("; ")
        ));
    }

    next.run(Request::from_parts(parts, Body::from(bytes)))
        .await
}

fn request_schema(method: &Method, path: &str) -> Option<Value> {
    let paths = OPENAPI_SPEC.get("paths")?.as_object()?;
    let method_key = method.as_str().to_ascii_lowercase();
    for (template, item) in paths {
        if !path_matches(template, path) {
            continue;
        }
        let operation = item.get(&method_key)?;
        return operation
            .pointer("/requestBody/content/application~1json/schema")
            .cloned()
            .map(resolve_schema_refs);
    }
    None
}

fn resolve_schema_refs(value: Value) -> Value {
    match value {
        Value::Object(mut object) => {
            if let Some(Value::String(reference)) = object.get("$ref")
                && let Some(schema_name) = reference.strip_prefix("#/components/schemas/")
                && let Some(schema) =
                    OPENAPI_SPEC.pointer(&format!("/components/schemas/{schema_name}"))
            {
                return resolve_schema_refs(schema.clone());
            }
            for child in object.values_mut() {
                *child = resolve_schema_refs(std::mem::take(child));
            }
            Value::Object(object)
        }
        Value::Array(items) => Value::Array(items.into_iter().map(resolve_schema_refs).collect()),
        other => other,
    }
}

fn path_matches(template: &str, path: &str) -> bool {
    let template_parts = template.trim_matches('/').split('/').collect::<Vec<_>>();
    let path_parts = path.trim_matches('/').split('/').collect::<Vec<_>>();
    template_parts.len() == path_parts.len()
        && template_parts
            .iter()
            .zip(path_parts.iter())
            .all(|(left, right)| (left.starts_with('{') && left.ends_with('}')) || left == right)
}

fn is_json_request(headers: &axum::http::HeaderMap) -> bool {
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
