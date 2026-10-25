use axum::http::HeaderValue;
use config::RootConfig;
use tower_http::cors::CorsLayer;

pub(super) fn cors_layer(root: &RootConfig) -> CorsLayer {
    let mut layer = CorsLayer::new()
        .allow_methods([
            axum::http::Method::GET,
            axum::http::Method::POST,
            axum::http::Method::PUT,
            axum::http::Method::PATCH,
            axum::http::Method::DELETE,
            axum::http::Method::OPTIONS,
        ])
        .allow_headers([axum::http::header::CONTENT_TYPE]);
    match cors_origins(root) {
        CorsOrigins::Any => layer = layer.allow_origin(tower_http::cors::Any),
        CorsOrigins::Exact(origins) => layer = layer.allow_origin(origins),
        CorsOrigins::None => {}
    }
    layer
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CorsOrigins {
    Any,
    Exact(Vec<HeaderValue>),
    None,
}

pub(crate) fn cors_origins(root: &RootConfig) -> CorsOrigins {
    let origins = &root.http.cors.allow_origins;
    if origins.iter().any(|origin| origin == "*") {
        return CorsOrigins::Any;
    }
    let vals: Vec<HeaderValue> = origins
        .iter()
        .filter_map(|origin| HeaderValue::from_str(origin).ok())
        .collect();
    if vals.is_empty() {
        CorsOrigins::None
    } else {
        CorsOrigins::Exact(vals)
    }
}
