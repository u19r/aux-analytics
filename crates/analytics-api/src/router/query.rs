use super::*;

#[utoipa::path(
    post,
    path = "/unscoped-sql-query",
    request_body = UnscopedSqlQueryRequest,
    responses(
        (status = 400, body = ErrorResponse, description = "Use tenant-scoped structured queries for HTTP reads")
    ),
    tag = "Analytics"
)]
pub(crate) async fn unscoped_sql_query(
    State(_app_state): State<Arc<AppState>>,
    ValidatedJson(request): ValidatedJson<UnscopedSqlQueryRequest>,
) -> Response {
    let started = Instant::now();
    let _sql_len = request.sql.len();
    record_query_metrics("raw", "rejected", started);
    error_response_with_code(
        StatusCode::BAD_REQUEST,
        "Use tenant-query with target_tenant_id for tenant-scoped HTTP reads",
        Some("raw_sql_unavailable"),
    )
}

#[utoipa::path(
    post,
    path = "/unscoped-structured-query",
    request_body = UnscopedStructuredQueryRequest,
    responses(
        (status = 200, body = QueryResponse, description = "Query explicitly unscoped rows as JSON objects"),
        (status = 400, body = ErrorResponse, description = "Invalid request or query")
    ),
    tag = "Analytics"
)]
pub(crate) async fn unscoped_structured_query(
    State(app_state): State<Arc<AppState>>,
    ValidatedJson(request): ValidatedJson<UnscopedStructuredQueryRequest>,
) -> Response {
    let started = Instant::now();
    let manifest = app_state.manifest.read().await.clone();
    match app_state
        .engine
        .with_read(move |engine| {
            let prepared = prepare_unscoped_structured_query(&manifest, &request.query)?;
            let rows = engine.query_prepared_structured_json(&prepared)?;
            Ok::<_, AnalyticsEngineError>((rows, prepared))
        })
        .await
    {
        Ok(Ok((rows, prepared))) => {
            record_query_metrics("unscoped_structured", "success", started);
            Json(QueryResponse::new(rows, prepared.metadata(), started)).into_response()
        }
        Ok(Err(err)) => {
            record_query_metrics("unscoped_structured", "error", started);
            query_error_response(StatusCode::BAD_REQUEST, &err)
        }
        Err(err) => {
            record_query_metrics("unscoped_structured", "error", started);
            error_response(StatusCode::SERVICE_UNAVAILABLE, &err.to_string())
        }
    }
}

#[utoipa::path(
    post,
    path = "/tenant-query",
    request_body = TenantQueryRequest,
    responses(
        (status = 200, body = QueryResponse, description = "Query tenant-scoped rows as JSON objects"),
        (status = 400, body = ErrorResponse, description = "Invalid request or query")
    ),
    tag = "Analytics"
)]
pub(crate) async fn tenant_query(
    State(app_state): State<Arc<AppState>>,
    ValidatedJson(request): ValidatedJson<TenantQueryRequest>,
) -> Response {
    let started = Instant::now();
    let manifest = app_state.manifest.read().await.clone();
    match app_state
        .engine
        .with_read(move |engine| {
            let prepared = prepare_tenant_structured_query(
                &manifest,
                &request.query,
                request.target_tenant_id.as_str(),
            )?;
            let rows = engine.query_prepared_structured_json(&prepared)?;
            Ok::<_, AnalyticsEngineError>((rows, prepared))
        })
        .await
    {
        Ok(Ok((rows, prepared))) => {
            record_query_metrics("tenant_structured", "success", started);
            Json(QueryResponse::new(rows, prepared.metadata(), started)).into_response()
        }
        Ok(Err(err)) => {
            record_query_metrics("tenant_structured", "error", started);
            query_error_response(StatusCode::BAD_REQUEST, &err)
        }
        Err(err) => {
            record_query_metrics("tenant_structured", "error", started);
            error_response(StatusCode::SERVICE_UNAVAILABLE, &err.to_string())
        }
    }
}

#[utoipa::path(
    post,
    path = "/tenant-query-batch",
    request_body = TenantQueryBatchRequest,
    responses(
        (status = 200, body = QueryBatchResponse, description = "Query tenant-scoped rows for multiple named structured queries"),
        (status = 400, body = ErrorResponse, description = "Invalid request or query")
    ),
    tag = "Analytics"
)]
pub(crate) async fn tenant_query_batch(
    State(app_state): State<Arc<AppState>>,
    ValidatedJson(request): ValidatedJson<TenantQueryBatchRequest>,
) -> Response {
    let started = Instant::now();
    if request.queries.is_empty() {
        record_query_metrics("tenant_structured_batch", "validation_error", started);
        return error_response(
            StatusCode::BAD_REQUEST,
            "queries must contain at least one structured query",
        );
    }

    let manifest = app_state.manifest.read().await.clone();
    match execute_tenant_query_batch(
        app_state.engine.clone(),
        manifest,
        request.target_tenant_id,
        request.queries,
    )
    .await
    {
        Ok(results) => {
            record_query_metrics("tenant_structured_batch", "success", started);
            Json(QueryBatchResponse { results }).into_response()
        }
        Err(err) => {
            record_query_metrics("tenant_structured_batch", "error", started);
            error_response(err.status_code(), &err.to_string())
        }
    }
}

async fn execute_tenant_query_batch(
    engine: AnalyticsEngineAccess,
    manifest: Arc<AnalyticsManifest>,
    target_tenant_id: String,
    queries: Vec<TenantQueryBatchItem>,
) -> Result<Vec<QueryBatchResult>, TenantQueryBatchExecutionError> {
    if queries.len() > MAX_TENANT_QUERY_BATCH_ITEMS {
        return Err(TenantQueryBatchExecutionError::TooManyQueries {
            actual: queries.len(),
            maximum: MAX_TENANT_QUERY_BATCH_ITEMS,
        });
    }
    AnalyticsEngineAccess::execute_query_batch(queries, |item, cancellation| {
        let engine = engine.clone();
        let manifest = Arc::clone(&manifest);
        let target_tenant_id = target_tenant_id.clone();
        async move {
            let started = Instant::now();
            let TenantQueryBatchItem { name, query } = item;
            let (rows, prepared) = engine
                .with_cancellable_read(cancellation, move |engine| {
                    let prepared = prepare_tenant_structured_query(
                        manifest.as_ref(),
                        &query,
                        target_tenant_id.as_str(),
                    )?;
                    let rows = engine.query_prepared_structured_json(&prepared)?;
                    Ok::<_, AnalyticsEngineError>((rows, prepared))
                })
                .await??;
            let response = QueryBatchResult::new(name, rows, prepared.metadata(), started);
            Ok::<_, TenantQueryBatchExecutionError>(response)
        }
    })
    .await
}

#[derive(Debug)]
enum TenantQueryBatchExecutionError {
    TooManyQueries { actual: usize, maximum: usize },
    Query(AnalyticsEngineError),
    Access(AnalyticsEngineAccessError),
    Join(tokio::task::JoinError),
}

impl TenantQueryBatchExecutionError {
    const fn status_code(&self) -> StatusCode {
        match self {
            Self::TooManyQueries { .. } => StatusCode::BAD_REQUEST,
            Self::Query(_) => StatusCode::BAD_REQUEST,
            Self::Access(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::Join(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl fmt::Display for TenantQueryBatchExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyQueries { actual, maximum } => write!(
                formatter,
                "tenant query batch contains {actual} queries; at most {maximum} are allowed"
            ),
            Self::Query(error) => write!(formatter, "{error}"),
            Self::Access(error) => write!(formatter, "{error}"),
            Self::Join(error) => write!(formatter, "{error}"),
        }
    }
}

impl From<AnalyticsEngineError> for TenantQueryBatchExecutionError {
    fn from(error: AnalyticsEngineError) -> Self {
        Self::Query(error)
    }
}

impl From<AnalyticsEngineAccessError> for TenantQueryBatchExecutionError {
    fn from(error: AnalyticsEngineAccessError) -> Self {
        Self::Access(error)
    }
}

impl From<tokio::task::JoinError> for TenantQueryBatchExecutionError {
    fn from(error: tokio::task::JoinError) -> Self {
        Self::Join(error)
    }
}
