use super::{response::ingest_failure_response, *};

#[utoipa::path(
    get,
    path = "/manifest",
    responses((status = 200, body = analytics_contract::AnalyticsManifest, description = "Active analytics manifest")),
    tag = "Analytics"
)]
pub(crate) async fn manifest(
    State(app_state): State<Arc<AppState>>,
) -> Json<Arc<AnalyticsManifest>> {
    let manifest = app_state.manifest.read().await.clone();
    Json(manifest)
}

#[utoipa::path(
    post,
    path = "/tables",
    request_body = analytics_contract::TableRegistration,
    responses(
        (status = 200, body = analytics_contract::TableRegistration, description = "Registered analytics table"),
        (status = 400, body = ErrorResponse, description = "Invalid table registration")
    ),
    tag = "Analytics"
)]
pub(crate) async fn register_table(
    State(app_state): State<Arc<AppState>>,
    ValidatedJson(table): ValidatedJson<TableRegistration>,
) -> Response {
    match register_table_batch(&app_state, vec![table]).await {
        Ok(mut registered) => Json(registered.pop().expect("single table batch")).into_response(),
        Err(message) => error_response(StatusCode::BAD_REQUEST, &message),
    }
}

#[utoipa::path(
    post,
    path = "/tables/batch",
    request_body = Vec<analytics_contract::TableRegistration>,
    responses(
        (status = 200, body = Vec<analytics_contract::TableRegistration>, description = "Registered analytics tables"),
        (status = 400, body = ErrorResponse, description = "Invalid table registrations")
    ),
    tag = "Analytics"
)]
pub(crate) async fn register_tables(
    State(app_state): State<Arc<AppState>>,
    ValidatedJson(tables): ValidatedJson<Vec<TableRegistration>>,
) -> Response {
    match register_table_batch(&app_state, tables).await {
        Ok(registered) => Json(registered).into_response(),
        Err(message) => error_response(StatusCode::BAD_REQUEST, &message),
    }
}

async fn register_table_batch(
    app_state: &AppState,
    tables: Vec<TableRegistration>,
) -> Result<Vec<TableRegistration>, String> {
    if tables.is_empty() {
        return Err("at least one table registration is required".to_string());
    }
    let mut active_manifest = app_state.manifest.write().await;
    let mut candidate_manifest = active_manifest.as_ref().clone();
    for table in &tables {
        upsert_manifest_table(&mut candidate_manifest, table.clone());
    }
    let candidate_manifest = Arc::new(candidate_manifest);
    let candidate_manifest_for_engine = Arc::clone(&candidate_manifest);
    if let Err(err) = app_state
        .engine
        .with_write(move |engine| engine.ensure_manifest(&candidate_manifest_for_engine))
        .await
    {
        return Err(err.to_string());
    }
    *active_manifest = candidate_manifest;
    Ok(tables)
}

fn upsert_manifest_table(manifest: &mut AnalyticsManifest, table: TableRegistration) {
    if let Some(existing) = manifest
        .tables
        .iter_mut()
        .find(|registered| registered.analytics_table_name == table.analytics_table_name)
    {
        *existing = table;
    } else {
        manifest.tables.push(table);
    }
}

#[utoipa::path(
    post,
    path = "/ingest/{analytics_table_name}",
    params(("analytics_table_name" = String, Path, description = "Registered analytics table name")),
    request_body = IngestStreamRecordRequest,
    responses(
        (status = 200, body = IngestResponse, description = "Ingest outcome"),
        (status = 400, body = ErrorResponse, description = "Invalid request or ingest failure")
    ),
    tag = "Analytics"
)]
pub(crate) async fn ingest_stream_record(
    State(app_state): State<Arc<AppState>>,
    Path(analytics_table_name): Path<String>,
    ValidatedJson(request): ValidatedJson<IngestStreamRecordRequest>,
) -> Response {
    let started = Instant::now();
    match app_state
        .ingest_record_into_table(&analytics_table_name, request)
        .await
    {
        Ok(response) => {
            record_ingest_metrics("success", started);
            Json(response).into_response()
        }
        Err(error) => ingest_failure_response(
            &analytics_table_name,
            &error,
            started,
            "analytics record ingestion failed",
        ),
    }
}

#[utoipa::path(
    post,
    path = "/ingest-batch/{analytics_table_name}",
    params(("analytics_table_name" = String, Path, description = "Registered analytics table name")),
    request_body = IngestStreamRecordBatchRequest,
    responses(
        (status = 200, body = IngestBatchResponse, description = "Batch ingest outcomes"),
        (status = 400, body = ErrorResponse, description = "Invalid request or batch ingest failure")
    ),
    tag = "Analytics"
)]
pub(crate) async fn ingest_stream_record_batch(
    State(app_state): State<Arc<AppState>>,
    Path(analytics_table_name): Path<String>,
    ValidatedJson(request): ValidatedJson<IngestStreamRecordBatchRequest>,
) -> Response {
    let started = Instant::now();
    if let Err(message) = app_state.validate_ingest_batch(&request, started) {
        return error_response(StatusCode::BAD_REQUEST, message);
    }
    match app_state
        .ingest_record_batch_into_table(&analytics_table_name, request)
        .await
    {
        Ok(response) => {
            record_ingest_metrics("success", started);
            Json(response).into_response()
        }
        Err(error) => ingest_failure_response(
            &analytics_table_name,
            &error,
            started,
            "analytics record batch ingestion failed",
        ),
    }
}
