use super::*;

#[utoipa::path(
    get,
    path = "/operations",
    responses(
        (status = 200, body = OperationListResponse, description = "Durable operation list"),
        (status = 503, body = ErrorResponse, description = "Durable operation storage is unavailable")
    ),
    tag = "Operations"
)]
pub(crate) async fn list_operations(State(app_state): State<Arc<AppState>>) -> Response {
    match operation_store_response(&app_state, |store| {
        let operations = store
            .list_operations()?
            .into_iter()
            .map(Into::into)
            .collect();
        Ok(OperationListResponse { operations })
    }) {
        Ok(value) => Json(value).into_response(),
        Err(response) => *response,
    }
}

#[utoipa::path(
    post,
    path = "/operations",
    summary = "Submit a table-fix or trim operation",
    description = "Create a durable operation from a table-fix or trim request.",
    request_body(
        content = HttpMaintenanceRequest,
        description = "Table-fix or trim request with operation-specific validation and execution options."
    ),
    responses(
        (status = 200, body = analytics_operations::MaintenanceSubmission, description = "Durable maintenance operation submitted"),
        (status = 400, body = ErrorResponse, description = "Invalid maintenance request"),
        (status = 409, body = ErrorResponse, description = "Operation id conflicts with an existing request"),
        (status = 503, body = ErrorResponse, description = "Durable operation storage is unavailable")
    ),
    tag = "Operations"
)]
pub(crate) async fn submit_operation(
    State(app_state): State<Arc<AppState>>,
    ValidatedJson(request): ValidatedJson<HttpMaintenanceRequest>,
) -> Response {
    let actor = http_operation_actor();
    let request = analytics_operations::MaintenanceRequest::from(request);
    match operation_store_response(&app_state, |store| {
        let operations = MaintenanceOperations::new(&store);
        operations
            .submit(&request, &actor)
            .map_err(OperationApiError::from)
    }) {
        Ok(value) => Json(value).into_response(),
        Err(response) => *response,
    }
}

#[utoipa::path(
    get,
    path = "/operations/{operation_id}",
    params(("operation_id" = String, Path, description = "Operation id")),
    responses(
        (status = 200, body = OperationStatusResponse, description = "Durable operation status"),
        (status = 404, body = ErrorResponse, description = "Operation not found"),
        (status = 503, body = ErrorResponse, description = "Durable operation storage is unavailable")
    ),
    tag = "Operations"
)]
pub(crate) async fn operation_status(
    State(app_state): State<Arc<AppState>>,
    Path(operation_id): Path<String>,
) -> Response {
    operation_response_for_id(&app_state, operation_id, |store, operation_id| {
        Ok(OperationStatusResponse::from(
            store.show_operation(operation_id)?,
        ))
    })
}

#[utoipa::path(
    get,
    path = "/operations/{operation_id}/audit",
    params(("operation_id" = String, Path, description = "Operation id")),
    responses(
        (status = 200, body = OperationAuditResponse, description = "Durable operation audit events"),
        (status = 404, body = ErrorResponse, description = "Operation not found"),
        (status = 503, body = ErrorResponse, description = "Durable operation storage is unavailable")
    ),
    tag = "Operations"
)]
pub(crate) async fn operation_audit(
    State(app_state): State<Arc<AppState>>,
    Path(operation_id): Path<String>,
) -> Response {
    operation_response_for_id(&app_state, operation_id, |store, operation_id| {
        let events = store
            .audit_events(operation_id)?
            .into_iter()
            .map(OperationAuditEventResponse::from)
            .collect();
        Ok(OperationAuditResponse { events })
    })
}

#[utoipa::path(
    post,
    path = "/operations/{operation_id}/cancel",
    params(
        ("operation_id" = String, Path, description = "Operation id")
    ),
    responses(
        (status = 200, body = OperationCancelResponse, description = "Cancellation request accepted"),
        (status = 404, body = ErrorResponse, description = "Operation not found"),
        (status = 503, body = ErrorResponse, description = "Durable operation storage is unavailable")
    ),
    tag = "Operations"
)]
pub(crate) async fn cancel_operation(
    State(app_state): State<Arc<AppState>>,
    Path(operation_id): Path<String>,
) -> Response {
    let actor = http_operation_actor();
    operation_response_for_id(&app_state, operation_id, |store, operation_id| {
        MaintenanceOperations::new(store).request_cancellation(operation_id, &actor)?;
        Ok(OperationCancelResponse {
            operation_id: operation_id.to_string(),
            cancellation_requested: true,
        })
    })
}

fn http_operation_actor() -> OperationActor {
    OperationActor::try_new("analytics-http-api")
        .expect("the service-owned HTTP actor is non-empty")
}
