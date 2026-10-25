use analytics_contract::{
    RangePurgeClaimHttpRequest, TenantRangePurgeEffectHttpRequest, TenantRangePurgeResponse,
};
use analytics_engine::{TenantRangePurgeEffectDisposition, TenantRangePurgeEffectRequest};

use super::*;

#[utoipa::path(
    post,
    path = "/maintenance/tenant-range-purge/claim",
    summary = "Register shared job authority for range purges",
    description = "Acknowledge a durable whole-job claim before scheduler execution. Exact replay succeeds; older or conflicting claims cannot replace registered authority.",
    request_body = RangePurgeClaimHttpRequest,
    responses(
        (status = 200, description = "Claim durably accepted or already current"),
        (status = 400, body = ErrorResponse, description = "Invalid claim descriptor"),
        (status = 409, body = ErrorResponse, description = "Stale or conflicting claim"),
        (status = 503, body = ErrorResponse, description = "Destination storage is unavailable")
    ),
    tag = "Operations"
)]
pub(crate) async fn register_range_purge_claim(
    State(app_state): State<Arc<AppState>>,
    ValidatedJson(input): ValidatedJson<RangePurgeClaimHttpRequest>,
) -> Response {
    match app_state
        .engine
        .with_write(move |engine| engine.register_range_purge_claim(&input.claim))
        .await
    {
        Ok(()) => Json(()).into_response(),
        Err(error) => purge_error_response(error),
    }
}

#[utoipa::path(
    post,
    path = "/maintenance/tenant-range-purge",
    summary = "Apply one fenced tenant-range purge chunk",
    description = "Apply a bounded deletion and its execution receipt atomically. Replays return the retained chunk result; stale fencing or conflicting requests fail.",
    request_body = TenantRangePurgeEffectHttpRequest,
    responses(
        (status = 200, body = TenantRangePurgeResponse, description = "Applied or replayed chunk result"),
        (status = 400, body = ErrorResponse, description = "Invalid purge predicate or execution authority"),
        (status = 409, body = ErrorResponse, description = "Stale fence or conflicting execution receipt"),
        (status = 503, body = ErrorResponse, description = "Destination storage is unavailable")
    ),
    tag = "Operations"
)]
pub(crate) async fn apply_tenant_range_purge(
    State(app_state): State<Arc<AppState>>,
    ValidatedJson(input): ValidatedJson<TenantRangePurgeEffectHttpRequest>,
) -> Response {
    let result = app_state
        .engine
        .with_write(move |engine| {
            engine.apply_tenant_range_purge(TenantRangePurgeEffectRequest {
                request: &input.request,
                authority: &input.claim,
                request_id: &input.request_id,
            })
        })
        .await;
    match result {
        Ok(
            TenantRangePurgeEffectDisposition::Applied(response)
            | TenantRangePurgeEffectDisposition::AlreadyApplied(response),
        ) => Json(response).into_response(),
        Err(error) => purge_error_response(error),
    }
}

fn purge_error_response(error: AnalyticsEngineError) -> Response {
    let status = match error {
        AnalyticsEngineError::InvalidRangePurge(_)
        | AnalyticsEngineError::InvalidRemoteJobClaim
        | AnalyticsEngineError::InvalidMaintenanceEffectExecutionId
        | AnalyticsEngineError::InvalidMaintenanceEffectFence => StatusCode::BAD_REQUEST,
        AnalyticsEngineError::StaleMaintenanceEffectFence(_)
        | AnalyticsEngineError::ConflictingMaintenanceEffectReceipt(_) => StatusCode::CONFLICT,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    error_response(status, &error.to_string())
}
