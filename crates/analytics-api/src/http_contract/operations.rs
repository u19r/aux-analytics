use analytics_operations::{OperationEvent, StoredOperation, TableFixRequest, TrimRequest};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::{
    PartialSchema, ToSchema,
    openapi::{
        Ref, RefOr,
        schema::{AdditionalProperties, Object, OneOf, Schema, Type},
    },
};

/// Maintenance operations accepted by the HTTP submission endpoint.
///
/// The request contains one table-fix or trim operation. Operation attribution
/// and execution policy are assigned by the service rather than by the caller.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(
    tag = "kind",
    content = "request",
    rename_all = "snake_case",
    deny_unknown_fields
)]
#[schemars(deny_unknown_fields)]
pub(crate) enum HttpMaintenanceRequest {
    TableFix(TableFixRequest),
    Trim(TrimRequest),
}

// Utoipa does not propagate deny_unknown_fields to adjacent enum envelopes.
// Keep the closed envelope schema at the request boundary that enforces it.
impl PartialSchema for HttpMaintenanceRequest {
    fn schema() -> RefOr<Schema> {
        OneOf::builder()
            .description(Some(
                "Maintenance operations accepted by the HTTP submission endpoint.",
            ))
            .item(Self::variant_schema::<TableFixRequest>("table_fix"))
            .item(Self::variant_schema::<TrimRequest>("trim"))
            .into()
    }
}

impl ToSchema for HttpMaintenanceRequest {
    fn schemas(schemas: &mut Vec<(String, RefOr<Schema>)>) {
        schemas.push((
            TableFixRequest::name().into_owned(),
            TableFixRequest::schema(),
        ));
        TableFixRequest::schemas(schemas);
        schemas.push((TrimRequest::name().into_owned(), TrimRequest::schema()));
        TrimRequest::schemas(schemas);
    }
}

impl HttpMaintenanceRequest {
    fn variant_schema<Request: ToSchema>(kind: &str) -> RefOr<Schema> {
        Object::builder()
            .property(
                "kind",
                Object::builder()
                    .schema_type(Type::String)
                    .enum_values(Some([kind])),
            )
            .property("request", Ref::from_schema_name(Request::name()))
            .required("kind")
            .required("request")
            .additional_properties(Some(AdditionalProperties::FreeForm(false)))
            .into()
    }
}

impl From<HttpMaintenanceRequest> for analytics_operations::MaintenanceRequest {
    fn from(request: HttpMaintenanceRequest) -> Self {
        match request {
            HttpMaintenanceRequest::TableFix(request) => Self::table_fix(request),
            HttpMaintenanceRequest::Trim(request) => Self::trim(request),
        }
    }
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub(crate) struct OperationListResponse {
    pub(crate) operations: Vec<OperationStatusResponse>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub(crate) struct OperationStatusResponse {
    pub(crate) operation_id: String,
    pub(crate) kind: String,
    pub(crate) actor: String,
    pub(crate) target_tables: Vec<String>,
    pub(crate) dry_run: bool,
    pub(crate) phase: String,
    pub(crate) status: String,
    pub(crate) cancellation_state: String,
    pub(crate) cursor: Option<OperationCursorResponse>,
    pub(crate) created_at_ms: i64,
    pub(crate) updated_at_ms: i64,
}

impl From<StoredOperation> for OperationStatusResponse {
    fn from(operation: StoredOperation) -> Self {
        Self {
            operation_id: operation.operation_id.to_string(),
            kind: operation.kind.to_string(),
            actor: operation.actor.into(),
            target_tables: operation.target_tables,
            dry_run: operation.dry_run,
            phase: operation.phase.to_string(),
            status: operation.status.to_string(),
            cancellation_state: operation.cancellation_state.to_string(),
            cursor: operation.cursor.map(Into::into),
            created_at_ms: operation.created_at_ms,
            updated_at_ms: operation.updated_at_ms,
        }
    }
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub(crate) struct OperationAuditResponse {
    pub(crate) events: Vec<OperationAuditEventResponse>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub(crate) struct OperationAuditEventResponse {
    pub(crate) operation_id: String,
    pub(crate) event_id: u64,
    pub(crate) occurred_at_ms: i64,
    pub(crate) kind: String,
    pub(crate) phase: String,
    pub(crate) status: String,
    pub(crate) cursor: Option<OperationCursorResponse>,
    pub(crate) message: Option<String>,
}

impl From<OperationEvent> for OperationAuditEventResponse {
    fn from(event: OperationEvent) -> Self {
        Self {
            operation_id: event.operation_id.to_string(),
            event_id: event.event_id,
            occurred_at_ms: event.occurred_at_ms,
            kind: event.kind.to_string(),
            phase: event.phase.to_string(),
            status: event.status.to_string(),
            cursor: event.cursor.map(Into::into),
            message: event.message,
        }
    }
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub(crate) struct OperationCancelResponse {
    pub(crate) operation_id: String,
    pub(crate) cancellation_requested: bool,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub(crate) struct OperationCursorResponse {
    pub(crate) label: String,
    pub(crate) position: u64,
}

impl From<analytics_operations::OperationCursor> for OperationCursorResponse {
    fn from(cursor: analytics_operations::OperationCursor) -> Self {
        Self {
            label: cursor.label,
            position: cursor.position,
        }
    }
}
