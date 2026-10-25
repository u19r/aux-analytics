use std::{path::Path, str::FromStr};

use duckdb::{Connection, OptionalExt, params};
use thiserror::Error;

use crate::{
    CancellationState, MaintenanceExecutionAuthority, OperationActor, OperationChunkKey,
    OperationCursor, OperationEvent, OperationEventKind, OperationId, OperationKind,
    OperationPhase, OperationRequest, OperationStatus, constants::EXECUTION_LEASE_MS,
    operation_contract::OperationTypeError,
};

mod admission;
mod cancellation;
mod connection;
mod effects;
mod lifecycle;
mod reports;
mod rows;
mod schema;
mod transition;

mod error;
mod state;

pub(crate) use effects::EffectAdmissionMode;
pub use error::{OperationStoreError, OperationStoreResult};
pub(crate) use reports::{OperationReportKind, OperationReportWrite};
pub use state::{ExecutionAttempt, OperationStore, StoredOperation};
pub use transition::{OperationEffectCompletion, OperationTransition};

#[cfg(test)]
mod connection_tests;
