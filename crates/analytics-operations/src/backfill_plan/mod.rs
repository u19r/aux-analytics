use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

use crate::RateLimitPolicy;

mod capabilities;
mod planning_steps;

pub use capabilities::{
    AuxStorageBackfillMetadata, AuxStorageBackfillMetadataAdapter, BackfillCapabilityDiscovery,
    BackfillDiscoveryAdapters, BackfillSourceCapabilities, BackfillSourceKind,
    DynamoDbBackfillMetadata, DynamoDbBackfillMetadataAdapter, StaticBackfillCapabilityDiscovery,
};
use planning_steps::{
    choose_snapshot_method, estimate_chunks, required_permissions, source_impact, step,
    validate_request, validation_steps, warnings,
};

mod error;
mod planning;
mod request;

pub use error::{BackfillPlanError, BackfillPlanResult};
pub use planning::{BackfillPlan, BackfillPlanStep, BackfillPlanner, ConservativeBackfillPlanner};
pub use request::{
    BackfillRequest, BackfillRequestInput, DestinationBackend, SnapshotMethod, ValidationMode,
};
