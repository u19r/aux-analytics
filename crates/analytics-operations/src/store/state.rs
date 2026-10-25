use super::*;

/// The durable lease held by one execution worker.
///
/// The generation is monotonic for an operation. A retry can take over only
/// after the previous lease expires, and every effect write must carry the
/// generation it acquired. The owner token is deliberately opaque to callers
/// and is persisted so a same-actor retry cannot masquerade as the old worker.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExecutionAttempt {
    pub(super) operation_id: OperationId,
    pub(super) actor: OperationActor,
    pub(super) generation: u64,
    pub(super) owner: String,
}

impl ExecutionAttempt {
    #[must_use]
    pub fn operation_id(&self) -> &OperationId {
        &self.operation_id
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub fn actor(&self) -> &OperationActor {
        &self.actor
    }

    #[must_use]
    pub fn owner(&self) -> &str {
        self.owner.as_str()
    }

    pub fn maintenance_authority(
        &self,
    ) -> Result<MaintenanceExecutionAuthority, OperationTypeError> {
        MaintenanceExecutionAuthority::try_new(self.operation_id.clone(), self.generation)
    }
}

pub struct OperationStore {
    pub(super) database: OperationDatabase,
}

pub(super) enum OperationDatabase {
    File(std::path::PathBuf),
    Memory(Connection),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct StoredOperation {
    pub operation_id: OperationId,
    pub kind: OperationKind,
    pub actor: OperationActor,
    pub target_tables: Vec<String>,
    pub dry_run: bool,
    pub rate_limit: crate::RateLimitPolicy,
    pub payload: serde_json::Value,
    pub phase: OperationPhase,
    pub status: OperationStatus,
    pub cancellation_state: CancellationState,
    pub cursor: Option<OperationCursor>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub revision: u64,
    pub progress: serde_json::Value,
    pub execution_generation: u64,
    pub execution_owner: Option<String>,
    pub execution_lease_until_ms: i64,
}

impl StoredOperation {
    #[must_use]
    pub fn execution_attempt(&self) -> Option<ExecutionAttempt> {
        let owner = self.execution_owner.clone()?;
        if self.execution_generation == 0 || owner.is_empty() {
            return None;
        }
        Some(ExecutionAttempt {
            operation_id: self.operation_id.clone(),
            actor: self.actor.clone(),
            generation: self.execution_generation,
            owner,
        })
    }
}
