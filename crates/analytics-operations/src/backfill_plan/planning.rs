use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillPlan {
    pub target_tables: Vec<String>,
    pub snapshot_method: SnapshotMethod,
    pub destination_backend: DestinationBackend,
    pub dry_run: bool,
    pub chunk_count: u64,
    pub estimated_rows: u64,
    pub source_impact: String,
    pub destination_impact: String,
    pub deletes_can_be_proven: bool,
    pub validation_mode: ValidationMode,
    pub validation_steps: Vec<String>,
    pub required_permissions: Vec<String>,
    pub warnings: Vec<String>,
    pub steps: Vec<BackfillPlanStep>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillPlanStep {
    pub order: u8,
    pub name: String,
}

pub trait BackfillPlanner {
    fn plan(&self, request: &BackfillRequest) -> BackfillPlanResult<BackfillPlan>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ConservativeBackfillPlanner;

impl BackfillPlanner for ConservativeBackfillPlanner {
    fn plan(&self, request: &BackfillRequest) -> BackfillPlanResult<BackfillPlan> {
        validate_request(request)?;
        let snapshot_method = choose_snapshot_method(request)?;
        let chunk_count = estimate_chunks(request.estimated_table_rows, request.chunk_target_rows);
        Ok(BackfillPlan {
            target_tables: request.target_tables.clone(),
            snapshot_method,
            destination_backend: request.destination_backend,
            dry_run: request.dry_run,
            chunk_count,
            estimated_rows: request.estimated_table_rows,
            source_impact: source_impact(snapshot_method).to_string(),
            destination_impact: "bounded writes through operation rate limits".to_string(),
            deletes_can_be_proven: request.source.can_prove_deletes,
            validation_mode: request.validation_mode,
            validation_steps: validation_steps(request.validation_mode),
            required_permissions: required_permissions(
                snapshot_method,
                request.destination_backend,
            ),
            warnings: warnings(request, snapshot_method),
            steps: vec![
                step(1, "inspect_source"),
                step(2, "inspect_destination"),
                step(3, "choose_snapshot_strategy"),
                step(4, "estimate_chunks"),
                step(5, "reserve_operation"),
                step(6, "run_validation_after_backfill"),
            ],
        })
    }
}
