use super::*;

impl AnalyticsLambdaHandler {
    pub(super) fn submit_maintenance(
        &self,
        request: analytics_operations::MaintenanceRequest,
        actor: String,
    ) -> Result<Box<RawValue>, AnalyticsLambdaError> {
        self.ensure_mutation_allowed()?;
        let actor = OperationActor::try_new(actor)
            .map_err(|error| AnalyticsLambdaError::validation(error.to_string()))?;
        let Some(path) = self.operation_store_path.as_ref() else {
            return Err(AnalyticsLambdaError::validation(
                "operation store is not configured",
            ));
        };
        let store =
            OperationStore::connect_duckdb(path.as_path()).map_err(MaintenanceError::Store)?;
        let operations = MaintenanceOperations::new(&store);
        serialize_response(operations.submit(&request, &actor)?)
    }
}
