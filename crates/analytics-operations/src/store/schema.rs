use super::*;
use crate::constants::OPERATION_SCHEMA_SQL;

impl OperationStore {
    pub(super) fn initialize_schema(conn: &Connection) -> OperationStoreResult<()> {
        conn.execute_batch(OPERATION_SCHEMA_SQL)?;
        Ok(())
    }
}
