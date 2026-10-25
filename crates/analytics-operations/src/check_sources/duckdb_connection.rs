use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use super::{CheckError, CheckResult};

/// File inputs are opened independently. Live databases, including attached
/// catalogs and in-memory fixtures, must share their writer's native database.
#[derive(Debug, Clone)]
pub(super) enum DuckDbReadConnection {
    File(String),
    Shared(Arc<Mutex<duckdb::Connection>>),
}

impl DuckDbReadConnection {
    pub(super) fn try_from_connection(connection: &duckdb::Connection) -> CheckResult<Self> {
        Ok(Self::Shared(Arc::new(Mutex::new(connection.try_clone()?))))
    }

    pub(super) fn open_connection(&self) -> CheckResult<duckdb::Connection> {
        match self {
            Self::File(path) => Ok(duckdb::Connection::open(Path::new(path))?),
            Self::Shared(connection) => Ok(connection
                .lock()
                .map_err(|_| {
                    CheckError::InvalidBackendConfiguration(
                        "DuckDB read connection lock poisoned".to_owned(),
                    )
                })?
                .try_clone()?),
        }
    }
}
