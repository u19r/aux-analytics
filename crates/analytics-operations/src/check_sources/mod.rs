use std::path::Path;

use ::duckdb::{Connection, params};
use analytics_contract::TableRegistration;
use serde::{Deserialize, Serialize, ser::SerializeMap};
use storage_types::VecMap;

use crate::{
    CheckError, CheckResult, CheckRow, CheckRowStream, CheckRowStreamItem, CheckRowStreamSource,
    CheckSourcePosition, LocalCheckDataset,
};

mod backend;
mod paging;
mod projected_row;
mod readers;

pub use backend::{CheckBackendKind, CheckBackendSupport, CheckRowSource};
pub use paging::{CurrentRowPageRequest, CurrentRowStreamOrder, ProjectedCurrentRowPage};
pub use projected_row::{ProjectedCurrentRow, ProjectedCurrentRowItem};
pub use readers::{
    CurrentRowPageReader, CurrentRowReader, CurrentRowStreamReader, ProductionCurrentRowPageReader,
};

mod duckdb;
mod duckdb_connection;
mod duckdb_projection;
mod duckdb_scan;
#[cfg(test)]
mod duckdb_scan_experiment_tests;
#[cfg(test)]
mod duckdb_scan_tests;
#[cfg(test)]
mod duckdb_tests;
mod projection;
mod row_conversion;
mod row_hash;
mod static_backend;
mod stream;

pub use stream::ProjectedCurrentRowStream;

#[cfg(test)]
mod row_conversion_tests;

pub use duckdb::{DuckDbAnalyticalCheckRows, DuckDbCheckRows};
pub use projection::{
    DuckDbTableScanCurrentRowReader, FixtureCurrentRowReader, FixturePagedCurrentRowReader,
    ProductionCurrentRowReader,
};
use row_conversion::{
    ProjectedCheckRowStreamSource, current_row_cursor_offset, current_row_sql_bound,
    projected_stream_to_check_rows, quote_identifier, sort_projected_current_rows,
    sort_projected_pages, source_position_json_to_check_position, validate_identifier,
};
use row_hash::CheckRowHash;
pub use static_backend::{
    AuxStorageCheckRows, CheckRowProjection, DuckLakeCheckRows, DynamoDbCheckRows,
    LocalFixtureCheckRows, ProjectionBackedCheckRows, StaticCheckBackendRows,
};

#[cfg(test)]
mod stream_tests;
