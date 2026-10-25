use std::fs;

use analytics_contract::StructuredQuery;

use crate::error::CliResult;

pub(crate) fn read_structured_query(path: &std::path::Path) -> CliResult<StructuredQuery> {
    let contents = fs::read_to_string(path)?;
    let query = serde_json::from_str::<StructuredQuery>(contents.as_str())?;
    query.validate_shape()?;
    Ok(query)
}
