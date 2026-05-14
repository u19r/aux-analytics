use std::fs;

use analytics_contract::StructuredQuery;
use config::{
    ConfigError, load_with_override_args, resolve_manifest_path as config_resolve_manifest_path,
    validate_source_config as config_validate_source_config,
};

use crate::error::{CliError, CliResult};

pub(crate) fn read_structured_query(path: &std::path::Path) -> CliResult<StructuredQuery> {
    let contents = fs::read_to_string(path)?;
    let query = serde_json::from_str::<StructuredQuery>(contents.as_str())?;
    query.validate_shape()?;
    Ok(query)
}

pub(crate) fn load_config(
    config_path: Option<&std::path::Path>,
    override_args: &[String],
) -> Result<std::sync::Arc<config::Config>, ConfigError> {
    load_with_override_args(config_path, override_args)
}

pub(crate) fn resolve_manifest_path(
    manifest_arg: Option<&str>,
    root: &config::RootConfig,
) -> CliResult<String> {
    config_resolve_manifest_path(manifest_arg, root).map_err(CliError::from)
}

pub(crate) fn validate_source_config(source: &config::AnalyticsSourceConfig) -> CliResult<()> {
    config_validate_source_config(source).map_err(CliError::from)
}
