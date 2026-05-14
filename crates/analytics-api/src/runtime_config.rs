use std::sync::Arc;

use config::{
    ConfigError, load_optional_with_overrides, parse_override_args,
    resolve_manifest_path as config_resolve_manifest_path,
    validate_source_config as config_validate_source_config,
};

use crate::{
    cli::ApiCli,
    error::{ApiError, ApiResult},
};

pub(crate) fn load_serve_config(args: &ApiCli) -> Result<Arc<config::Config>, ConfigError> {
    let mut overrides = Vec::new();
    if let Some(port) = args.port.as_ref() {
        overrides.push(("http.bind_addr".to_string(), format!("0.0.0.0:{port}")));
    }
    overrides.extend(parse_override_args(args.overrides.as_slice())?);
    load_optional_with_overrides(args.config.as_deref(), overrides.as_slice())
}

pub(crate) fn resolve_manifest_path(
    manifest_arg: Option<&str>,
    root: &config::RootConfig,
) -> ApiResult<String> {
    config_resolve_manifest_path(manifest_arg, root).map_err(ApiError::from)
}

pub(crate) fn validate_source_config(source: &config::AnalyticsSourceConfig) -> ApiResult<()> {
    config_validate_source_config(source).map_err(ApiError::from)
}
