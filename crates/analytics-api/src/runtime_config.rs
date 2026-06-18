use std::{fs, path::Path, sync::Arc};

use analytics_contract::PrivacyPolicy;
use config::{
    ConfigError, load_optional_with_overrides, parse_override_args,
    resolve_manifest_path as config_resolve_manifest_path,
    validate_ingest_config as config_validate_ingest_config,
    validate_source_config as config_validate_source_config,
};

use crate::{
    cli::ApiCli,
    error::{ApiError, ApiResult},
};

const PRIVACY_POLICY_LOAD_FAILURES_METRIC: &str = "analytics.privacy.policy_load_failures_total";

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

pub(crate) fn validate_ingest_config(ingest: &config::AnalyticsIngestConfig) -> ApiResult<()> {
    config_validate_ingest_config(ingest).map_err(ApiError::from)
}

pub(crate) fn load_privacy_policy(root: &config::RootConfig) -> ApiResult<Option<PrivacyPolicy>> {
    let Some(path) = root.analytics.privacy.policy_path.as_deref() else {
        return Ok(None);
    };
    read_privacy_policy(Path::new(path))
        .map(Some)
        .inspect_err(|_| {
            metrics::counter!(PRIVACY_POLICY_LOAD_FAILURES_METRIC).increment(1);
        })
}

fn read_privacy_policy(path: &Path) -> ApiResult<PrivacyPolicy> {
    let policy: PrivacyPolicy = serde_json::from_str(fs::read_to_string(path)?.as_str())?;
    policy.validate()?;
    Ok(policy)
}
