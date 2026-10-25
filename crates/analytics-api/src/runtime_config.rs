use std::{fs, path::Path};

use analytics_contract::PrivacyPolicy;

use crate::{cli::ApiCli, constants::PRIVACY_POLICY_LOAD_FAILURES_METRIC, error::ApiResult};

pub(crate) fn resolve_serve_config(
    args: &ApiCli,
) -> Result<config::ResolvedConfig, config::ConfigError> {
    let mut overrides = Vec::new();
    if let Some(port) = args.port.as_ref() {
        overrides.push(format!("http.bind_addr=0.0.0.0:{port}"));
    }
    overrides.extend(args.overrides.iter().cloned());
    config::resolve_config(config::ConfigRequest::api(
        args.config.clone(),
        args.manifest.clone(),
        overrides,
        (&args.backend).into(),
    ))
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
