use aws_config::{BehaviorVersion, SdkConfig};
use aws_credential_types::Credentials;
use aws_sdk_dynamodb::config::Region;
use config::RemoteCredentialsConfig;

/// Resolve credentials independently of the service's endpoint transport.
pub(super) async fn resolve_aws_config(
    region: Option<&str>,
    endpoint_url: Option<&str>,
    credentials: Option<&RemoteCredentialsConfig>,
) -> SdkConfig {
    let mut loader = aws_config::defaults(BehaviorVersion::latest());
    if let Some(region) = region {
        loader = loader.region(Region::new(region.to_owned()));
    }
    if let Some(endpoint_url) = endpoint_url {
        loader = loader.endpoint_url(endpoint_url);
    }
    if let Some(credentials) = credentials.and_then(|credentials| credentials.r#static.as_ref()) {
        loader = loader.credentials_provider(Credentials::new(
            credentials.access_key.clone(),
            credentials.secret_key.clone(),
            credentials.session_token.clone(),
            None,
            "aux-analytics",
        ));
    }
    let config = loader.load().await;
    if endpoint_url.is_some_and(|endpoint| {
        endpoint
            .split_once("://")
            .is_some_and(|(scheme, _)| scheme.eq_ignore_ascii_case("http"))
    }) {
        return config
            .into_builder()
            .http_client(aws_smithy_http_client::Builder::new().build_http())
            .build();
    }
    config
}
