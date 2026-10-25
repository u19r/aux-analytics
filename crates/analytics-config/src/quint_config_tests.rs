use analytics_quint_test_support::run_fresh_traces;
use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    AnalyticsStreamType, BackendOverride, ConfigErrorKind, ConfigRequest, ManifestSource,
    RootConfig, resolve_config,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum ModelCaller {
    Cli,
    Api,
    Lambda,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum ModelStreamKind {
    NoStream,
    StorageStream,
    AuxStorage,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum ModelManifestSource {
    ExplicitArgument,
    ConfigFile,
    ApiEmpty,
    Missing,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum ModelResult {
    NotChecked,
    Accepted,
    AcceptedEmptyApi,
    RejectedMissingManifest,
    RejectedMalformedManifest,
    RejectedSourceStreamType,
    RejectedRetentionBounds,
    RejectedQueryLimit,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct ModelInput {
    caller: ModelCaller,
    manifest_argument_present: bool,
    manifest_argument_non_empty: bool,
    manifest_config_present: bool,
    manifest_config_non_empty: bool,
    source_table_configured: bool,
    source_global_stream: ModelStreamKind,
    source_table_stream: ModelStreamKind,
    retention_enabled: bool,
    retention_query_configured: bool,
    retention_sweep_interval: i64,
    retention_delete_batch: i64,
    query_limit_present: bool,
    query_limit: i64,
    payload_well_formed: bool,
    ingest_endpoint_enabled: bool,
    ingest_processor_enabled: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct ConfigObservedState {
    last_result: ModelResult,
    manifest_source: ModelManifestSource,
    effective_stream: ModelStreamKind,
    ingest_route_enabled: bool,
}

impl State<ConfigDriver> for ConfigObservedState {
    fn from_driver(driver: &ConfigDriver) -> Result<Self> {
        Ok(driver.state)
    }
}

struct ConfigDriver {
    state: ConfigObservedState,
}

impl Default for ConfigDriver {
    fn default() -> Self {
        Self {
            state: ConfigObservedState {
                last_result: ModelResult::NotChecked,
                manifest_source: ModelManifestSource::Missing,
                effective_stream: ModelStreamKind::NoStream,
                ingest_route_enabled: false,
            },
        }
    }
}

impl Driver for ConfigDriver {
    type State = ConfigObservedState;

    fn config() -> quint_connect::Config {
        quint_connect::Config {
            state: &["state"],
            ..quint_connect::Config::default()
        }
    }

    #[allow(
        clippy::too_many_arguments,
        non_snake_case,
        reason = "Generated switch dispatch must match the external Quint CheckConfig and step \
                  signatures"
    )]
    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => *self = Self::default(),
            CheckConfig(
                caller: ModelCaller,
                argumentPresent: bool,
                argumentNonEmpty: bool,
                configPresent: bool,
                configNonEmpty: bool,
                tableConfigured: bool,
                globalStream: ModelStreamKind,
                tableStream: ModelStreamKind,
                retentionEnabled: bool,
                queryConfigured: bool,
                sweepInterval: i64,
                deleteBatch: i64,
                queryLimitPresent: bool,
                queryLimit: i64,
                payloadWellFormed: bool,
                endpointEnabled: bool,
                processorEnabled: bool,
            ) => {
                self.state = resolve_case(ModelInput {
                    caller,
                    manifest_argument_present: argumentPresent,
                    manifest_argument_non_empty: argumentNonEmpty,
                    manifest_config_present: configPresent,
                    manifest_config_non_empty: configNonEmpty,
                    source_table_configured: tableConfigured,
                    source_global_stream: globalStream,
                    source_table_stream: tableStream,
                    retention_enabled: retentionEnabled,
                    retention_query_configured: queryConfigured,
                    retention_sweep_interval: sweepInterval,
                    retention_delete_batch: deleteBatch,
                    query_limit_present: queryLimitPresent,
                    query_limit: queryLimit,
                    payload_well_formed: payloadWellFormed,
                    ingest_endpoint_enabled: endpointEnabled,
                    ingest_processor_enabled: processorEnabled,
                })?;
            },
            step(
                caller: ModelCaller?,
                argumentPresent: bool?,
                argumentNonEmpty: bool?,
                configPresent: bool?,
                configNonEmpty: bool?,
                tableConfigured: bool?,
                globalStream: ModelStreamKind?,
                tableStream: ModelStreamKind?,
                retentionEnabled: bool?,
                queryConfigured: bool?,
                sweepInterval: i64?,
                deleteBatch: i64?,
                queryLimitPresent: bool?,
                queryLimit: i64?,
                payloadWellFormed: bool?,
                endpointEnabled: bool?,
                processorEnabled: bool?,
            ) => {
                if let (
                    Some(caller),
                    Some(argument_present),
                    Some(argument_non_empty),
                    Some(config_present),
                    Some(config_non_empty),
                    Some(table_configured),
                    Some(global_stream),
                    Some(table_stream),
                    Some(retention_enabled),
                    Some(query_configured),
                    Some(retention_sweep_interval),
                    Some(retention_delete_batch),
                    Some(query_limit_present),
                    Some(query_limit),
                    Some(payload_well_formed),
                    Some(ingest_endpoint_enabled),
                    Some(ingest_processor_enabled),
                ) = (
                    caller,
                    argumentPresent,
                    argumentNonEmpty,
                    configPresent,
                    configNonEmpty,
                    tableConfigured,
                    globalStream,
                    tableStream,
                    retentionEnabled,
                    queryConfigured,
                    sweepInterval,
                    deleteBatch,
                    queryLimitPresent,
                    queryLimit,
                    payloadWellFormed,
                    endpointEnabled,
                    processorEnabled,
                ) {
                    self.state = resolve_case(ModelInput {
                        caller,
                        manifest_argument_present: argument_present,
                        manifest_argument_non_empty: argument_non_empty,
                        manifest_config_present: config_present,
                        manifest_config_non_empty: config_non_empty,
                        source_table_configured: table_configured,
                        source_global_stream: global_stream,
                        source_table_stream: table_stream,
                        retention_enabled,
                        retention_query_configured: query_configured,
                        retention_sweep_interval,
                        retention_delete_batch,
                        query_limit_present,
                        query_limit,
                        payload_well_formed,
                        ingest_endpoint_enabled,
                        ingest_processor_enabled,
                    })?;
                }
            },
        })
    }
}

#[test]
fn given_runtime_boundary_when_quint_config_api_then_matches_runtime_boundary() {
    let seeds: Vec<_> = (0..50)
        .map(|offset| format!("0x{:x}", 0xa312_u64 + offset))
        .collect();
    let factory_invocations = std::cell::Cell::new(0);
    let receipt = run_fresh_traces(
        "quint_config_api_matches_runtime_boundary",
        "../../specs/config_api.qnt",
        12,
        seeds.clone(),
        || {
            factory_invocations.set(factory_invocations.get() + 1);
            ConfigDriver::default()
        },
    )
    .expect("configuration traces");
    assert_eq!(receipt.expected_trace_count, seeds.len());
    assert_eq!(receipt.factory_invocations, seeds.len());
    assert_eq!(receipt.seeds, seeds);
    assert_eq!(factory_invocations.get(), 50);
}

#[test]
fn given_unreferenced_malformed_manifest_when_api_config_is_resolved_then_manifest_is_empty() {
    let fixtures = ConfigFixtures::for_input(ModelInput {
        caller: ModelCaller::Api,
        manifest_argument_present: false,
        manifest_argument_non_empty: false,
        manifest_config_present: false,
        manifest_config_non_empty: false,
        source_table_configured: false,
        source_global_stream: ModelStreamKind::NoStream,
        source_table_stream: ModelStreamKind::NoStream,
        retention_enabled: false,
        retention_query_configured: false,
        retention_sweep_interval: 1,
        retention_delete_batch: 1,
        query_limit_present: false,
        query_limit: 1,
        payload_well_formed: false,
        ingest_endpoint_enabled: true,
        ingest_processor_enabled: true,
    })
    .expect("API fixture");
    let resolved = resolve_config(fixtures.request)
        .expect("an unreferenced malformed manifest must remain API-empty");

    assert_eq!(resolved.manifest.source, ManifestSource::ApiEmpty);
}

fn resolve_case(input: ModelInput) -> Result<ConfigObservedState> {
    let fixtures = ConfigFixtures::for_input(input)?;
    let result = resolve_config(fixtures.request);
    match result {
        Ok(resolved) => Ok(ConfigObservedState {
            last_result: if resolved.manifest.source == ManifestSource::ApiEmpty {
                ModelResult::AcceptedEmptyApi
            } else {
                ModelResult::Accepted
            },
            manifest_source: model_manifest_source_from_production(resolved.manifest.source),
            effective_stream: model_effective_stream(&resolved.root),
            ingest_route_enabled: resolved.routes.ingest_enabled,
        }),
        Err(error) => Ok(ConfigObservedState {
            last_result: model_result_for_error(error.kind()),
            manifest_source: ModelManifestSource::Missing,
            effective_stream: ModelStreamKind::NoStream,
            ingest_route_enabled: false,
        }),
    }
}

struct ConfigFixtures {
    request: ConfigRequest,
    _config_file: tempfile::NamedTempFile,
    _manifest_file: tempfile::NamedTempFile,
}

impl ConfigFixtures {
    fn for_input(input: ModelInput) -> Result<Self> {
        let config_manifest_path =
            input
                .manifest_config_present
                .then_some(if input.manifest_config_non_empty {
                    "__VALID_MANIFEST__"
                } else {
                    ""
                });
        let config_root = config_json(input, config_manifest_path)?;
        let config_file = tempfile::NamedTempFile::new()?;
        let manifest_file = tempfile::NamedTempFile::new()?;
        let manifest_contents = if input.payload_well_formed {
            serde_json::to_vec(&analytics_contract::AnalyticsManifest::new(Vec::new()))?
        } else {
            b"not-json".to_vec()
        };
        std::fs::write(manifest_file.path(), manifest_contents)?;
        let manifest_path = manifest_file.path().to_string_lossy().into_owned();
        let config_contents =
            replace_manifest_placeholder(serde_json::to_vec(&config_root)?, &manifest_path);
        std::fs::write(config_file.path(), config_contents)?;
        let config_path = config_file.path().to_path_buf();
        let manifest_argument = if input.manifest_argument_present {
            if input.manifest_argument_non_empty {
                Some(manifest_path.clone())
            } else {
                Some(String::new())
            }
        } else {
            None
        };
        let request = match input.caller {
            ModelCaller::Cli => crate::ConfigRequest::cli(
                Some(config_path),
                manifest_argument,
                Vec::new(),
                backend(),
            ),
            ModelCaller::Api => crate::ConfigRequest::api(
                Some(config_path),
                manifest_argument,
                Vec::new(),
                backend(),
            ),
            ModelCaller::Lambda => {
                let config_path = config_file.path().to_string_lossy().into_owned();
                let manifest_argument = manifest_argument.clone();
                let environment = crate::LambdaEnvironment::from_lookup(|key| match key {
                    "AUX_ANALYTICS_CONFIG" => Some(config_path.clone()),
                    "AUX_ANALYTICS_MANIFEST" => manifest_argument.clone(),
                    "AUX_ANALYTICS_DUCKDB" => Some(":memory:".to_string()),
                    _ => None,
                });
                crate::ConfigRequest::lambda(environment)
            }
        };
        Ok(Self {
            request,
            _config_file: config_file,
            _manifest_file: manifest_file,
        })
    }
}

fn config_json(input: ModelInput, manifest_path: Option<&str>) -> Result<Value> {
    let mut root = serde_json::to_value(RootConfig::default())?;
    let analytics = root
        .get_mut("analytics")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| anyhow::anyhow!("default config has no analytics object"))?;
    if let Some(path) = manifest_path {
        analytics.insert("manifest_path".to_string(), json!(path));
    } else {
        analytics.remove("manifest_path");
    }
    analytics.insert(
        "http".to_string(),
        json!({"ingest_endpoint_enabled": input.ingest_endpoint_enabled}),
    );
    analytics.insert(
        "ingest".to_string(),
        json!({"processor_enabled": input.ingest_processor_enabled}),
    );
    let mut source = json!({"tables": []});
    if input.source_global_stream != ModelStreamKind::NoStream {
        source["stream_type"] = json!(stream_name(input.source_global_stream));
    }
    if input.source_table_configured {
        let mut table = json!({"table_name": "users"});
        if input.source_table_stream != ModelStreamKind::NoStream {
            table["stream_type"] = json!(stream_name(input.source_table_stream));
        }
        source["tables"] = json!([table]);
    }
    analytics.insert("source".to_string(), source);
    let mut retention = json!({
        "enabled": input.retention_enabled,
        "sweep_interval_ms": input.retention_sweep_interval,
        "delete_batch_size": input.retention_delete_batch,
        "tables": [],
    });
    if input.retention_enabled && input.retention_query_configured {
        let mut request = json!({
            "query_table": {
                "table_name": "retention",
                "key_condition_expression": "tenant_id = :tenant_id",
                "expression_attribute_values": {":tenant_id": {"S": "${tenant_id}"}},
            }
        });
        if input.query_limit_present {
            request["query_table"]["limit"] = json!(input.query_limit);
        }
        retention["tables"] = json!([{
            "analytics_table_name": "users",
            "default_period_ms": 1,
            "timestamp": {"kind": "ingested_at"},
            "tenant_policy": {
                "source": "dynamodb",
                "request": request,
                "duration_selector": {"attribute_path": "retention_ms"},
            },
        }]);
    }
    analytics.insert("retention".to_string(), retention);
    Ok(root)
}

fn replace_manifest_placeholder(mut bytes: Vec<u8>, manifest_path: &str) -> Vec<u8> {
    let placeholder = br#"__VALID_MANIFEST__"#;
    let replacement = manifest_path.as_bytes();
    if let Some(offset) = bytes
        .windows(placeholder.len())
        .position(|window| window == placeholder)
    {
        let end = offset + placeholder.len();
        bytes.splice(offset..end, replacement.iter().copied());
    }
    bytes
}

fn backend() -> BackendOverride {
    BackendOverride {
        duckdb: Some(":memory:".to_string()),
        ..BackendOverride::default()
    }
}

fn stream_name(stream: ModelStreamKind) -> &'static str {
    match stream {
        ModelStreamKind::NoStream => unreachable!("no stream is omitted from config"),
        ModelStreamKind::StorageStream => "storage_stream",
        ModelStreamKind::AuxStorage => "aux_storage",
    }
}

fn model_manifest_source_from_production(source: ManifestSource) -> ModelManifestSource {
    match source {
        ManifestSource::ExplicitArgument => ModelManifestSource::ExplicitArgument,
        ManifestSource::ConfigFile => ModelManifestSource::ConfigFile,
        ManifestSource::ApiEmpty => ModelManifestSource::ApiEmpty,
        ManifestSource::Missing => ModelManifestSource::Missing,
    }
}

fn model_effective_stream(root: &RootConfig) -> ModelStreamKind {
    root.analytics
        .source
        .tables
        .first()
        .and_then(|table| table.stream_type)
        .or(root.analytics.source.stream_type)
        .map(model_stream_from_production)
        .unwrap_or(ModelStreamKind::NoStream)
}

fn model_stream_from_production(stream: AnalyticsStreamType) -> ModelStreamKind {
    match stream {
        AnalyticsStreamType::StorageStream => ModelStreamKind::StorageStream,
        AnalyticsStreamType::AuxStorage => ModelStreamKind::AuxStorage,
    }
}

fn model_result_for_error(kind: ConfigErrorKind) -> ModelResult {
    match kind {
        ConfigErrorKind::MissingManifestPath => ModelResult::RejectedMissingManifest,
        ConfigErrorKind::InvalidManifestPath | ConfigErrorKind::Manifest => {
            ModelResult::RejectedMalformedManifest
        }
        ConfigErrorKind::InvalidSourceTableStreamType => ModelResult::RejectedSourceStreamType,
        ConfigErrorKind::InvalidRetentionConfig => ModelResult::RejectedRetentionBounds,
        // A missing query_table.limit is rejected while RootConfig is
        // deserialized, before resolve_config reaches semantic retention
        // validation.
        ConfigErrorKind::Json => ModelResult::RejectedQueryLimit,
        other => panic!("unexpected config boundary error: {other:?}"),
    }
}
