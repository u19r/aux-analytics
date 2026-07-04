use std::{
    env, fs, io,
    os::unix::{fs::PermissionsExt, process::CommandExt},
    path::{Path, PathBuf},
    process::Command,
};

use aws_config::BehaviorVersion;
use aws_sdk_ssm::Client as SsmClient;

const ENV_CLUSTER_FILE: &str = "AUX_DUCKLAKE_FDB_CLUSTER_FILE";
const ENV_CLUSTER_FILE_PARAMETER: &str = "AUX_DUCKLAKE_FDB_CLUSTER_FILE_PARAMETER";
const DEFAULT_CLUSTER_FILE_PATH: &str = "/tmp/aux-ducklake/fdb.cluster";

type BootstrapError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[tokio::main]
async fn main() -> Result<(), BootstrapError> {
    let mut args = env::args().skip(1);
    let Some(runtime) = args.next() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: aux-analytics-lambda-bootstrap <runtime> [args...]",
        )
        .into());
    };
    let runtime_args = args.collect::<Vec<_>>();

    let cluster_file = materialize_cluster_file().await?;
    let mut command = Command::new(runtime);
    command.args(runtime_args);
    if let Some(cluster_file) = cluster_file {
        command.env(ENV_CLUSTER_FILE, cluster_file);
    }
    let error = command.exec();
    Err(error.into())
}

async fn materialize_cluster_file() -> Result<Option<PathBuf>, BootstrapError> {
    if env::var_os(ENV_CLUSTER_FILE).is_some() {
        return Ok(None);
    }
    let Some(parameter_name) = optional_env(ENV_CLUSTER_FILE_PARAMETER) else {
        return Ok(None);
    };
    let cluster_file = fetch_ssm_parameter_value(parameter_name.as_str()).await?;
    let path = PathBuf::from(DEFAULT_CLUSTER_FILE_PATH);
    write_cluster_file(path.as_path(), cluster_file.as_str())?;
    Ok(Some(path))
}

async fn fetch_ssm_parameter_value(parameter_name: &str) -> Result<String, BootstrapError> {
    let sdk_config = aws_config::defaults(BehaviorVersion::latest()).load().await;
    let client = SsmClient::new(&sdk_config);
    let output = client
        .get_parameter()
        .name(parameter_name)
        .with_decryption(true)
        .send()
        .await
        .map_err(|source| {
            io::Error::other(format!(
                "failed to read SSM parameter {parameter_name}: {source}"
            ))
        })?;
    output
        .parameter()
        .and_then(|parameter| parameter.value())
        .map(str::to_string)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("SSM parameter {parameter_name} did not contain a non-empty value"),
            )
            .into()
        })
}

fn write_cluster_file(path: &Path, cluster_file: &str) -> Result<(), BootstrapError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, cluster_file)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

fn optional_env(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}
