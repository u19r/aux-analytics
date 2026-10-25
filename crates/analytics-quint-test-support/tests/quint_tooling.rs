#![cfg(unix)]

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

struct TemporaryDirectory {
    path: PathBuf,
}

impl TemporaryDirectory {
    fn new(label: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("aux-quint-tooling-{label}-{nonce}"));
        fs::create_dir_all(&path).expect("temporary directory");
        Self { path }
    }
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn write_executable(path: &Path, source: &str) {
    fs::write(path, source).expect("executable source");
    let mut permissions = fs::metadata(path)
        .expect("executable metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("executable permissions");
}

#[test]
fn given_unrelated_cwd_when_runner_executes_adapter_then_manifest_binds_workspace() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repository root");
    let temporary = TemporaryDirectory::new("adapter-cwd");
    let bin = temporary.path.join("bin");
    fs::create_dir(&bin).expect("fake command directory");
    let log = temporary.path.join("commands.log");
    let target = temporary.path.join("target");

    write_executable(
        &bin.join("cargo"),
        r##"#!/usr/bin/env bash
set -euo pipefail
printf '%s\t%s\n' "$PWD" "$*" >> "$QUINT_TOOLING_LOG"
if [[ "${1:-}" == "run" ]]; then
  printf 'adapter\tanalytics-quint-test-support\n'
fi
"##,
    );
    write_executable(
        &bin.join("quint"),
        "#!/usr/bin/env bash\nset -euo pipefail\n",
    );

    let path = format!("{}:/usr/bin:/bin", bin.display());
    let output = Command::new("/bin/bash")
        .arg(repository.join("scripts/quint-models.sh"))
        .arg("--skip-mutations")
        .current_dir(&temporary.path)
        .env_clear()
        .env("PATH", path)
        .env("AUX_CARGO_TARGET_ACTIVE", "1")
        .env("CARGO_TARGET_DIR", &target)
        .env("QUINT_TOOLING_LOG", &log)
        .output()
        .expect("runner process");
    assert!(
        output.status.success(),
        "runner failed: {}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    let commands = fs::read_to_string(log).expect("command log");
    let mut commands = commands.lines();
    let checker = commands.next().expect("checker command");
    assert!(checker.starts_with(&format!("{}\trun ", repository.display())));
    assert!(checker.contains("--run-plan --combined --skip-mutations"));
    let adapter = commands.next().expect("adapter command");
    let unrelated_cwd = temporary.path.canonicalize().expect("unrelated cwd");
    assert!(
        adapter.starts_with(&format!("{}\ttest ", unrelated_cwd.display())),
        "unexpected adapter command: {adapter}"
    );
    assert!(adapter.contains(&format!(
        "--manifest-path {}/Cargo.toml",
        repository.display()
    )));
    assert!(commands.next().is_none(), "unexpected extra Cargo command");
}

#[test]
fn given_shell_metacharacters_when_bdd_scenario_is_forwarded_then_they_remain_data() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repository root");
    let temporary = TemporaryDirectory::new("bdd-scenario");
    let bin = temporary.path.join("bin");
    fs::create_dir(&bin).expect("fake command directory");
    let log = temporary.path.join("commands.log");
    let marker = temporary.path.join("unexpected-shell-command");
    write_executable(
        &bin.join("cargo"),
        r##"#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$QUINT_BDD_LOG"
printf '1 feature\n'
"##,
    );

    let payload = format!("safe'; touch {}; #", marker.display());
    let inherited_path = std::env::var_os("PATH").unwrap_or_default();
    let path = format!("{}:{}", bin.display(), inherited_path.to_string_lossy());
    let output = Command::new("just")
        .arg("bdd-scenario")
        .arg(&payload)
        .current_dir(&repository)
        .env_clear()
        .env("PATH", path)
        .env("QUINT_BDD_LOG", &log)
        .output()
        .expect("bdd scenario process");

    assert!(
        output.status.success(),
        "bdd scenario failed: {}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !marker.exists(),
        "scenario name was executed as shell source"
    );
    let commands = fs::read_to_string(log).expect("cargo command log");
    assert!(
        commands.contains(&payload),
        "scenario name was not forwarded intact"
    );
}
