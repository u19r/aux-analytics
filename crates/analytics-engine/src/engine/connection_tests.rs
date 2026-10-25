use std::{
    io::{Read, Write},
    net::TcpListener,
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use config::{
    AnalyticsObjectStorageConfig, CatalogType, RemoteCredentialsConfig,
    RemoteStaticCredentialsConfig, StorageBackend,
};

use super::AnalyticsEngine;

#[test]
fn given_sqlite_catalog_when_readers_overlap_schema_writes_then_metadata_remains_available() {
    let directory = tempfile::tempdir().expect("catalog directory");
    let backend = build_sqlite_concurrency_backend(directory.path());
    let owner = AnalyticsEngine::connect(&backend).expect("initialize catalog");
    owner
        .connection()
        .execute_batch(
            "CREATE TABLE connection_probe(value INTEGER); INSERT INTO connection_probe VALUES \
             (42)",
        )
        .expect("create committed source data");
    let barrier = std::sync::Barrier::new(8);
    thread::scope(|scope| {
        let writer = scope.spawn(|| write_parallel_catalog_schema(&backend, &barrier));
        let readers: Vec<_> = (0..7)
            .map(|_| {
                scope.spawn(|| {
                    read_parallel_catalog_rows(&backend, &barrier);
                })
            })
            .collect();
        writer.join().expect("parallel metadata writer");
        for reader in readers {
            reader.join().expect("parallel metadata reader");
        }
    });
}

fn build_sqlite_concurrency_backend(directory: &std::path::Path) -> StorageBackend {
    StorageBackend::DuckLake {
        catalog: CatalogType::Sqlite,
        catalog_path: directory
            .join("catalog.sqlite")
            .to_string_lossy()
            .into_owned(),
        data_path: directory.join("data").to_string_lossy().into_owned(),
        object_storage: None,
        catalog_settings: config::DuckLakeCatalogSettings {
            duckdb_threads: Some(1),
            ..Default::default()
        },
    }
}

fn write_parallel_catalog_schema(backend: &StorageBackend, barrier: &std::sync::Barrier) {
    barrier.wait();
    let writer = AnalyticsEngine::connect(backend).expect("parallel catalog writer");
    for iteration in 0..32 {
        writer
            .connection()
            .execute_batch(&format!(
                "CREATE TABLE probe_{iteration}(value INTEGER); DROP TABLE probe_{iteration}"
            ))
            .expect("commit metadata write beside readers");
    }
}

fn read_parallel_catalog_rows(backend: &StorageBackend, barrier: &std::sync::Barrier) {
    barrier.wait();
    let reader = AnalyticsEngine::connect(backend).expect("parallel catalog reader");
    for _ in 0..32 {
        assert_eq!(
            reader
                .query_unscoped_sql_json("SELECT value FROM connection_probe")
                .expect("read committed metadata"),
            [serde_json::json!({"value":42})]
        );
    }
}

#[test]
fn given_bucketless_static_environment_and_expiring_configuration_when_native_connections_run_then_only_expiring_credentials_are_refetched()
 {
    for mode in ["bucketless", "static", "environment", "refresh"] {
        let listener = TcpListener::bind("127.0.0.1:0").expect("credential endpoint");
        let endpoint = format!(
            "http://{}",
            listener.local_addr().expect("endpoint address")
        );
        listener
            .set_nonblocking(true)
            .expect("nonblocking listener");
        let stop = Arc::new(AtomicBool::new(false));
        let server_stop = Arc::clone(&stop);
        let server = thread::spawn(move || serve_credentials(&listener, &server_stop));
        let mut command = Command::new(std::env::current_exe().expect("test executable"));
        command.args([
            "--exact",
            "engine::connection_tests::given_isolated_credentials_when_native_connections_refresh_then_settings_and_failure_recovery_are_preserved",
            "--ignored",
            "--nocapture",
        ]);
        for (name, _) in std::env::vars_os() {
            if name.to_string_lossy().starts_with("AWS_") {
                command.env_remove(name);
            }
        }
        command
            .env("ANALYTICS_CONNECTION_CREDENTIAL_MODE", mode)
            .env("AWS_CONTAINER_CREDENTIALS_FULL_URI", endpoint)
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .env("AWS_CONFIG_FILE", "/dev/null")
            .env("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
            .env(
                "AWS_PROFILE",
                "analytics-nonexistent-credential-test-profile",
            );
        if mode != "refresh" {
            command.env("AWS_ACCESS_KEY_ID", "environment-key");
        }
        if mode == "environment" {
            command.env("AWS_SECRET_ACCESS_KEY", "environment-secret");
        }
        let result = command.output();
        stop.store(true, Ordering::Release);
        let requests = server.join().expect("credential endpoint joined");
        let output = result.expect("credential worker launch");
        assert!(
            output.status.success(),
            "worker {mode}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
        assert_eq!(
            requests,
            if mode == "refresh" { 5 } else { 0 },
            "provider calls for {mode}"
        );
    }
}

#[test]
#[ignore = "requires the isolated parent credential endpoint and environment"]
fn given_isolated_credentials_when_native_connections_refresh_then_settings_and_failure_recovery_are_preserved()
 {
    let mode = std::env::var("ANALYTICS_CONNECTION_CREDENTIAL_MODE").expect("parent mode");
    assert!(["bucketless", "static", "environment", "refresh"].contains(&mode.as_str()));
    let directory = tempfile::tempdir().expect("native catalog directory");
    let credentials = (mode == "static").then(|| RemoteCredentialsConfig {
        r#static: Some(RemoteStaticCredentialsConfig {
            access_key: "static-key".to_owned(),
            secret_key: "static-secret".to_owned(),
            session_token: Some("static-session".to_owned()),
        }),
        instance_keys: None,
    });
    let backend = StorageBackend::DuckLake {
        catalog: if std::env::var_os("AUX_ANALYTICS_DUCKLAKE_FDB_LIVE").is_some() {
            CatalogType::AuxCatalog
        } else {
            CatalogType::Sqlite
        },
        catalog_path: directory
            .path()
            .join("catalog.sqlite")
            .to_string_lossy()
            .into_owned(),
        data_path: directory.path().join("data").to_string_lossy().into_owned(),
        object_storage: Some(Box::new(AnalyticsObjectStorageConfig {
            bucket: (mode != "bucketless").then(|| "fixture-bucket".to_owned()),
            endpoint_url: Some("http://127.0.0.1:9123".to_owned()),
            region: Some("fixture-region".to_owned()),
            credentials,
            ..Default::default()
        })),
        catalog_settings: Default::default(),
    };
    let engine = AnalyticsEngine::connect(&backend).expect("configured native catalog");
    assert_native_secret_and_selected_catalog(
        &engine,
        match mode.as_str() {
            "bucketless" => None,
            "static" => Some("static-key"),
            "environment" => Some("environment-key"),
            _ => Some("initial-key"),
        },
    );
    if mode == "refresh" {
        assert_literal_query(&engine);
        assert_native_secret_and_selected_catalog(&engine, Some("replacement-key"));
        assert!(
            engine
                .query_unscoped_sql_json("SELECT 42 AS answer")
                .is_err()
        );
        assert_native_secret_and_selected_catalog(&engine, Some("replacement-key"));
        assert_literal_query(&engine);
        assert_native_secret_and_selected_catalog(&engine, Some("recovered-key"));
    }
    for _ in 0..3 {
        assert_literal_query(&engine);
    }
    drop(engine);
    let reopened = AnalyticsEngine::connect(&backend).expect("repeated dedicated connection");
    assert_literal_query(&reopened);
    assert_native_secret_and_selected_catalog(
        &reopened,
        match mode.as_str() {
            "bucketless" => None,
            "static" => Some("static-key"),
            "environment" => Some("environment-key"),
            _ => Some("recovered-key"),
        },
    );
}

fn assert_literal_query(engine: &AnalyticsEngine) {
    assert_eq!(
        engine
            .query_unscoped_sql_json("SELECT 42 AS answer")
            .expect("native query"),
        [serde_json::json!({"answer": 42})]
    );
}

fn assert_native_secret_and_selected_catalog(engine: &AnalyticsEngine, key: Option<&str>) {
    if let Some(key) = key {
        let secret: String = engine
            .connection()
            .query_row(
                "SELECT secret_string FROM duckdb_secrets() WHERE name = \
                 'aux_analytics_object_store'",
                [],
                |row| row.get(0),
            )
            .expect("native configured secret");
        for expected in [key, "fixture-region", "127.0.0.1:9123"] {
            assert!(
                secret.contains(expected),
                "native secret preserves expected setting"
            );
        }
    } else {
        let count: u64 = engine
            .connection()
            .query_row(
                "SELECT count(*) FROM duckdb_secrets() WHERE name = 'aux_analytics_object_store'",
                [],
                |row| row.get(0),
            )
            .expect("native secret absence");
        assert_eq!(count, 0, "bucketless storage needs no object-store secret");
    }
    let catalog: String = engine
        .connection()
        .query_row("SELECT current_database()", [], |row| row.get(0))
        .expect("selected catalog");
    assert_eq!(catalog, "dlake");
}

fn serve_credentials(listener: &TcpListener, stop: &AtomicBool) -> usize {
    let mut requests = 0;
    while !stop.load(Ordering::Acquire) {
        let (mut stream, _) = match listener.accept() {
            Ok(connection) => connection,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5));
                continue;
            }
            Err(error) => panic!("credential endpoint: {error}"),
        };
        stream
            .set_nonblocking(false)
            .expect("blocking accepted credential connection");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("read deadline");
        if !read_credential_request(&mut stream).expect("credential request") {
            continue;
        }
        requests += 1;
        let (status, body) = match requests {
            1 => (
                "200 OK",
                serialize_credential_response("initial-key", "2000-01-01T00:00:00Z"),
            ),
            2 => (
                "200 OK",
                serialize_credential_response("replacement-key", "2000-01-01T00:00:00Z"),
            ),
            3 => ("503 Service Unavailable", "{}".to_owned()),
            _ => (
                "200 OK",
                serialize_credential_response("recovered-key", "2099-01-01T00:00:00Z"),
            ),
        };
        write!(
            stream,
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: \
             {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .expect("credential response");
    }
    requests
}

fn read_credential_request(reader: &mut impl Read) -> std::io::Result<bool> {
    use std::io::ErrorKind;

    let mut request = [0; 4096];
    let mut received = 0;
    while !request[..received]
        .windows(4)
        .any(|bytes| bytes == b"\r\n\r\n")
    {
        if received == request.len() {
            return Err(ErrorKind::InvalidData.into());
        }
        match reader.read(&mut request[received..]) {
            Ok(0) if received == 0 => return Ok(false),
            Ok(0) => {
                return Err(ErrorKind::UnexpectedEof.into());
            }
            Ok(count) => received += count,
            Err(error)
                if received == 0
                    && matches!(
                        error.kind(),
                        ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::ConnectionReset
                    ) =>
            {
                return Ok(false);
            }
            Err(error) => return Err(error),
        }
    }
    if !request[..received].starts_with(b"GET / HTTP/1.1\r\n") {
        return Err(ErrorKind::InvalidData.into());
    }
    Ok(true)
}

#[test]
fn given_empty_fragmented_and_invalid_credential_headers_when_read_then_only_complete_get_requests_are_accepted()
 {
    use std::io::ErrorKind;

    assert!(!read_credential_request(&mut std::io::empty()).expect("unused connection"));
    assert!(
        !read_credential_request(&mut TimedOutHeaderReader { bytes: &[] })
            .expect("unused timed out connection")
    );
    let mut fragmented = b"GET / HTTP/1.1\r\n"
        .as_slice()
        .chain(b"Host: local\r\n\r\n".as_slice());
    assert!(read_credential_request(&mut fragmented).expect("complete fragmented GET"));
    for (bytes, kind) in [
        (b"GET / HTTP/1.1\r\n".as_slice(), ErrorKind::UnexpectedEof),
        (
            b"POST / HTTP/1.1\r\n\r\n".as_slice(),
            ErrorKind::InvalidData,
        ),
        ([b'x'; 4096].as_slice(), ErrorKind::InvalidData),
    ] {
        let mut reader = bytes;
        assert_eq!(
            read_credential_request(&mut reader)
                .expect_err("invalid headers")
                .kind(),
            kind
        );
    }
    assert_eq!(
        read_credential_request(&mut TimedOutHeaderReader { bytes: b"GET /" })
            .expect_err("partial timed out request must fail")
            .kind(),
        ErrorKind::TimedOut
    );
}

struct TimedOutHeaderReader<'a> {
    bytes: &'a [u8],
}

impl Read for TimedOutHeaderReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if self.bytes.is_empty() {
            return Err(std::io::ErrorKind::TimedOut.into());
        }
        self.bytes.read(buffer)
    }
}

fn serialize_credential_response(key: &str, expiration: &str) -> String {
    serde_json::json!({
        "AccessKeyId": key,
        "SecretAccessKey": "fixture-secret",
        "Token": "fixture-session",
        "Expiration": expiration,
    })
    .to_string()
}

#[test]
#[ignore = "manual paired native connection allocation measurement"]
fn given_equal_native_backends_when_repeated_connections_borrow_or_copy_then_complete_queries_and_cost_are_recorded()
 {
    let directory = tempfile::tempdir().expect("native catalog directory");
    for width in [64, 8192] {
        let backend = StorageBackend::DuckLake {
            catalog: CatalogType::Sqlite,
            catalog_path: directory
                .path()
                .join(format!("catalog-{width}.sqlite"))
                .to_string_lossy()
                .into_owned(),
            data_path: directory
                .path()
                .join(format!("data-{width}"))
                .to_string_lossy()
                .into_owned(),
            object_storage: Some(Box::new(AnalyticsObjectStorageConfig {
                bucket: Some("fixture-bucket".to_owned()),
                path: Some("x".repeat(width)),
                region: Some("fixture-region".to_owned()),
                credentials: Some(RemoteCredentialsConfig {
                    r#static: Some(RemoteStaticCredentialsConfig {
                        access_key: "fixture-key".to_owned(),
                        secret_key: "fixture-secret".to_owned(),
                        session_token: None,
                    }),
                    instance_keys: None,
                }),
                ..Default::default()
            })),
            catalog_settings: Default::default(),
        };
        drop(AnalyticsEngine::connect(&backend).expect("warm native catalog"));
        for trial in 0..3 {
            let modes = if trial % 2 == 0 {
                ["copied", "borrowed"]
            } else {
                ["borrowed", "copied"]
            };
            for mode in modes {
                let guard = alloc_counter::AllocationGuard::start(
                    "analytics-engine",
                    "native-connection",
                    file!(),
                    line!(),
                    None,
                );
                let started = std::time::Instant::now();
                for _ in 0..8 {
                    // Reconstruct only the former unchanged-backend copy at the
                    // same native boundary.
                    let copied = (mode == "copied").then(|| backend.clone());
                    let engine = AnalyticsEngine::connect(copied.as_ref().unwrap_or(&backend))
                        .expect("native connection");
                    assert_literal_query(&engine);
                    let catalog: String = engine
                        .connection()
                        .query_row("SELECT current_database()", [], |row| row.get(0))
                        .expect("selected catalog");
                    assert_eq!(catalog, "dlake");
                }
                let elapsed = started.elapsed();
                let report = guard.finish();
                println!(
                    "CONNECTION_SAMPLE {}",
                    serde_json::json!({
                        "width": width, "trial": trial, "mode": mode,
                        "connections": 8, "elapsed_ns": elapsed.as_nanos(),
                        "allocation_count": report.allocation_count,
                        "allocation_bytes": report.allocated_bytes,
                        "retained_refresh_config_bytes": 0,
                    })
                );
            }
        }
    }
}

#[test]
#[ignore = "manual measurement of reconstructed default-chain refresh ownership"]
fn given_default_chain_refresh_fields_when_snapshots_are_reconstructed_then_owned_rust_buffers_are_measured()
 {
    for width in [64, 8192] {
        let config = AnalyticsObjectStorageConfig {
            bucket: Some("fixture-bucket".to_owned()),
            path: Some("x".repeat(width)),
            endpoint_url: Some("http://127.0.0.1:9123".to_owned()),
            region: Some("fixture-region".to_owned()),
            credentials: Some(RemoteCredentialsConfig {
                r#static: None,
                instance_keys: Some(true),
            }),
            ..Default::default()
        };
        let resolved = aws_credentials::AwsResolvedCredentials {
            credentials: aws_credentials::AwsStaticCredentials {
                access_key_id: "fixture-key".to_owned(),
                secret_access_key: "fixture-secret".to_owned(),
                session_token: Some("fixture-session".to_owned()),
            },
            expires_after: None,
            refresh_after: None,
        };
        let full_bytes = measure_refresh_snapshot(&config, &resolved, width);
        let narrow_bytes = measure_secret_refresh_snapshot(&config, &resolved, width);
        assert!(narrow_bytes < full_bytes);
    }
}

fn measure_refresh_snapshot(
    config: &AnalyticsObjectStorageConfig,
    resolved: &aws_credentials::AwsResolvedCredentials,
    width: usize,
) -> u64 {
    let guard = alloc_counter::AllocationGuard::start(
        "analytics-engine",
        "refresh-snapshot",
        file!(),
        line!(),
        None,
    );
    // Reconstruct the two owned fields of DuckLakeObjectStorageCredentials. All
    // cloned String buffers remain alive when the counter is stopped.
    let snapshot = std::hint::black_box((config.clone(), resolved.clone()));
    let report = guard.finish();
    let owned_buffer_bytes = snapshot.0.scheme.capacity()
        + [
            &snapshot.0.bucket,
            &snapshot.0.path,
            &snapshot.0.endpoint_url,
            &snapshot.0.region,
        ]
        .into_iter()
        .flatten()
        .map(String::capacity)
        .sum::<usize>()
        + snapshot.1.credentials.access_key_id.capacity()
        + snapshot.1.credentials.secret_access_key.capacity()
        + snapshot
            .1
            .credentials
            .session_token
            .as_ref()
            .unwrap()
            .capacity();
    assert_eq!(report.allocation_count, 8);
    assert_eq!(report.allocated_bytes, owned_buffer_bytes as u64);
    println!(
        "REFRESH_SNAPSHOT_SAMPLE {}",
        serde_json::json!({
            "width": width,
            "variant": "full_config_reference",
            "retained_config_and_credential_requested_bytes": owned_buffer_bytes,
            "owner_header_bytes": std::mem::size_of::<(AnalyticsObjectStorageConfig, aws_credentials::AwsResolvedCredentials)>(),
            "allocation_count": report.allocation_count,
            "scope": "reconstructed Rust snapshot; excludes allocator padding, providers and native buffers",
        })
    );
    report.allocated_bytes
}

fn measure_secret_refresh_snapshot(
    config: &AnalyticsObjectStorageConfig,
    resolved: &aws_credentials::AwsResolvedCredentials,
    width: usize,
) -> u64 {
    let guard = alloc_counter::AllocationGuard::start(
        "analytics-engine",
        "secret-refresh-snapshot",
        file!(),
        line!(),
        None,
    );
    let snapshot = std::hint::black_box((
        crate::sql::ObjectStorageSecret::from_config(config)
            .expect("bucket is configured")
            .into_owned(),
        resolved.clone(),
    ));
    let report = guard.finish();
    // The five owned buffers all remain alive: region, endpoint and resolved
    // key, secret and session token. No intermediate owned buffer is created.
    assert_eq!(report.allocation_count, 5);
    assert_eq!(report.allocated_bytes, 75);
    std::hint::black_box(&snapshot);
    println!(
        "REFRESH_SNAPSHOT_SAMPLE {}",
        serde_json::json!({
            "width": width,
            "variant": "secret_settings",
            "retained_config_and_credential_requested_bytes": report.allocated_bytes,
            "owner_header_bytes": std::mem::size_of::<super::connection::DuckLakeObjectStorageCredentials>(),
            "allocation_count": report.allocation_count,
            "scope": "actual owned secret settings and resolved credentials; excludes allocator padding, providers and native buffers",
        })
    );
    report.allocated_bytes
}
