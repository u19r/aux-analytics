use super::process_duty::ProcessDuty;

#[test]
fn given_lambda_endpoint_when_worker_duty_is_requested_then_startup_rejects() {
    let web = lambda_web_runtime::LambdaWebEnvironment::parse(
        Some("127.0.0.1:9000"),
        Some("127.0.0.1:9001"),
    )
    .unwrap();
    assert!(
        ProcessDuty::WorkersOnly
            .resolve_for_endpoint(web.as_ref())
            .is_err()
    );
    let duty = ProcessDuty::Combined
        .resolve_for_endpoint(web.as_ref())
        .unwrap();
    assert_eq!(duty, ProcessDuty::ApiOnly);
    assert!(!duty.can_run_background_work());
    assert_eq!(
        duty.bind_address("0.0.0.0:8080", web.as_ref())
            .unwrap()
            .to_string(),
        "127.0.0.1:9000"
    );
}

#[test]
fn given_worker_duty_when_a_public_bind_is_configured_then_the_listener_is_loopback() {
    for address in ["0.0.0.0:0", "[::]:0"] {
        let bind = ProcessDuty::WorkersOnly
            .bind_address(address, None)
            .unwrap();
        let listener = std::net::TcpListener::bind(bind).unwrap();
        assert!(listener.local_addr().unwrap().ip().is_loopback());
    }
    assert_eq!(
        ProcessDuty::Combined
            .bind_address("0.0.0.0:8080", None)
            .unwrap()
            .to_string(),
        "0.0.0.0:8080"
    );
}
