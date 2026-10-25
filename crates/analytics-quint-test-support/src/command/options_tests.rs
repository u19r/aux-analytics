use super::options::CommandOptions;

#[test]
fn given_value_flags_without_arguments_when_parsed_then_exact_usage_error_is_returned() {
    for (flag, expected) in [
        ("--repo-root", "--repo-root requires a path"),
        ("--mutation", "--mutation requires an ID"),
        ("--family", "--family requires a value"),
        ("--mode", "--mode requires a value"),
    ] {
        let error = CommandOptions::parse([flag.to_owned()])
            .err()
            .expect("usage error");
        assert_eq!(error.exit_code, 64);
        assert_eq!(error.message, format!("quint contract error: {expected}\n"));
    }
}

#[test]
fn given_unknown_flag_when_parsed_then_diagnostic_preserves_flag() {
    let error = CommandOptions::parse(["--unknown".to_owned()])
        .err()
        .expect("usage error");
    assert_eq!(error.exit_code, 64);
    assert_eq!(
        error.message,
        "quint contract error: unknown argument --unknown\n"
    );
}
