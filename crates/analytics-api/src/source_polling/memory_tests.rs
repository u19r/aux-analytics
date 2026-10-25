use super::memory::parse_resident_bytes;

#[test]
fn given_linux_status_when_resident_memory_is_read_then_units_and_overflow_are_checked() {
    assert_eq!(
        parse_resident_bytes("Name: worker\nVmRSS: 2048 kB\nThreads: 4\n"),
        Some(2 * 1024 * 1024)
    );
    for status in [
        "Name: worker",
        "VmRSS: missing kB",
        "VmRSS: 1 bytes",
        "VmRSS: 18446744073709551615 kB",
    ] {
        assert_eq!(parse_resident_bytes(status), None, "{status}");
    }
}
