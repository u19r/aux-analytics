use crate::run::async_worker_threads_for;

#[test]
fn given_single_cpu_host_when_runtime_composed_then_two_request_workers_are_available() {
    assert_eq!(async_worker_threads_for(1), 2);
}

#[test]
fn given_multiple_cpu_host_when_runtime_composed_then_all_workers_are_used() {
    assert_eq!(async_worker_threads_for(8), 8);
}
