use super::async_worker_threads_for;

#[test]
fn async_runtime_keeps_two_request_workers_on_single_cpu_hosts() {
    assert_eq!(async_worker_threads_for(1), 2);
}

#[test]
fn async_runtime_uses_all_workers_on_larger_hosts() {
    assert_eq!(async_worker_threads_for(8), 8);
}
