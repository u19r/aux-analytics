use storage_types::{SystemStreamCheckpointToken, SystemStreamScope};

use crate::{
    SourcePoller, SourceTablePlan, SystemStreamResume,
    constants::{
        DESCRIBE, EMPTY_PAGE, EMPTY_SELECTED_PAGE, JOB_PAGE, MALFORMED_JOB_PAGE, SELECTED_PAGE,
    },
    poller::test_support::serve_sequence_with_capture,
};

#[tokio::test]
async fn given_committed_destination_frontier_when_local_progress_reloads_then_polling_resumes_there()
 {
    let (url, requests) =
        serve_sequence_with_capture(vec![DESCRIBE, SELECTED_PAGE, EMPTY_SELECTED_PAGE]).await;
    let initial = checkpoint("v1.00000000000000000000000000000000");
    let mut poller = build_poller(&url, Some(initial.clone())).await;
    let batch = poller.poll_once().await.expect("selected page");
    let stream = batch.system_stream.expect("system stream batch");
    assert_eq!(stream.previous_checkpoint, Some(initial));
    poller
        .reload_system_resume(Some(SystemStreamResume::new(
            stream.source_scope,
            stream.checkpoint.clone(),
        )))
        .expect("reload destination frontier without a local commit");
    assert!(
        poller
            .poll_once()
            .await
            .expect("idle page")
            .records
            .is_empty()
    );
    assert!(requests.lock().expect("requests")[2].contains(stream.checkpoint.as_str()));
}

#[tokio::test]
async fn given_another_source_scope_when_local_progress_reloads_then_both_cursors_are_preserved() {
    let (url, requests) = serve_sequence_with_capture(vec![DESCRIBE, SELECTED_PAGE]).await;
    let initial = checkpoint("v1.00000000000000000000000000000000");
    let mut poller = build_poller(&url, Some(initial.clone())).await;
    let resume = SystemStreamResume::new(
        SystemStreamScope::try_new("another-system", 1).expect("scope"),
        checkpoint("v1.22222222222222222222222222222222"),
    );
    let error = poller
        .reload_system_resume(Some(resume))
        .expect_err("scope mismatch");
    assert_eq!(error.category(), "invalid_source_batch_scope");
    let batch = poller
        .poll_once()
        .await
        .expect("selected page after rejection");
    assert_eq!(
        batch
            .system_stream
            .expect("system stream batch")
            .previous_checkpoint,
        Some(initial.clone())
    );
    assert!(requests.lock().expect("requests")[1].contains(initial.as_str()));
}

#[tokio::test]
async fn given_job_only_page_when_polled_then_no_job_effect_is_created_and_selected_predecessor_stays_durable()
 {
    let (url, requests) = serve_sequence_with_capture(vec![
        DESCRIBE,
        JOB_PAGE,
        SELECTED_PAGE,
        SELECTED_PAGE,
        EMPTY_SELECTED_PAGE,
    ])
    .await;
    let initial = checkpoint("v1.00000000000000000000000000000000");
    let mut poller = build_poller(&url, Some(initial.clone())).await;

    let ignored = poller.poll_once().await.expect("job-only page");
    assert!(ignored.records.is_empty());
    assert!(
        ignored.system_stream.is_none(),
        "job-only pages must not enqueue jobs that write more job records"
    );
    assert_eq!(ignored.source_record_count, 1);
    poller.commit_batch(&ignored);

    let selected = poller.poll_once().await.expect("selected page");
    let stream = selected.system_stream.as_ref().expect("selected effect");
    assert_eq!(stream.previous_checkpoint.as_ref(), Some(&initial));
    assert_eq!(
        stream.checkpoint,
        checkpoint("v1.22222222222222222222222222222222")
    );
    assert_eq!(selected.records.len(), 1);

    let retry = poller
        .poll_once()
        .await
        .expect("retry uncommitted selected page");
    assert_eq!(retry.system_stream, selected.system_stream);
    poller.commit_batch(&selected);
    let empty = poller.poll_once().await.expect("idle page after commit");
    assert!(empty.system_stream.is_none());
    let requests = requests.lock().expect("requests");
    assert!(requests[1].contains(initial.as_str()));
    assert!(requests[2].contains("v1.11111111111111111111111111111111"));
    assert!(requests[3].contains("v1.11111111111111111111111111111111"));
    assert!(requests[4].contains("v1.22222222222222222222222222222222"));
}

#[tokio::test]
async fn given_unrouted_transient_progress_when_restarted_then_scan_restarts_at_durable_frontier() {
    let (url, requests) =
        serve_sequence_with_capture(vec![DESCRIBE, JOB_PAGE, DESCRIBE, SELECTED_PAGE]).await;
    let initial = checkpoint("v1.00000000000000000000000000000000");
    let mut before = build_poller(&url, Some(initial.clone())).await;
    let ignored = before.poll_once().await.expect("unrouted page");
    assert!(ignored.system_stream.is_none());
    drop(before);
    let mut after = build_poller(&url, Some(initial.clone())).await;
    let selected = after
        .poll_once()
        .await
        .expect("selected page after restart");
    assert_eq!(
        selected
            .system_stream
            .expect("selected effect")
            .previous_checkpoint,
        Some(initial.clone())
    );
    assert!(requests.lock().expect("requests")[3].contains(initial.as_str()));
}

#[tokio::test]
async fn given_checkpoint_only_idle_page_when_polled_then_no_durable_job_is_created() {
    let (url, _) = serve_sequence_with_capture(vec![DESCRIBE, EMPTY_PAGE, EMPTY_PAGE]).await;
    let mut poller = build_poller(&url, None).await;
    for _ in 0..2 {
        let batch = poller.poll_once().await.expect("idle page");
        assert!(batch.records.is_empty());
        assert!(batch.system_stream.is_none());
    }
}

#[tokio::test]
async fn given_transient_scan_progress_when_checkpoints_reload_then_scan_resets_to_durable_frontier()
 {
    let (url, requests) =
        serve_sequence_with_capture(vec![DESCRIBE, JOB_PAGE, SELECTED_PAGE]).await;
    let initial = checkpoint("v1.00000000000000000000000000000000");
    let mut poller = build_poller(&url, Some(initial.clone())).await;
    assert!(
        poller
            .poll_once()
            .await
            .expect("unrouted page")
            .system_stream
            .is_none()
    );
    poller
        .reload_checkpoints(&[])
        .await
        .expect("reload checkpoint state");
    let selected = poller
        .poll_once()
        .await
        .expect("selected page after reload");
    assert_eq!(
        selected
            .system_stream
            .expect("selected effect")
            .previous_checkpoint,
        Some(initial.clone())
    );
    assert!(requests.lock().expect("requests")[2].contains(initial.as_str()));
}

#[tokio::test]
async fn given_transient_scan_progress_when_next_page_is_malformed_then_durable_and_scan_predecessors_are_preserved()
 {
    let (url, requests) =
        serve_sequence_with_capture(vec![DESCRIBE, JOB_PAGE, MALFORMED_JOB_PAGE, SELECTED_PAGE])
            .await;
    let initial = checkpoint("v1.00000000000000000000000000000000");
    let mut poller = build_poller(&url, Some(initial.clone())).await;
    assert!(
        poller
            .poll_once()
            .await
            .expect("unrouted page")
            .system_stream
            .is_none()
    );
    assert!(
        poller.poll_once().await.is_err(),
        "malformed unregistered records must fail closed"
    );
    let selected = poller.poll_once().await.expect("retry selected page");
    assert_eq!(
        selected
            .system_stream
            .expect("selected effect")
            .previous_checkpoint,
        Some(initial)
    );
    let requests = requests.lock().expect("requests");
    assert!(requests[2].contains("v1.11111111111111111111111111111111"));
    assert!(requests[3].contains("v1.11111111111111111111111111111111"));
}

async fn build_poller(url: &str, checkpoint: Option<SystemStreamCheckpointToken>) -> SourcePoller {
    let source = config::AnalyticsSourceConfig {
        endpoint_url: Some(url.to_owned()),
        poll_max_responses_per_interval: 1,
        ..Default::default()
    };
    let resume = checkpoint.map(|checkpoint| {
        SystemStreamResume::new(
            SystemStreamScope::try_new("aux-system", 7).expect("scope"),
            checkpoint,
        )
    });
    SourcePoller::from_plans_with_system_resume(
        &source,
        vec![SourceTablePlan::aux_storage(
            "source_users".to_owned(),
            vec!["users".to_owned()],
        )],
        &[],
        resume,
    )
    .await
    .expect("source poller")
}

fn checkpoint(value: &str) -> SystemStreamCheckpointToken {
    SystemStreamCheckpointToken::try_from(value).expect("checkpoint")
}

#[tokio::test]
async fn given_uncommitted_page_when_limit_is_reduced_then_repoll_preserves_durable_predecessor_and_commit()
 {
    let (url, requests) = serve_sequence_with_capture(vec![
        DESCRIBE,
        SELECTED_PAGE,
        SELECTED_PAGE,
        EMPTY_SELECTED_PAGE,
    ])
    .await;
    let initial = checkpoint("v1.00000000000000000000000000000000");
    let mut poller = build_poller(&url, Some(initial.clone())).await;
    let first = poller.poll_once().await.expect("first page");
    assert!(poller.reduce_page_limit().await.expect("reduce limit"));
    let smaller = poller.poll_once().await.expect("smaller retry");
    assert_eq!(smaller.system_stream, first.system_stream);
    assert_eq!(
        smaller
            .system_stream
            .as_ref()
            .expect("scope")
            .previous_checkpoint
            .as_ref(),
        Some(&initial)
    );
    poller.commit_batch(&smaller);
    assert!(
        poller
            .poll_once()
            .await
            .expect("after commit")
            .records
            .is_empty()
    );
    let captured = requests.lock().expect("requests");
    assert!(captured[1].contains("\"Limit\":8192"));
    assert!(captured[2].contains("\"Limit\":4096"));
    assert!(captured[2].contains(initial.as_str()));
    assert!(captured[3].contains("v1.22222222222222222222222222222222"));
}

#[tokio::test]
async fn given_page_limit_of_one_when_reduced_then_reduction_stops_without_consuming_source() {
    let (url, requests) = serve_sequence_with_capture(vec![DESCRIBE, SELECTED_PAGE]).await;
    let mut poller = build_poller(&url, None).await;
    for _ in 0..13 {
        assert!(poller.reduce_page_limit().await.expect("reduce limit"));
    }
    assert!(!poller.reduce_page_limit().await.expect("minimum limit"));
    assert_eq!(requests.lock().expect("requests").len(), 1);
    assert_eq!(
        poller
            .poll_once()
            .await
            .expect("one-record page")
            .records
            .len(),
        1
    );
    assert!(requests.lock().expect("requests")[1].contains("\"Limit\":1"));
}
