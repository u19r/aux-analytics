use storage_types::{SystemStreamCheckpointToken, SystemStreamScope};

use crate::{
    SourcePoller, SourceTablePlan, SystemStreamResume,
    constants::{
        DESCRIBE, EMPTY_PAGE, EMPTY_SELECTED_PAGE, JOB_PAGE, MALFORMED_JOB_PAGE,
        NEXT_SELECTED_PAGE, SELECTED_PAGE,
    },
    poller::test_support::serve_sequence_with_capture,
};

#[tokio::test]
async fn given_pending_effect_when_polled_then_read_waits_until_commit() {
    let (url, requests) =
        serve_sequence_with_capture(vec![DESCRIBE, SELECTED_PAGE, NEXT_SELECTED_PAGE]).await;
    let mut poller = build_poller(&url, None).await;

    let first = poller.poll_once().await.expect("first page");
    let first_stream = first.system_stream.clone().expect("first effect");
    assert_eq!(first.records.len(), 1);

    // The current effect is in flight. No further source read is issued.
    let pending = poller.poll_once().await.expect("pending effect");
    assert!(pending.records.is_empty());
    assert!(pending.system_stream.is_none());

    assert_eq!(requests.lock().unwrap().len(), 2);
    poller.commit_batch(&first);
    let next = poller.poll_once().await.expect("next page");
    let next_stream = next.system_stream.as_ref().expect("next effect");
    assert_eq!(
        next_stream.previous_checkpoint.as_ref(),
        Some(&first_stream.checkpoint)
    );
    assert_eq!(
        next_stream.checkpoint,
        checkpoint("v1.33333333333333333333333333333333")
    );
    assert_eq!(next.records.len(), 1);

    let captured = requests.lock().expect("requests");
    assert_eq!(captured.len(), 3);
    assert!(captured[2].contains(first_stream.checkpoint.as_str()));
}

#[tokio::test]
async fn given_committed_destination_frontier_when_pending_then_next_page_starts_at_commit() {
    let (url, requests) =
        serve_sequence_with_capture(vec![DESCRIBE, SELECTED_PAGE, NEXT_SELECTED_PAGE]).await;
    let initial = checkpoint("v1.00000000000000000000000000000000");
    let mut poller = build_poller(&url, Some(initial)).await;

    let first = poller.poll_once().await.expect("first page");
    let first_stream = first.system_stream.clone().expect("first effect");
    assert!(
        poller
            .poll_once()
            .await
            .expect("pending effect")
            .records
            .is_empty()
    );

    // A destination-authority reload resumes at its committed frontier.
    poller
        .reload_system_resume(Some(SystemStreamResume::new(
            first_stream.source_scope.clone(),
            first_stream.checkpoint.clone(),
        )))
        .expect("reload matching frontier");

    let next = poller.poll_once().await.expect("next page");
    let next_stream = next.system_stream.as_ref().expect("next effect");
    assert_eq!(
        next_stream.previous_checkpoint.as_ref(),
        Some(&first_stream.checkpoint)
    );
    assert_eq!(next.records.len(), 1);
    assert_eq!(requests.lock().expect("requests").len(), 3);
}

#[tokio::test]
async fn given_rewound_destination_frontier_when_pending_then_first_page_is_replayed() {
    let (url, requests) =
        serve_sequence_with_capture(vec![DESCRIBE, SELECTED_PAGE, SELECTED_PAGE]).await;
    let initial = checkpoint("v1.00000000000000000000000000000000");
    let mut poller = build_poller(&url, Some(initial.clone())).await;

    let first = poller.poll_once().await.expect("first page");
    let first_stream = first.system_stream.clone().expect("first effect");
    assert!(
        poller
            .poll_once()
            .await
            .expect("pending effect")
            .records
            .is_empty()
    );

    // The in-flight effect did not commit. Re-read the first page from
    // its predecessor instead of skipping to the following page.
    poller
        .reload_system_resume(Some(SystemStreamResume::new(
            first_stream.source_scope.clone(),
            initial,
        )))
        .expect("reload rewound frontier");

    let replay = poller.poll_once().await.expect("re-read from frontier");
    let replay_stream = replay.system_stream.as_ref().expect("replayed effect");
    assert_eq!(
        replay_stream.previous_checkpoint,
        first_stream.previous_checkpoint
    );
    assert_eq!(replay_stream.checkpoint, first_stream.checkpoint);
    assert_eq!(replay.records.len(), 1);
    let captured = requests.lock().expect("requests");
    assert_eq!(captured.len(), 3);
    assert!(captured[2].contains("v1.00000000000000000000000000000000"));
}

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
    let (url, requests) =
        serve_sequence_with_capture(vec![DESCRIBE, JOB_PAGE, SELECTED_PAGE, EMPTY_SELECTED_PAGE])
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

    let pending = poller
        .poll_once()
        .await
        .expect("pending selected page must not be re-read");
    assert!(pending.records.is_empty());
    assert!(pending.system_stream.is_none());
    poller.commit_batch(&selected);
    let empty = poller.poll_once().await.expect("idle page after commit");
    assert!(empty.system_stream.is_none());
    let requests = requests.lock().expect("requests");
    assert!(requests[1].contains(initial.as_str()));
    assert!(requests[2].contains("v1.11111111111111111111111111111111"));
    assert!(requests[3].contains("v1.22222222222222222222222222222222"));
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
        // These tests exercise record and page bounds, not the byte budget.
        max_batch_bytes: usize::MAX,
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
    // A page never asks for more records than the routed batch can hold.
    // Reducing the page limit also halves the batch bound.
    assert!(captured[1].contains("\"Limit\":4096"));
    assert!(captured[2].contains("\"Limit\":2048"));
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

#[tokio::test]
async fn given_multiple_destinations_when_pages_are_combined_then_the_combined_job_stays_bounded() {
    let (url, requests) =
        serve_sequence_with_capture(vec![DESCRIBE, SELECTED_PAGE, SELECTED_PAGE]).await;
    let source = config::AnalyticsSourceConfig {
        endpoint_url: Some(url),
        poll_max_responses_per_interval: 4,
        max_batch_bytes: usize::MAX,
        ..Default::default()
    };
    let mut poller = SourcePoller::from_plans(
        &source,
        vec![SourceTablePlan::aux_storage(
            "source_users".to_owned(),
            vec!["users".to_owned(), "directory".to_owned()],
        )],
        &[],
    )
    .await
    .expect("fanout poller");
    let super::state::SourceTablePoller::AuxStorage(source) = &mut poller.tables[0] else {
        panic!("SystemStream source");
    };
    source.max_records_per_batch = 3;
    let batch = poller.poll_once().await.expect("bounded batch");
    let destinations: usize = batch
        .records
        .iter()
        .map(|record| record.analytics_table_names.len())
        .sum();
    assert_eq!(
        destinations, 2,
        "one page must fit; the next would exceed the job bound"
    );
    assert_eq!(requests.lock().expect("requests").len(), 2);
    assert!(batch.system_stream.is_some());
}

#[tokio::test]
async fn given_valid_page_then_malformed_page_when_batch_retries_then_no_records_are_skipped() {
    let (url, requests) = serve_sequence_with_capture(vec![
        DESCRIBE,
        SELECTED_PAGE,
        MALFORMED_JOB_PAGE,
        SELECTED_PAGE,
    ])
    .await;
    let initial = checkpoint("v1.00000000000000000000000000000000");
    let mut poller = build_poller(&url, Some(initial.clone())).await;
    poller.config.max_responses_per_interval = 2;
    assert!(poller.poll_once().await.is_err());
    poller.config.max_responses_per_interval = 1;
    let replay = poller
        .poll_once()
        .await
        .expect("replay entire rejected batch");
    assert_eq!(replay.records.len(), 1);
    assert_eq!(
        replay.system_stream.unwrap().previous_checkpoint,
        Some(initial.clone())
    );
    let captured = requests.lock().unwrap();
    assert!(captured[2].contains("v1.22222222222222222222222222222222"));
    assert!(
        captured[3].contains(initial.as_str()),
        "rejected batch must restart before its first page"
    );
}

#[tokio::test]
async fn given_single_record_above_byte_budget_when_polled_then_error_is_explicit_and_cursor_is_preserved()
 {
    let (url, requests) =
        serve_sequence_with_capture(vec![DESCRIBE, SELECTED_PAGE, SELECTED_PAGE]).await;
    let initial = checkpoint("v1.00000000000000000000000000000000");
    let mut poller = build_poller(&url, Some(initial.clone())).await;
    let super::state::SourceTablePoller::AuxStorage(source) = &mut poller.tables[0] else {
        panic!("system source");
    };
    source.max_records_per_response = 1;
    source.max_batch_bytes = 1;
    assert!(
        poller.poll_once().await.is_err(),
        "oversized indivisible record must not look like an idle stream"
    );
    let super::state::SourceTablePoller::AuxStorage(source) = &mut poller.tables[0] else {
        panic!("system source");
    };
    source.max_batch_bytes = usize::MAX;
    assert_eq!(poller.poll_once().await.unwrap().records.len(), 1);
    assert!(requests.lock().unwrap()[2].contains(initial.as_str()));
}

#[tokio::test]
async fn given_two_routed_pages_when_batched_then_order_and_last_checkpoint_commit_together() {
    let (url, requests) = serve_sequence_with_capture(vec![
        DESCRIBE,
        SELECTED_PAGE,
        NEXT_SELECTED_PAGE,
        EMPTY_SELECTED_PAGE,
    ])
    .await;
    let mut poller = build_poller(&url, None).await;
    poller.config.max_responses_per_interval = 2;
    let batch = poller.poll_once().await.unwrap();
    assert_eq!(
        batch
            .records
            .iter()
            .map(|r| r.record.sequence_number.as_str())
            .collect::<Vec<_>>(),
        ["user-1", "user-2"]
    );
    assert_eq!(batch.source_response_count, 2);
    assert_eq!(batch.source_record_count, 2);
    assert_eq!(
        batch.system_stream.as_ref().unwrap().checkpoint,
        checkpoint("v1.33333333333333333333333333333333")
    );
    assert!(poller.system_stream_checkpoint().is_none());
    assert!(poller.poll_once().await.unwrap().records.is_empty());
    assert_eq!(requests.lock().unwrap().len(), 3);
    poller.commit_batch(&batch);
    assert_eq!(
        poller.system_stream_checkpoint(),
        Some(&checkpoint("v1.33333333333333333333333333333333"))
    );
    poller.config.max_responses_per_interval = 1;
    assert!(poller.poll_once().await.unwrap().records.is_empty());
    assert!(requests.lock().unwrap()[3].contains("v1.33333333333333333333333333333333"));
}

#[tokio::test]
async fn given_oversized_page_when_resized_then_replay_uses_original_cursor_and_fits_budget() {
    let mut large: serde_json::Value = serde_json::from_str(SELECTED_PAGE).unwrap();
    large["Records"][0]["Item"] = serde_json::json!({"payload":{"S":"x".repeat(1000)}});
    let record = large["Records"][0].clone();
    large["Records"] = serde_json::json!([record.clone(), record]);
    let (url, requests) = serve_sequence_with_capture(vec![
        DESCRIBE.to_string(),
        large.to_string(),
        SELECTED_PAGE.to_string(),
    ])
    .await;
    let initial = checkpoint("v1.00000000000000000000000000000000");
    let mut poller = build_poller(&url, Some(initial.clone())).await;
    let super::state::SourceTablePoller::AuxStorage(source) = &mut poller.tables[0] else {
        panic!("system source");
    };
    source.max_batch_bytes = 2_048;
    assert_eq!(
        poller.poll_once().await.unwrap_err().category(),
        "source_batch_exceeds_budget"
    );
    let batch = poller.poll_once().await.unwrap();
    assert_eq!(batch.records.len(), 1);
    assert_eq!(
        batch.system_stream.unwrap().previous_checkpoint,
        Some(initial.clone())
    );
    let captured = requests.lock().unwrap();
    assert!(captured[1].contains("\"Limit\":2"));
    assert!(captured[2].contains("\"Limit\":1"));
    assert!(captured[2].contains(initial.as_str()));
}

#[tokio::test]
async fn given_two_destinations_when_page_limit_reaches_one_then_routed_record_still_fits() {
    let (url, requests) = serve_sequence_with_capture(vec![DESCRIBE, SELECTED_PAGE]).await;
    let source = config::AnalyticsSourceConfig {
        endpoint_url: Some(url),
        max_batch_bytes: usize::MAX,
        ..Default::default()
    };
    let mut poller = SourcePoller::from_plans(
        &source,
        vec![SourceTablePlan::aux_storage(
            "source_users".to_owned(),
            vec!["users".to_owned(), "directory".to_owned()],
        )],
        &[],
    )
    .await
    .unwrap();
    while poller.reduce_page_limit().await.unwrap() {}
    let batch = poller.poll_once().await.unwrap();
    assert_eq!(batch.records.len(), 1);
    assert_eq!(batch.records[0].analytics_table_names.len(), 2);
    assert!(batch.system_stream.is_some());
    assert!(requests.lock().unwrap()[1].contains("\"Limit\":1"));
}
