use super::comparison::compare_count_totals;

#[test]
fn given_zero_exact_over_limit_and_extreme_counts_when_compared_then_totals_and_bounded_sample_order_are_preserved()
 {
    for (source, destination, limit, missing, extra, expected_missing, expected_extra) in [
        (0, 0, 3, 0, 0, vec![], vec![]),
        (1, 0, 0, 1, 0, vec![], vec![]),
        (1, 0, 3, 1, 0, vec!["count_missing:0"], vec![]),
        (
            3,
            0,
            3,
            3,
            0,
            vec!["count_missing:0", "count_missing:1", "count_missing:2"],
            vec![],
        ),
        (
            4,
            1,
            2,
            3,
            0,
            vec!["count_missing:0", "count_missing:1"],
            vec![],
        ),
        (
            1,
            4,
            2,
            0,
            3,
            vec![],
            vec!["count_extra:0", "count_extra:1"],
        ),
        (
            u64::MAX,
            0,
            2,
            u64::MAX,
            0,
            vec!["count_missing:0", "count_missing:1"],
            vec![],
        ),
        (
            0,
            u64::MAX,
            2,
            0,
            u64::MAX,
            vec![],
            vec!["count_extra:0", "count_extra:1"],
        ),
    ] {
        let (actual_missing, actual_extra, samples) =
            compare_count_totals(source, destination, limit);
        assert_eq!((actual_missing, actual_extra), (missing, extra));
        assert_eq!(samples.missing_keys, expected_missing);
        assert_eq!(samples.extra_keys, expected_extra);
        assert!(samples.mismatched_keys.is_empty());
        assert!(samples.stale_keys.is_empty());
        assert!(samples.privacy_violation_keys.is_empty());
    }
}
