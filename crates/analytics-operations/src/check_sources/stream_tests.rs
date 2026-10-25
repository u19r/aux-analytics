use super::{ProjectedCurrentRow, ProjectedCurrentRowPage, ProjectedCurrentRowStream};

fn row(key: &str) -> ProjectedCurrentRow {
    ProjectedCurrentRow::new(
        key,
        Default::default(),
        crate::check_test_support::backup_position(1),
    )
}

#[test]
fn given_buffered_pages_when_drained_then_empty_terminal_pages_preserve_following_rows_and_partitions()
 {
    let mut stream = ProjectedCurrentRowStream::from_pages(vec![
        ProjectedCurrentRowPage::terminal_partition("a", vec![]),
        ProjectedCurrentRowPage::terminal_partition("b", vec![row("first"), row("second")]),
        ProjectedCurrentRowPage::terminal_partition("c", vec![row("third")]),
    ]);
    for (key, partition) in [("first", "b"), ("second", "b"), ("third", "c")] {
        let item = stream.next_item().unwrap().unwrap();
        assert_eq!(item.row.key, key);
        assert_eq!(item.partition.as_deref(), Some(partition));
    }
    assert!(stream.next_item().unwrap().is_none());
    assert!(stream.next_item().unwrap().is_none());
}
