use super::*;

pub struct CheckRowStream {
    state: CheckRowStreamState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckRowStreamItem {
    pub partition: Option<String>,
    pub row: CheckRow,
}

enum CheckRowStreamState {
    Buffered {
        rows: vec::IntoIter<CheckRow>,
    },
    Streaming {
        source: Box<dyn CheckRowStreamSource>,
    },
}

pub trait CheckRowStreamSource {
    fn next_item(&mut self) -> CheckResult<Option<CheckRowStreamItem>>;
}

impl CheckRowStream {
    #[must_use]
    pub fn from_unsorted(mut rows: Vec<CheckRow>) -> Self {
        rows.sort_by(|left, right| row_sort_key(left).cmp(&row_sort_key(right)));
        Self::from_sorted(rows)
    }

    #[must_use]
    pub fn from_sorted(rows: Vec<CheckRow>) -> Self {
        Self {
            state: CheckRowStreamState::Buffered {
                rows: rows.into_iter(),
            },
        }
    }

    #[must_use]
    pub fn from_source(source: Box<dyn CheckRowStreamSource>) -> Self {
        Self {
            state: CheckRowStreamState::Streaming { source },
        }
    }

    pub fn next_row(&mut self) -> CheckResult<Option<CheckRow>> {
        Ok(self.next_item()?.map(|item| item.row))
    }

    pub(super) fn next_item(&mut self) -> CheckResult<Option<CheckRowStreamItem>> {
        match &mut self.state {
            CheckRowStreamState::Buffered { rows } => {
                Ok(rows.next().map(CheckRowStreamItem::global))
            }
            CheckRowStreamState::Streaming { source } => source.next_item(),
        }
    }
}

impl CheckRowStreamItem {
    #[must_use]
    pub fn global(row: CheckRow) -> Self {
        Self {
            partition: None,
            row,
        }
    }

    #[must_use]
    pub fn partitioned(partition: impl Into<String>, row: CheckRow) -> Self {
        Self {
            partition: Some(partition.into()),
            row,
        }
    }
}
