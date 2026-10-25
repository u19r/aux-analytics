use std::{collections::HashSet, fmt};

use super::*;
use crate::constants::{MAX_CONSECUTIVE_EMPTY_PAGES, MAX_PAGE_WORK};

pub struct ProjectedCurrentRowStream {
    source: CurrentRowPages,
    current_rows: std::vec::IntoIter<ProjectedCurrentRow>,
    current_partition: Option<String>,
}

enum CurrentRowPages {
    Native(super::duckdb_scan::DuckDbScan<ProjectedCurrentRow>),
    Buffered(std::vec::IntoIter<ProjectedCurrentRowPage>),
    Paged {
        source: Box<dyn ProjectedCurrentRowPageSource>,
        exhausted: bool,
    },
}

trait ProjectedCurrentRowPageSource {
    fn next_page(&mut self) -> CheckResult<Option<ProjectedCurrentRowPage>>;
}

impl CurrentRowPages {
    fn next_page(&mut self) -> CheckResult<Option<ProjectedCurrentRowPage>> {
        match self {
            Self::Native(scan) => Ok(scan.next_batch()?.map(ProjectedCurrentRowPage::terminal)),
            Self::Buffered(pages) => Ok(pages.next()),
            Self::Paged { source, exhausted } => {
                if *exhausted {
                    return Ok(None);
                }
                let page = source.next_page()?;
                *exhausted = page.as_ref().is_none_or(|page| page.next_cursor.is_none());
                Ok(page)
            }
        }
    }
}

struct AdapterPageSource<Reader> {
    reader: Reader,
    projection: CheckRowProjection,
    cursor: Option<String>,
    seen_cursors: HashSet<String>,
    page_work: usize,
    consecutive_empty_pages: usize,
    page_size: usize,
    stream_order: CurrentRowStreamOrder,
    previous_partition: Option<String>,
}

impl ProjectedCurrentRowPage {
    #[must_use]
    pub fn terminal(rows: Vec<ProjectedCurrentRow>) -> Self {
        Self {
            rows,
            next_cursor: None,
            partition: None,
        }
    }

    #[must_use]
    pub fn with_next_cursor(
        rows: Vec<ProjectedCurrentRow>,
        next_cursor: impl Into<String>,
    ) -> Self {
        Self {
            rows,
            next_cursor: Some(next_cursor.into()),
            partition: None,
        }
    }

    #[must_use]
    pub fn terminal_partition(
        partition: impl Into<String>,
        rows: Vec<ProjectedCurrentRow>,
    ) -> Self {
        Self {
            rows,
            next_cursor: None,
            partition: Some(partition.into()),
        }
    }

    #[must_use]
    pub fn partition_with_next_cursor(
        partition: impl Into<String>,
        rows: Vec<ProjectedCurrentRow>,
        next_cursor: impl Into<String>,
    ) -> Self {
        Self {
            rows,
            next_cursor: Some(next_cursor.into()),
            partition: Some(partition.into()),
        }
    }
}

impl ProjectedCurrentRowStream {
    pub(super) fn from_duckdb_scan(
        scan: super::duckdb_scan::DuckDbScan<ProjectedCurrentRow>,
    ) -> Self {
        Self {
            source: CurrentRowPages::Native(scan),
            current_rows: Vec::new().into_iter(),
            current_partition: None,
        }
    }

    #[must_use]
    pub fn from_unsorted_rows(rows: Vec<ProjectedCurrentRow>) -> Self {
        Self::from_sorted_rows(sort_projected_current_rows(rows))
    }

    #[must_use]
    pub fn from_sorted_rows(rows: Vec<ProjectedCurrentRow>) -> Self {
        Self::from_pages(vec![ProjectedCurrentRowPage::terminal(rows)])
    }

    #[must_use]
    pub fn from_pages(pages: Vec<ProjectedCurrentRowPage>) -> Self {
        Self {
            source: CurrentRowPages::Buffered(pages.into_iter()),
            current_rows: Vec::new().into_iter(),
            current_partition: None,
        }
    }

    pub fn from_page_reader<Reader>(
        reader: Reader,
        projection: CheckRowProjection,
        page_size: usize,
    ) -> CheckResult<Self>
    where
        Reader: ProductionCurrentRowPageReader + 'static,
    {
        projection.validate()?;
        let stream_order = reader.stream_order();
        if stream_order == CurrentRowStreamOrder::Unordered {
            return Err(CheckError::InvalidBackendConfiguration(format!(
                "production current-row reader for {} must yield globally sorted or \
                 partition-sorted rows for bounded check comparison; declared {:?}",
                projection.table_name, stream_order
            )));
        }
        Ok(Self {
            source: CurrentRowPages::Paged {
                source: Box::new(AdapterPageSource {
                    reader,
                    projection,
                    cursor: None,
                    seen_cursors: HashSet::new(),
                    page_work: 0,
                    consecutive_empty_pages: 0,
                    page_size,
                    stream_order,
                    previous_partition: None,
                }),
                exhausted: false,
            },
            current_rows: Vec::new().into_iter(),
            current_partition: None,
        })
    }

    pub fn next_row(&mut self) -> CheckResult<Option<ProjectedCurrentRow>> {
        Ok(self.next_item()?.map(|item| item.row))
    }

    pub fn next_item(&mut self) -> CheckResult<Option<ProjectedCurrentRowItem>> {
        loop {
            if let Some(row) = self.current_rows.next() {
                return Ok(Some(ProjectedCurrentRowItem {
                    partition: self.current_partition.clone(),
                    row,
                }));
            }
            let Some(page) = self.source.next_page()? else {
                return Ok(None);
            };
            self.current_partition = page.partition;
            self.current_rows = page.rows.into_iter();
        }
    }

    pub fn into_rows(mut self) -> CheckResult<Vec<ProjectedCurrentRow>> {
        let mut rows = Vec::new();
        while let Some(row) = self.next_row()? {
            rows.push(row);
        }
        Ok(rows)
    }
}

impl fmt::Debug for ProjectedCurrentRowStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.source {
            CurrentRowPages::Native(_) => formatter
                .debug_struct("ProjectedCurrentRowStream")
                .field("mode", &"native")
                .finish(),
            CurrentRowPages::Buffered(pages) => formatter
                .debug_struct("ProjectedCurrentRowStream")
                .field("mode", &"buffered")
                .field("pages", &pages.len())
                .finish(),
            CurrentRowPages::Paged { exhausted, .. } => formatter
                .debug_struct("ProjectedCurrentRowStream")
                .field("mode", &"paged")
                .field("exhausted", exhausted)
                .finish(),
        }
    }
}

impl<Reader> ProjectedCurrentRowPageSource for AdapterPageSource<Reader>
where Reader: ProductionCurrentRowPageReader
{
    fn next_page(&mut self) -> CheckResult<Option<ProjectedCurrentRowPage>> {
        loop {
            let page = self.fetch_page()?;
            if !page.rows.is_empty() {
                self.consecutive_empty_pages = 0;
                return Ok(Some(page));
            }
            if page.next_cursor.is_none() {
                self.consecutive_empty_pages = 0;
                return Ok(None);
            }
            self.record_empty_page()?;
        }
    }
}

impl<Reader: ProductionCurrentRowPageReader> AdapterPageSource<Reader> {
    fn fetch_page(&mut self) -> CheckResult<ProjectedCurrentRowPage> {
        if self.page_work >= MAX_PAGE_WORK {
            return Err(CheckError::InvalidBackendConfiguration(format!(
                "production current-row reader for {} exceeded the page/cursor work budget of {}",
                self.projection.table_name, MAX_PAGE_WORK
            )));
        }
        self.page_work += 1;
        let request = CurrentRowPageRequest {
            projection: self.projection.clone(),
            cursor: self.cursor.clone(),
            page_size: self.page_size,
        };
        let mut page = self.reader.current_row_page(&request)?;
        page.rows = sort_projected_current_rows(page.rows);
        self.validate_partition(&page)?;
        self.validate_cursor_progress(request.cursor.as_deref(), page.next_cursor.as_deref())?;
        self.cursor.clone_from(&page.next_cursor);
        Ok(page)
    }

    fn record_empty_page(&mut self) -> CheckResult<()> {
        self.consecutive_empty_pages += 1;
        if self.consecutive_empty_pages > MAX_CONSECUTIVE_EMPTY_PAGES {
            return Err(CheckError::InvalidBackendConfiguration(format!(
                "production current-row reader for {} exceeded the consecutive empty page budget \
                 of {}",
                self.projection.table_name, MAX_CONSECUTIVE_EMPTY_PAGES
            )));
        }
        Ok(())
    }

    fn validate_cursor_progress(
        &mut self,
        requested: Option<&str>,
        next: Option<&str>,
    ) -> CheckResult<()> {
        let Some(next) = next else {
            return Ok(());
        };
        if next.trim().is_empty() {
            return Err(CheckError::InvalidBackendConfiguration(format!(
                "production current-row reader for {} returned an empty cursor",
                self.projection.table_name
            )));
        }
        if requested == Some(next) {
            return Err(CheckError::InvalidBackendConfiguration(format!(
                "production current-row reader for {} returned an unchanged cursor {next}",
                self.projection.table_name
            )));
        }
        if !self.seen_cursors.insert(next.to_string()) {
            return Err(CheckError::InvalidBackendConfiguration(format!(
                "production current-row reader for {} returned a cyclic cursor {next}",
                self.projection.table_name
            )));
        }
        Ok(())
    }

    fn validate_partition(&mut self, page: &ProjectedCurrentRowPage) -> CheckResult<()> {
        if self.stream_order != CurrentRowStreamOrder::PartitionSortedByKey {
            return Ok(());
        }
        let partition = page.partition.as_ref().ok_or_else(|| {
            CheckError::InvalidBackendConfiguration(format!(
                "partition-sorted production current-row reader for {} returned a page without a \
                 partition key",
                self.projection.table_name
            ))
        })?;
        if let Some(previous) = self.previous_partition.as_ref()
            && partition < previous
        {
            return Err(CheckError::InvalidBackendConfiguration(format!(
                "partition-sorted production current-row reader for {} returned partition \
                 {partition} after {previous}",
                self.projection.table_name
            )));
        }
        self.previous_partition = Some(partition.clone());
        Ok(())
    }
}
