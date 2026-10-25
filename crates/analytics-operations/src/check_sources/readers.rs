use super::*;

pub trait CurrentRowReader {
    fn current_rows(
        &self,
        projection: &CheckRowProjection,
    ) -> CheckResult<Vec<ProjectedCurrentRow>>;
}

pub trait CurrentRowStreamReader {
    fn current_row_stream(
        &self,
        projection: &CheckRowProjection,
    ) -> CheckResult<ProjectedCurrentRowStream>;
}

pub trait CurrentRowPageReader {
    fn current_row_pages(
        &self,
        projection: &CheckRowProjection,
    ) -> CheckResult<Vec<ProjectedCurrentRowPage>>;
}

pub trait ProductionCurrentRowPageReader {
    fn open_row_stream(
        &self,
        projection: CheckRowProjection,
        page_size: usize,
    ) -> CheckResult<ProjectedCurrentRowStream>
    where
        Self: Sized + Clone + 'static,
    {
        ProjectedCurrentRowStream::from_page_reader(self.clone(), projection, page_size)
    }

    fn current_row_page(
        &self,
        request: &CurrentRowPageRequest,
    ) -> CheckResult<ProjectedCurrentRowPage>;

    fn stream_order(&self) -> CurrentRowStreamOrder {
        CurrentRowStreamOrder::Unordered
    }
}
