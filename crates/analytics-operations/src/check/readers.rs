use super::*;

pub trait SourceTruthRows {
    fn source_rows(&self, target_tables: &[String]) -> CheckResult<Vec<CheckRow>>;

    fn source_row_stream(&self, target_tables: &[String]) -> CheckResult<CheckRowStream> {
        Ok(CheckRowStream::from_unsorted(
            self.source_rows(target_tables)?,
        ))
    }
}

pub trait DestinationRows {
    fn destination_rows(&self, target_tables: &[String]) -> CheckResult<Vec<CheckRow>>;

    fn destination_row_stream(&self, target_tables: &[String]) -> CheckResult<CheckRowStream> {
        Ok(CheckRowStream::from_unsorted(
            self.destination_rows(target_tables)?,
        ))
    }
}

impl SourceTruthRows for LocalCheckDataset {
    fn source_rows(&self, target_tables: &[String]) -> CheckResult<Vec<CheckRow>> {
        Ok(filter_target_tables(&self.rows, target_tables))
    }
}

impl DestinationRows for LocalCheckDataset {
    fn destination_rows(&self, target_tables: &[String]) -> CheckResult<Vec<CheckRow>> {
        Ok(filter_target_tables(&self.rows, target_tables))
    }
}

impl<T> SourceTruthRows for T
where T: CheckRowSource
{
    fn source_rows(&self, target_tables: &[String]) -> CheckResult<Vec<CheckRow>> {
        self.rows(target_tables)
            .map_err(|error| map_backend_read_error(error, self.support(), true))
    }

    fn source_row_stream(&self, target_tables: &[String]) -> CheckResult<CheckRowStream> {
        let support = self.support();
        let stream = self
            .row_stream(target_tables)
            .map_err(|error| map_backend_read_error(error, support.clone(), true))?;
        Ok(CheckRowStream::from_source(Box::new(
            BackendMappedCheckRowStream {
                stream,
                support,
                is_source: true,
            },
        )))
    }
}

impl<T> DestinationRows for T
where T: CheckRowSource
{
    fn destination_rows(&self, target_tables: &[String]) -> CheckResult<Vec<CheckRow>> {
        self.rows(target_tables)
            .map_err(|error| map_backend_read_error(error, self.support(), false))
    }

    fn destination_row_stream(&self, target_tables: &[String]) -> CheckResult<CheckRowStream> {
        let support = self.support();
        let stream = self
            .row_stream(target_tables)
            .map_err(|error| map_backend_read_error(error, support.clone(), false))?;
        Ok(CheckRowStream::from_source(Box::new(
            BackendMappedCheckRowStream {
                stream,
                support,
                is_source: false,
            },
        )))
    }
}
