use std::{
    sync::mpsc::{Receiver, SyncSender, sync_channel},
    thread::{self, JoinHandle},
};

use super::{CheckRowProjection, CheckRowStreamSource};
use crate::{
    CheckError, CheckResult, CheckRow, CheckRowStreamItem, constants::DUCKDB_CHECK_BATCH_ROWS,
};

/// Own one ordered native query without a self-referential
/// connection/statement. The rendezvous channel queues no batches. Disconnect
/// before joining so a producer blocked on delivery cannot outlive its
/// consumer.
pub(super) struct DuckDbScan<Row> {
    batches: Option<Receiver<Vec<Row>>>,
    first_batch: Option<Vec<Row>>,
    worker: Option<JoinHandle<CheckResult<()>>>,
}

impl<Row: Send + 'static> DuckDbScan<Row> {
    pub(super) fn try_new(
        connection: duckdb::Connection,
        projection: CheckRowProjection,
        batch_rows: usize,
        decode: fn(&CheckRowProjection, &duckdb::Row<'_>) -> CheckResult<Row>,
    ) -> CheckResult<Self> {
        let (delivery, receiver) = DuckDbBatchDelivery::try_new(batch_rows)?;
        let worker = thread::Builder::new()
            .name("duckdb-check-scan".to_owned())
            .spawn(move || delivery.send_query_rows(connection, projection, decode))
            .map_err(CheckError::ScanWorkerStart)?;
        let mut scan = Self {
            batches: Some(receiver),
            first_batch: None,
            worker: Some(worker),
        };
        // Establish the snapshot and surface preparation/first-batch errors
        // at admission, before another stream or writer is started.
        scan.first_batch = scan.next_batch()?;
        Ok(scan)
    }

    pub(super) fn next_batch(&mut self) -> CheckResult<Option<Vec<Row>>> {
        if let Some(rows) = self.first_batch.take() {
            return Ok(Some(rows));
        }
        let Some(receiver) = &self.batches else {
            return Ok(None);
        };
        match receiver.recv() {
            Ok(rows) => Ok(Some(rows)),
            Err(_) => {
                self.batches.take();
                self.finish_worker()?;
                Ok(None)
            }
        }
    }
}

impl<Row> DuckDbScan<Row> {
    fn finish_worker(&mut self) -> CheckResult<()> {
        let Some(worker) = self.worker.take() else {
            return Ok(());
        };
        worker.join().map_err(|_| CheckError::ScanWorkerPanicked)?
    }
}

impl<Row> Drop for DuckDbScan<Row> {
    fn drop(&mut self) {
        // Disconnect delivery, then settle execution and connection
        // destruction. No detached query, connection cache or background
        // registry survives.
        self.batches.take();
        let _ = self.finish_worker();
    }
}

pub(super) struct DuckDbCheckScan {
    scan: DuckDbScan<CheckRow>,
    current_rows: std::vec::IntoIter<CheckRow>,
}

impl DuckDbCheckScan {
    pub(super) fn try_new(
        connection: duckdb::Connection,
        projection: CheckRowProjection,
    ) -> CheckResult<Self> {
        Ok(Self {
            scan: DuckDbScan::try_new(
                connection,
                projection,
                DUCKDB_CHECK_BATCH_ROWS,
                CheckRowProjection::read_check_row,
            )?,
            current_rows: Vec::new().into_iter(),
        })
    }
}

impl CheckRowStreamSource for DuckDbCheckScan {
    fn next_item(&mut self) -> CheckResult<Option<CheckRowStreamItem>> {
        loop {
            if let Some(row) = self.current_rows.next() {
                return Ok(Some(CheckRowStreamItem::global(row)));
            }
            let Some(rows) = self.scan.next_batch()? else {
                return Ok(None);
            };
            self.current_rows = rows.into_iter();
        }
    }
}

/// Own the batch bound and rendezvous delivery together. A batch cannot be
/// configured empty, and there is never a queue of retained row buffers.
struct DuckDbBatchDelivery<Row> {
    sender: SyncSender<Vec<Row>>,
    batch_rows: usize,
}

impl<Row> DuckDbBatchDelivery<Row> {
    fn try_new(batch_rows: usize) -> CheckResult<(Self, Receiver<Vec<Row>>)> {
        if batch_rows == 0 {
            return Err(CheckError::InvalidBackendConfiguration(
                "production current-row page size must be greater than zero".to_owned(),
            ));
        }
        let (sender, receiver) = sync_channel(0);
        Ok((Self { sender, batch_rows }, receiver))
    }

    fn send_query_rows(
        self,
        connection: duckdb::Connection,
        projection: CheckRowProjection,
        decode: fn(&CheckRowProjection, &duckdb::Row<'_>) -> CheckResult<Row>,
    ) -> CheckResult<()> {
        let mut statement = connection.prepare(&projection.ordered_select_sql())?;
        let mut rows = statement.query([])?;
        loop {
            let mut batch = Vec::with_capacity(self.batch_rows);
            while batch.len() < self.batch_rows {
                let Some(row) = rows.next()? else {
                    break;
                };
                batch.push(decode(&projection, row)?);
            }
            if batch.is_empty() || self.sender.send(batch).is_err() {
                return Ok(());
            }
        }
    }
}
