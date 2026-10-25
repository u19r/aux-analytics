//! Serialized byte accounting without temporary payloads.

use std::io;

use analytics_contract::StorageStreamRecord;

use super::BackfillExecutionResult;
use crate::{
    OperationRateLimiter, RateLimitClock, RateLimitDenied, RateLimitUsage,
    enforce_rate_limit_with_limiter,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct BackfillBatchAccounting {
    pub(super) rows: u64,
    pub(super) bytes: u64,
}

pub(super) fn batch_accounting(
    records: &[StorageStreamRecord],
) -> BackfillExecutionResult<BackfillBatchAccounting> {
    if records.is_empty() {
        return Ok(BackfillBatchAccounting { rows: 0, bytes: 0 });
    }
    // Rate limiting needs the encoded length, not a temporary payload copy.
    let bytes = serialized_len(records)?;
    Ok(BackfillBatchAccounting {
        rows: u64::try_from(records.len()).unwrap_or(u64::MAX),
        bytes: u64::try_from(bytes).unwrap_or(u64::MAX),
    })
}

pub(crate) fn serialized_len<T: serde::Serialize + ?Sized>(
    value: &T,
) -> Result<usize, serde_json::Error> {
    let mut counter = ByteCounter::default();
    serde_json::to_writer(&mut counter, value)?;
    Ok(counter.len)
}

#[derive(Debug, Default)]
struct ByteCounter {
    len: usize,
}

impl io::Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.len = self.len.saturating_add(bytes.len());
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl BackfillBatchAccounting {
    pub(super) fn reserve_destination<C: RateLimitClock>(
        self,
        limiter: &OperationRateLimiter,
        clock: &C,
    ) -> Result<(), RateLimitDenied> {
        if self.rows == 0 {
            return Ok(());
        }
        enforce_rate_limit_with_limiter(
            limiter,
            RateLimitUsage {
                rows: self.rows,
                bytes: self.bytes,
                destination_writes: 1,
                ..RateLimitUsage::default()
            },
            clock,
        )
    }
}
