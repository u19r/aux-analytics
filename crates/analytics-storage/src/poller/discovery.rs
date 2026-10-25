use std::time::Instant;

use super::state::{AwsDiscoveryAttempt, AwsStreamTablePoller};
use crate::{
    aws_stream::{AwsShardDiscovery, AwsShardIteratorState},
    error::AnalyticsStorageResult,
};

impl AwsStreamTablePoller {
    pub(super) async fn fill_startup_iterators(
        &mut self,
        now: Instant,
    ) -> AnalyticsStorageResult<()> {
        let active_before_discovery = self.shard_iterators.len();
        let mut discovery_attempt = None;
        if !self.pending_expired_shard_ids.is_empty() {
            if self.shard_iterators.len() < self.max_shards
                && self.discovery_schedule.allows_pending_work(now)
            {
                self.retry_expired_shards(now).await?;
            }
            if !self.pending_expired_shard_ids.is_empty() {
                return Ok(());
            }
        }
        while self.shard_iterators.len() < self.max_shards {
            if self
                .begin_discovery_attempt(&mut discovery_attempt, now)
                .is_none()
            {
                break;
            }
            if self.pending_shard_ids.is_empty() {
                self.refill_discovery(&mut discovery_attempt, now).await?;
            }
            let Some(shard_id) = self.pending_shard_ids.pop_front() else {
                break;
            };
            if self
                .shard_iterators
                .iter()
                .any(|state| state.shard_id == shard_id)
                || self.pending_terminal_shards.contains(shard_id.as_str())
            {
                continue;
            }
            self.activate_pending_shard(shard_id, &mut discovery_attempt, now)
                .await?;
        }
        self.complete_discovery_attempt(&mut discovery_attempt, active_before_discovery, now);
        Ok(())
    }

    fn begin_discovery_attempt(
        &mut self,
        attempt: &mut Option<AwsDiscoveryAttempt>,
        now: Instant,
    ) -> Option<()> {
        if !self.pending_shard_ids.is_empty() {
            if attempt.is_some() {
                return Some(());
            }
            if !self.discovery_schedule.allows_pending_work(now) {
                return None;
            }
            *attempt = self.discovery_schedule.begin_pending(now);
            return Some(());
        }
        if self.startup_exhausted {
            if attempt.is_some() {
                return None;
            }
            let next = self
                .discovery_schedule
                .begin(self.shard_iterators.len(), now)?;
            self.startup_exhausted = false;
            *attempt = Some(next);
        } else if attempt.is_none() {
            // Resume the same bounded pagination window, including its error
            // path.
            *attempt = Some(AwsDiscoveryAttempt::Initial);
        }
        Some(())
    }

    async fn refill_discovery(
        &mut self,
        discovery_attempt: &mut Option<AwsDiscoveryAttempt>,
        now: Instant,
    ) -> AnalyticsStorageResult<()> {
        let include_closed_shards =
            !matches!(*discovery_attempt, Some(AwsDiscoveryAttempt::Retry { .. }));
        let capacity = self.max_shards.saturating_sub(self.shard_iterators.len());
        let mut discovery = AwsShardDiscovery {
            shard_iterators: Vec::new(),
            pending_shard_ids: std::mem::take(&mut self.pending_shard_ids),
            next_exclusive_start_shard_id: self.next_exclusive_start_shard_id.take(),
            startup_exhausted: self.startup_exhausted,
        };
        let refill_result = self
            .client
            .refill_shard_ids(
                self.stream_arn.as_str(),
                capacity,
                include_closed_shards,
                &mut discovery,
            )
            .await;
        self.pending_shard_ids = discovery.pending_shard_ids;
        self.next_exclusive_start_shard_id = discovery.next_exclusive_start_shard_id;
        self.startup_exhausted = discovery.startup_exhausted;
        if let Err(error) = refill_result {
            // A failed discovery must still finish the attempt. Keep
            // pagination schedulable so the next poll waits for the
            // wall-clock retry deadline instead of bypassing it.
            self.startup_exhausted = true;
            self.fail_discovery_attempt(discovery_attempt, now);
            return Err(error);
        }
        Ok(())
    }

    async fn activate_pending_shard(
        &mut self,
        shard_id: String,
        discovery_attempt: &mut Option<AwsDiscoveryAttempt>,
        now: Instant,
    ) -> AnalyticsStorageResult<()> {
        let iterator_result = self
            .client
            .shard_iterator(
                self.stream_arn.as_str(),
                &self.durable_checkpoints,
                self.source_table_name.as_str(),
                shard_id.as_str(),
            )
            .await;
        let iterator_state = match iterator_result {
            Ok(iterator_state) => iterator_state,
            Err(error) => {
                self.pending_shard_ids.push_front(shard_id);
                self.startup_exhausted = true;
                self.fail_discovery_attempt(discovery_attempt, now);
                return Err(error);
            }
        };
        if let Some(state) = iterator_state {
            self.shard_iterators.push(state);
        }
        Ok(())
    }

    fn complete_discovery_attempt(
        &mut self,
        discovery_attempt: &mut Option<AwsDiscoveryAttempt>,
        active_before_discovery: usize,
        now: Instant,
    ) {
        if let Some(attempt) = discovery_attempt.take() {
            self.discovery_schedule.complete(
                attempt,
                active_before_discovery,
                self.shard_iterators.len(),
                self.max_shards,
                !self.pending_shard_ids.is_empty(),
                now,
            );
        }
    }

    fn fail_discovery_attempt(
        &mut self,
        discovery_attempt: &mut Option<AwsDiscoveryAttempt>,
        now: Instant,
    ) {
        if let Some(attempt) = discovery_attempt.take() {
            self.discovery_schedule.fail(attempt, now);
        }
    }

    async fn rebuild_expired_shard(
        &mut self,
        shard_id: &str,
        now: Instant,
    ) -> AnalyticsStorageResult<Option<AwsShardIteratorState>> {
        let replacement = match self
            .client
            .shard_iterator(
                self.stream_arn.as_str(),
                &self.durable_checkpoints,
                self.source_table_name.as_str(),
                shard_id,
            )
            .await
        {
            Ok(replacement) => replacement,
            Err(error) => {
                self.discovery_schedule.fail_current(now);
                return Err(error);
            }
        };
        let Some(replacement) = replacement else {
            // A missing replacement is still a failed rebuild. Keep the
            // shard suspended and let the wall-clock schedule govern the
            // next attempt instead of spinning on every poll.
            self.discovery_schedule.fail_current(now);
            return Ok(None);
        };
        Ok(Some(replacement))
    }

    pub(super) fn suspend_expired_shard(&mut self, shard_id: String) {
        self.shard_iterators
            .retain(|state| state.shard_id != shard_id);
        if !self
            .pending_expired_shard_ids
            .iter()
            .any(|pending| pending == &shard_id)
        {
            self.pending_expired_shard_ids.push_back(shard_id);
        }
    }

    pub(super) async fn retry_expired_shards(
        &mut self,
        now: Instant,
    ) -> AnalyticsStorageResult<()> {
        if !self.discovery_schedule.allows_pending_work(now) {
            return Ok(());
        }
        let mut attempts_remaining = self.pending_expired_shard_ids.len();
        let mut first_error = None;
        while attempts_remaining > 0 {
            if self.shard_iterators.len() >= self.max_shards {
                break;
            }
            let Some(shard_id) = self.pending_expired_shard_ids.pop_front() else {
                break;
            };
            attempts_remaining -= 1;
            match self.retry_expired_shard(shard_id.as_str(), now).await {
                Ok(Some(replacement)) => self.shard_iterators.push(replacement),
                Ok(None) => {}
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    async fn retry_expired_shard(
        &mut self,
        shard_id: &str,
        now: Instant,
    ) -> AnalyticsStorageResult<Option<AwsShardIteratorState>> {
        match self.rebuild_expired_shard(shard_id, now).await {
            Ok(Some(replacement)) => Ok(Some(replacement)),
            Ok(None) => {
                self.pending_expired_shard_ids
                    .push_back(shard_id.to_string());
                Ok(None)
            }
            Err(error) => {
                self.pending_expired_shard_ids
                    .push_back(shard_id.to_string());
                Err(error)
            }
        }
    }
}
