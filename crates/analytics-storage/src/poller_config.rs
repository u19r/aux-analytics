use std::time::Duration;

use config::AnalyticsSourceConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PollerConfig {
    pub poll_interval: Duration,
    pub request_timeout: Duration,
    pub max_shards: usize,
    pub max_responses_per_interval: usize,
    pub max_records_per_response: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollRequest {
    pub shard_id: String,
    pub max_responses: usize,
}

impl Default for PollerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(100),
            request_timeout: Duration::from_millis(5_000),
            max_shards: 16,
            max_responses_per_interval: 4,
            max_records_per_response: 8_192,
        }
    }
}

impl PollerConfig {
    #[must_use]
    pub fn from_source_config(source: &AnalyticsSourceConfig) -> Self {
        Self {
            poll_interval: Duration::from_millis(source.poll_interval_ms),
            request_timeout: Duration::from_millis(source.poll_request_timeout_ms),
            max_shards: source.poll_max_shards,
            max_responses_per_interval: source.poll_max_responses_per_interval,
            max_records_per_response: source.poll_max_records_per_response,
        }
    }

    #[must_use]
    pub fn response_budget_per_shard(self) -> usize {
        self.max_responses_per_interval
            .saturating_div(self.max_shards)
            .max(1)
    }

    #[must_use]
    pub fn plan_requests<'a>(
        self,
        shard_ids: impl IntoIterator<Item = &'a str>,
    ) -> Vec<PollRequest> {
        let mut remaining_response_budget = self.max_responses_per_interval;
        let mut requests = Vec::new();
        for shard_id in shard_ids.into_iter().take(self.max_shards) {
            if remaining_response_budget == 0 {
                break;
            }
            let max_responses = self
                .response_budget_per_shard()
                .min(remaining_response_budget);
            remaining_response_budget = remaining_response_budget.saturating_sub(max_responses);
            requests.push(PollRequest {
                shard_id: shard_id.to_string(),
                max_responses,
            });
        }
        requests
    }
}
