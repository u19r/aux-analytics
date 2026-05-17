use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::{
    CheckComparisonStrategy, CheckOutputFormat, CheckRequest, CheckRow, LocalCheckDataset,
    LocalCheckExecutor, OperationActor, OperationId, OperationStore, RateLimitPolicy,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum CheckCase {
    Clean,
    Missing,
    Extra,
    MissingExtra,
    Mismatched,
    Stale,
    PrivacyViolation,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum MbtCheckStrategy {
    CountsOnly,
    KeySet,
    ChunkHashes,
    FullRows,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum SampleLimit {
    OneSample,
    TwoSamples,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum MbtCheckOutcome {
    NotChecked,
    Complete,
    Incomplete,
    MismatchedOutcome,
    StaleOutcome,
    PrivacyViolationOutcome,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct MbtReportState {
    outcome: MbtCheckOutcome,
    checked_rows: i64,
    missing_keys: i64,
    extra_keys: i64,
    mismatched_rows: i64,
    stale_rows: i64,
    privacy_violations: i64,
    deterministic_chunk_hash_mismatches: i64,
    missing_samples: i64,
    extra_samples: i64,
    mismatched_samples: i64,
    stale_samples: i64,
    privacy_samples: i64,
}

impl Default for MbtReportState {
    fn default() -> Self {
        Self {
            outcome: MbtCheckOutcome::NotChecked,
            checked_rows: 0,
            missing_keys: 0,
            extra_keys: 0,
            mismatched_rows: 0,
            stale_rows: 0,
            privacy_violations: 0,
            deterministic_chunk_hash_mismatches: 0,
            missing_samples: 0,
            extra_samples: 0,
            mismatched_samples: 0,
            stale_samples: 0,
            privacy_samples: 0,
        }
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct MbtCheckState {
    last_report: MbtReportState,
}

impl State<MbtCheckDriver> for MbtCheckState {
    fn from_driver(driver: &MbtCheckDriver) -> Result<Self> {
        Ok(Self {
            last_report: driver.last_report,
        })
    }
}

#[derive(Default)]
struct MbtCheckDriver {
    last_report: MbtReportState,
    operation_counter: u64,
}

impl Driver for MbtCheckDriver {
    type State = MbtCheckState;

    #[allow(non_snake_case)]
    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            CheckReport(
                checkCase: CheckCase,
                strategy: MbtCheckStrategy,
                sampleLimit: SampleLimit,
            ) => {
                self.last_report = self.run_check(checkCase, strategy, sampleLimit)?;
            },
            step(
                checkCase: CheckCase?,
                strategy: MbtCheckStrategy?,
                sampleLimit: SampleLimit?,
            ) => {
                if let (Some(check_case), Some(strategy), Some(sample_limit)) =
                    (checkCase, strategy, sampleLimit)
                {
                    self.last_report = self.run_check(check_case, strategy, sample_limit)?;
                }
            },
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/check_mbt.qnt",
    max_samples = 100,
    max_steps = 4,
    seed = "0xc0ffee"
)]
fn quint_check_mbt_compares_full_report_state() -> impl Driver {
    MbtCheckDriver::default()
}

impl MbtCheckDriver {
    fn run_check(
        &mut self,
        check_case: CheckCase,
        strategy: MbtCheckStrategy,
        sample_limit: SampleLimit,
    ) -> Result<MbtReportState> {
        self.operation_counter += 1;
        let store = OperationStore::connect_in_memory()?;
        let request = CheckRequest {
            operation_id: OperationId::new(format!("check_mbt_{}", self.operation_counter))?,
            actor: OperationActor::new("quint")?,
            target_tables: vec!["users".to_string()],
            strategy: rust_strategy(strategy),
            sample_limit: rust_sample_limit(sample_limit),
            privacy_policy_version: Some("policy-v1".to_string()),
            output: CheckOutputFormat::Json,
            rate_limit: RateLimitPolicy::default(),
        };
        let (source, destination) = check_datasets(check_case);
        let report = LocalCheckExecutor.execute(&store, &request, &source, &destination)?;
        Ok(MbtReportState {
            outcome: mbt_outcome(report.outcome),
            checked_rows: u64_to_i64(report.metrics.checked_rows),
            missing_keys: u64_to_i64(report.metrics.missing_keys),
            extra_keys: u64_to_i64(report.metrics.extra_keys),
            mismatched_rows: u64_to_i64(report.metrics.mismatched_rows),
            stale_rows: u64_to_i64(report.metrics.stale_rows),
            privacy_violations: u64_to_i64(report.metrics.privacy_violations),
            deterministic_chunk_hash_mismatches: u64_to_i64(
                report.metrics.deterministic_chunk_hash_mismatches,
            ),
            missing_samples: usize_to_i64(report.samples.missing_keys.len()),
            extra_samples: usize_to_i64(report.samples.extra_keys.len()),
            mismatched_samples: usize_to_i64(report.samples.mismatched_keys.len()),
            stale_samples: usize_to_i64(report.samples.stale_keys.len()),
            privacy_samples: usize_to_i64(report.samples.privacy_violation_keys.len()),
        })
    }
}

fn u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).expect("MBT metric fits in i64")
}

fn usize_to_i64(value: usize) -> i64 {
    i64::try_from(value).expect("MBT sample count fits in i64")
}

fn rust_strategy(strategy: MbtCheckStrategy) -> CheckComparisonStrategy {
    match strategy {
        MbtCheckStrategy::CountsOnly => CheckComparisonStrategy::CountsOnly,
        MbtCheckStrategy::KeySet => CheckComparisonStrategy::KeySet,
        MbtCheckStrategy::ChunkHashes => CheckComparisonStrategy::ChunkHashes,
        MbtCheckStrategy::FullRows => CheckComparisonStrategy::FullRows,
    }
}

fn rust_sample_limit(sample_limit: SampleLimit) -> usize {
    match sample_limit {
        SampleLimit::OneSample => 1,
        SampleLimit::TwoSamples => 2,
    }
}

fn mbt_outcome(outcome: crate::CheckOutcome) -> MbtCheckOutcome {
    match outcome {
        crate::CheckOutcome::Complete => MbtCheckOutcome::Complete,
        crate::CheckOutcome::Incomplete => MbtCheckOutcome::Incomplete,
        crate::CheckOutcome::Mismatched => MbtCheckOutcome::MismatchedOutcome,
        crate::CheckOutcome::Stale => MbtCheckOutcome::StaleOutcome,
        crate::CheckOutcome::PrivacyViolation => MbtCheckOutcome::PrivacyViolationOutcome,
    }
}

fn check_datasets(check_case: CheckCase) -> (LocalCheckDataset, LocalCheckDataset) {
    match check_case {
        CheckCase::Clean => (
            LocalCheckDataset::new(vec![row("a", 11, 5, false), row("b", 22, 6, false)]),
            LocalCheckDataset::new(vec![row("a", 11, 5, false), row("b", 22, 6, false)]),
        ),
        CheckCase::Missing => (
            LocalCheckDataset::new(vec![row("a", 11, 5, false), row("b", 22, 6, false)]),
            LocalCheckDataset::new(vec![row("a", 11, 5, false)]),
        ),
        CheckCase::Extra => (
            LocalCheckDataset::new(vec![row("a", 11, 5, false)]),
            LocalCheckDataset::new(vec![row("a", 11, 5, false), row("b", 22, 6, false)]),
        ),
        CheckCase::MissingExtra => (
            LocalCheckDataset::new(vec![row("a", 11, 5, false)]),
            LocalCheckDataset::new(vec![row("b", 22, 6, false)]),
        ),
        CheckCase::Mismatched => (
            LocalCheckDataset::new(vec![row("a", 11, 5, false), row("b", 22, 6, false)]),
            LocalCheckDataset::new(vec![row("a", 99, 5, false), row("b", 88, 6, false)]),
        ),
        CheckCase::Stale => (
            LocalCheckDataset::new(vec![row("a", 11, 5, false)]),
            LocalCheckDataset::new(vec![row("a", 11, 4, false)]),
        ),
        CheckCase::PrivacyViolation => (
            LocalCheckDataset::new(vec![row("a", 11, 5, false)]),
            LocalCheckDataset::new(vec![row("a", 11, 5, true)]),
        ),
    }
}

fn row(key: &str, row_hash: u64, source_position: u64, contains_private_data: bool) -> CheckRow {
    CheckRow {
        table: "users".to_string(),
        key: key.to_string(),
        row_hash,
        source_position,
        contains_private_data,
    }
}
