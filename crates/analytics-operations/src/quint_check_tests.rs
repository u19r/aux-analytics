use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::{
    CheckComparisonStrategy, CheckOutputFormat, CheckRequest, CheckRow, LocalCheckDataset,
    LocalCheckExecutor, OperationActor, OperationId, OperationStore, RateLimitPolicy,
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct MbtReportState {
    operation_id: String,
    strategy: MbtCheckStrategy,
    outcome: MbtCheckOutcome,
    source_rows: i64,
    destination_rows: i64,
    checked_rows: i64,
    missing_keys: i64,
    extra_keys: i64,
    mismatched_rows: i64,
    stale_rows: i64,
    privacy_violations: i64,
    deterministic_chunk_hash_mismatches: i64,
    unsupported_comparisons: i64,
    check_duration_ms: i64,
    missing_samples: Vec<String>,
    extra_samples: Vec<String>,
    mismatched_samples: Vec<String>,
    stale_samples: Vec<String>,
    privacy_samples: Vec<String>,
}

impl Default for MbtReportState {
    fn default() -> Self {
        Self {
            operation_id: String::new(),
            strategy: MbtCheckStrategy::FullRows,
            outcome: MbtCheckOutcome::NotChecked,
            source_rows: 0,
            destination_rows: 0,
            checked_rows: 0,
            missing_keys: 0,
            extra_keys: 0,
            mismatched_rows: 0,
            stale_rows: 0,
            privacy_violations: 0,
            deterministic_chunk_hash_mismatches: 0,
            unsupported_comparisons: 0,
            check_duration_ms: 0,
            missing_samples: Vec::new(),
            extra_samples: Vec::new(),
            mismatched_samples: Vec::new(),
            stale_samples: Vec::new(),
            privacy_samples: Vec::new(),
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
            last_report: driver.last_report.clone(),
        })
    }
}

#[derive(Default)]
struct MbtCheckDriver {
    last_report: MbtReportState,
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
fn given_quint_check_model_when_replayed_then_full_report_state_matches() -> impl Driver {
    MbtCheckDriver::default()
}

impl MbtCheckDriver {
    fn run_check(
        &self,
        check_case: CheckCase,
        strategy: MbtCheckStrategy,
        sample_limit: SampleLimit,
    ) -> Result<MbtReportState> {
        let store = OperationStore::connect_in_memory()?;
        let request = CheckRequest {
            operation_id: OperationId::try_new("check_mbt")?,
            actor: OperationActor::try_new("quint")?,
            target_tables: vec!["users".to_string()],
            strategy: rust_strategy(strategy),
            sample_limit: rust_sample_limit(sample_limit),
            privacy_policy_version: Some("policy-v1".to_string()),
            output: CheckOutputFormat::Json,
            rate_limit: RateLimitPolicy::default(),
        };
        let (source, destination) = check_datasets(check_case);
        let report = LocalCheckExecutor.execute(&store, &request, &source, &destination)?;
        let operation_id = report.operation_id.as_str().to_owned();
        Ok(MbtReportState {
            operation_id,
            strategy: mbt_strategy(report.strategy),
            outcome: mbt_outcome(report.outcome),
            source_rows: u64_to_i64(report.metrics.source_rows),
            destination_rows: u64_to_i64(report.metrics.destination_rows),
            checked_rows: u64_to_i64(report.metrics.checked_rows),
            missing_keys: u64_to_i64(report.metrics.missing_keys),
            extra_keys: u64_to_i64(report.metrics.extra_keys),
            mismatched_rows: u64_to_i64(report.metrics.mismatched_rows),
            stale_rows: u64_to_i64(report.metrics.stale_rows),
            privacy_violations: u64_to_i64(report.metrics.privacy_violations),
            deterministic_chunk_hash_mismatches: u64_to_i64(
                report.metrics.deterministic_chunk_hash_mismatches,
            ),
            unsupported_comparisons: u64_to_i64(report.metrics.unsupported_comparisons),
            check_duration_ms: u64_to_i64(report.metrics.check_duration_ms),
            missing_samples: report.samples.missing_keys,
            extra_samples: report.samples.extra_keys,
            mismatched_samples: report.samples.mismatched_keys,
            stale_samples: report.samples.stale_keys,
            privacy_samples: report.samples.privacy_violation_keys,
        })
    }
}

#[test]
fn given_full_row_mismatch_when_report_is_projected_then_typed_fields_are_preserved() {
    let report = MbtCheckDriver::default()
        .run_check(
            CheckCase::Mismatched,
            MbtCheckStrategy::FullRows,
            SampleLimit::TwoSamples,
        )
        .expect("local check report");

    assert_eq!(
        report,
        MbtReportState {
            operation_id: "check_mbt".to_string(),
            strategy: MbtCheckStrategy::FullRows,
            outcome: MbtCheckOutcome::MismatchedOutcome,
            source_rows: 2,
            destination_rows: 2,
            checked_rows: 2,
            missing_keys: 0,
            extra_keys: 0,
            mismatched_rows: 2,
            stale_rows: 0,
            privacy_violations: 0,
            deterministic_chunk_hash_mismatches: 0,
            unsupported_comparisons: 0,
            check_duration_ms: 0,
            missing_samples: Vec::new(),
            extra_samples: Vec::new(),
            mismatched_samples: vec!["users:a".to_string(), "users:b".to_string()],
            stale_samples: Vec::new(),
            privacy_samples: Vec::new(),
        }
    );
}

fn u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).expect("MBT metric fits in i64")
}

fn rust_strategy(strategy: MbtCheckStrategy) -> CheckComparisonStrategy {
    match strategy {
        MbtCheckStrategy::CountsOnly => CheckComparisonStrategy::CountsOnly,
        MbtCheckStrategy::KeySet => CheckComparisonStrategy::KeySet,
        MbtCheckStrategy::ChunkHashes => CheckComparisonStrategy::ChunkHashes,
        MbtCheckStrategy::FullRows => CheckComparisonStrategy::FullRows,
    }
}

fn mbt_strategy(strategy: CheckComparisonStrategy) -> MbtCheckStrategy {
    match strategy {
        CheckComparisonStrategy::CountsOnly => MbtCheckStrategy::CountsOnly,
        CheckComparisonStrategy::KeySet => MbtCheckStrategy::KeySet,
        CheckComparisonStrategy::ChunkHashes => MbtCheckStrategy::ChunkHashes,
        CheckComparisonStrategy::FullRows => MbtCheckStrategy::FullRows,
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
        source_position: crate::check_test_support::backup_position(source_position),
        contains_private_data,
    }
}
