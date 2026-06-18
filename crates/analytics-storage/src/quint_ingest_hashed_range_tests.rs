use quint_connect::{Driver, Result, State, Step, switch};
use serde::Deserialize;

use crate::{
    ChangeVersionstamp, LeaseToken, MarkerScanDecision, ProcessorGeneration, ProcessorId,
    ProcessorMember, ProcessorMode, ProgressDecision, SlotId, SlotLeaseAction, SlotLeaseState,
    TrimDecision, marker_scan_decision, owned_slots, plan_slot_leases, progress_decision,
    trim_decision,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum AssignmentResult {
    AssignmentNotChecked,
    OwnsAllSlots,
    OwnsSomeSlots,
    SkipsOwnership,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum ProgressResult {
    ProgressNotChecked,
    AdvancesProgress,
    BlocksProgress,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum TrimResult {
    TrimNotChecked,
    AllowsTrim,
    BlocksTrim,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum MarkerScanResult {
    MarkerScanNotChecked,
    PollsChangedTable,
    SkipsUnchangedTable,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "tag", content = "value")]
enum LeasePlanResult {
    LeasePlanNotChecked,
    AcquiresMissingLease,
    RenewsHeldLease,
    ReleasesUnownedLease,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct IngestHashedRangeState {
    #[serde(rename = "lastAssignment")]
    last_assignment: AssignmentResult,
    #[serde(rename = "lastProgress")]
    last_progress: ProgressResult,
    #[serde(rename = "lastTrim")]
    last_trim: TrimResult,
    #[serde(rename = "lastMarkerScan")]
    last_marker_scan: MarkerScanResult,
    #[serde(rename = "lastLeasePlan")]
    last_lease_plan: LeasePlanResult,
}

impl State<IngestHashedRangeDriver> for IngestHashedRangeState {
    fn from_driver(driver: &IngestHashedRangeDriver) -> Result<Self> {
        Ok(Self {
            last_assignment: driver.last_assignment,
            last_progress: driver.last_progress,
            last_trim: driver.last_trim,
            last_marker_scan: driver.last_marker_scan,
            last_lease_plan: driver.last_lease_plan,
        })
    }
}

struct IngestHashedRangeDriver {
    last_assignment: AssignmentResult,
    last_progress: ProgressResult,
    last_trim: TrimResult,
    last_marker_scan: MarkerScanResult,
    last_lease_plan: LeasePlanResult,
}

impl Default for IngestHashedRangeDriver {
    fn default() -> Self {
        Self {
            last_assignment: AssignmentResult::AssignmentNotChecked,
            last_progress: ProgressResult::ProgressNotChecked,
            last_trim: TrimResult::TrimNotChecked,
            last_marker_scan: MarkerScanResult::MarkerScanNotChecked,
            last_lease_plan: LeasePlanResult::LeasePlanNotChecked,
        }
    }
}

impl Driver for IngestHashedRangeDriver {
    type State = IngestHashedRangeState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                self.last_assignment = AssignmentResult::AssignmentNotChecked;
                self.last_progress = ProgressResult::ProgressNotChecked;
                self.last_trim = TrimResult::TrimNotChecked;
                self.last_marker_scan = MarkerScanResult::MarkerScanNotChecked;
                self.last_lease_plan = LeasePlanResult::LeasePlanNotChecked;
            },
            CheckQueryOnlySkipped => self.apply_case(ProcessorMode::QueryOnly, 5, false, false, false, 0, 0, false, 0)?,
            CheckSingleProcessorOwnsAll => self.apply_case(ProcessorMode::IngestCapable, 1, true, false, false, 0, 0, false, 1)?,
            CheckMultiProcessorOwnsSome => self.apply_case(ProcessorMode::IngestCapable, 2, true, false, false, 0, 0, true, 1)?,
            CheckFailedProcessorTakeover => self.apply_case(ProcessorMode::IngestCapable, 1, true, true, true, 2, 1, true, 2)?,
            CheckProgressAdvance => self.apply_case(ProcessorMode::IngestCapable, 1, true, true, true, 2, 1, true, 2)?,
            CheckProgressBlockedWithoutLease => self.apply_case(ProcessorMode::IngestCapable, 1, false, true, true, 2, 1, true, 2)?,
            CheckTrimBlockedAheadOfProgress => self.apply_case(ProcessorMode::IngestCapable, 1, true, true, true, 1, 2, true, 1)?,
            CheckMarkerWithoutProgressPolls => self.apply_case(ProcessorMode::IngestCapable, 1, true, false, false, 0, 0, false, 1)?,
            CheckNewerMarkerPolls => self.apply_case(ProcessorMode::IngestCapable, 1, true, false, false, 1, 0, true, 2)?,
            CheckOldMarkerSkips => self.apply_case(ProcessorMode::IngestCapable, 1, true, false, false, 2, 0, true, 1)?,
            CheckLeaseAcquire => self.last_lease_plan = lease_plan_result(false, true)?,
            CheckLeaseRenew => self.last_lease_plan = lease_plan_result(true, true)?,
            CheckLeaseRelease => self.last_lease_plan = lease_plan_result(true, false)?,
        })
    }
}

#[quint_connect::quint_run(
    spec = "../../specs/ingest_hashed_range_mbt.qnt",
    max_samples = 50,
    max_steps = 8,
    seed = "0x11"
)]
fn quint_ingest_hashed_range_model_matches_assignment_and_progress_helpers() -> impl Driver {
    IngestHashedRangeDriver::default()
}

impl IngestHashedRangeDriver {
    #[expect(clippy::too_many_arguments)]
    fn apply_case(
        &mut self,
        mode: ProcessorMode,
        active_processors: usize,
        lease_held: bool,
        ingest_succeeded: bool,
        progress_saved: bool,
        progress_version: u8,
        trim_version: u8,
        progress_present: bool,
        marker_version: u8,
    ) -> Result {
        self.last_assignment = assignment_result(mode, active_processors, lease_held)?;
        self.last_progress = match progress_decision(lease_held, ingest_succeeded, progress_saved) {
            ProgressDecision::Save => ProgressResult::AdvancesProgress,
            ProgressDecision::Block => ProgressResult::BlocksProgress,
        };
        self.last_trim = match trim_decision(
            progress_saved,
            &version(progress_version),
            &version(trim_version),
        ) {
            TrimDecision::Trim => TrimResult::AllowsTrim,
            TrimDecision::Block => TrimResult::BlocksTrim,
        };
        let previous_progress_version = progress_present.then(|| version(progress_version));
        self.last_marker_scan = match marker_scan_decision(
            previous_progress_version.as_ref(),
            &version(marker_version),
        ) {
            MarkerScanDecision::Poll => MarkerScanResult::PollsChangedTable,
            MarkerScanDecision::Skip => MarkerScanResult::SkipsUnchangedTable,
        };
        self.last_lease_plan = LeasePlanResult::LeasePlanNotChecked;
        Ok(())
    }
}

fn assignment_result(
    mode: ProcessorMode,
    active_processors: usize,
    lease_held: bool,
) -> Result<AssignmentResult> {
    let local = ProcessorMember {
        processor_id: ProcessorId::new("processor-0").expect("processor id"),
        generation: ProcessorGeneration::new("generation-0").expect("generation"),
        mode,
    };
    let mut active = (0..active_processors)
        .map(|index| {
            ProcessorMember::ingest_capable(
                ProcessorId::new(format!("processor-{index}")).expect("processor id"),
                ProcessorGeneration::new(format!("generation-{index}")).expect("generation"),
            )
        })
        .collect::<Vec<_>>();
    if mode == ProcessorMode::QueryOnly {
        active.push(local.clone());
    }

    let owned = if lease_held {
        owned_slots("cluster", &local, &active, 16)
    } else {
        Default::default()
    };
    if mode == ProcessorMode::QueryOnly || !lease_held {
        return Ok(AssignmentResult::SkipsOwnership);
    }
    if active_processors == 1 && owned.len() == 16 {
        Ok(AssignmentResult::OwnsAllSlots)
    } else if active_processors > 1 && !owned.is_empty() && owned.len() < 16 {
        Ok(AssignmentResult::OwnsSomeSlots)
    } else {
        Err(anyhow::anyhow!("unexpected assignment result"))
    }
}

fn version(value: u8) -> ChangeVersionstamp {
    ChangeVersionstamp::new(format!("{value:04}")).expect("version")
}

fn lease_plan_result(held: bool, owned: bool) -> Result<LeasePlanResult> {
    let processor = ProcessorMember::ingest_capable(
        ProcessorId::new("processor-0").expect("processor id"),
        ProcessorGeneration::new("generation-0").expect("generation"),
    );
    let slot = SlotId::new(1);
    let owned_slots = owned.then_some(slot).into_iter().collect();
    let held_leases = held
        .then(|| SlotLeaseState {
            slot,
            processor_id: processor.processor_id.clone(),
            generation: processor.generation.clone(),
            lease_token: LeaseToken::new("processor-0/generation-0/1").expect("lease token"),
            lease_until_ms: 10_000,
        })
        .into_iter()
        .collect::<Vec<_>>();
    let plan = plan_slot_leases(&owned_slots, &held_leases, &processor);
    match plan.actions.as_slice() {
        [SlotLeaseAction::Acquire(action_slot)] if *action_slot == slot => {
            Ok(LeasePlanResult::AcquiresMissingLease)
        }
        [SlotLeaseAction::Renew(action_slot)] if *action_slot == slot => {
            Ok(LeasePlanResult::RenewsHeldLease)
        }
        [SlotLeaseAction::Release(action_slot)] if *action_slot == slot => {
            Ok(LeasePlanResult::ReleasesUnownedLease)
        }
        actions => Err(anyhow::anyhow!(
            "unexpected lease plan actions: {actions:?}"
        )),
    }
}
