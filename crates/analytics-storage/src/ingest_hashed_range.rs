use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProcessorId(String);

impl ProcessorId {
    pub fn new(value: impl Into<String>) -> Option<Self> {
        let value = value.into();
        (!value.is_empty()).then_some(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProcessorGeneration(String);

impl ProcessorGeneration {
    pub fn new(value: impl Into<String>) -> Option<Self> {
        let value = value.into();
        (!value.is_empty()).then_some(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessorMode {
    QueryOnly,
    IngestCapable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SlotId(u16);

impl SlotId {
    #[must_use]
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessorMember {
    pub processor_id: ProcessorId,
    pub generation: ProcessorGeneration,
    pub mode: ProcessorMode,
}

impl ProcessorMember {
    #[must_use]
    pub fn ingest_capable(processor_id: ProcessorId, generation: ProcessorGeneration) -> Self {
        Self {
            processor_id,
            generation,
            mode: ProcessorMode::IngestCapable,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotLeaseState {
    pub slot: SlotId,
    pub processor_id: ProcessorId,
    pub generation: ProcessorGeneration,
    pub lease_token: LeaseToken,
    pub lease_until_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseToken(String);

impl LeaseToken {
    pub fn new(value: impl Into<String>) -> Option<Self> {
        let value = value.into();
        (!value.is_empty()).then_some(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotLeaseAction {
    Acquire(SlotId),
    Renew(SlotId),
    Release(SlotId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotLeasePlan {
    pub actions: Vec<SlotLeaseAction>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ChangeVersionstamp(String);

impl ChangeVersionstamp {
    pub fn new(value: impl Into<String>) -> Option<Self> {
        let value = value.into();
        (!value.is_empty()).then_some(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProgressDecision {
    Save,
    Block,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrimDecision {
    Trim,
    Block,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarkerScanDecision {
    Poll,
    Skip,
}

#[must_use]
pub fn owned_slots(
    cluster_id: &str,
    local_processor: &ProcessorMember,
    active_members: &[ProcessorMember],
    slot_count: u16,
) -> BTreeSet<SlotId> {
    if local_processor.mode == ProcessorMode::QueryOnly {
        return BTreeSet::new();
    }

    (0..slot_count)
        .map(SlotId::new)
        .filter(|slot| {
            rendezvous_owner(cluster_id, *slot, active_members)
                .as_ref()
                .is_some_and(|owner| {
                    owner.processor_id == local_processor.processor_id
                        && owner.generation == local_processor.generation
                })
        })
        .collect()
}

#[must_use]
pub fn rendezvous_owner<'a>(
    cluster_id: &str,
    slot: SlotId,
    active_members: &'a [ProcessorMember],
) -> Option<&'a ProcessorMember> {
    active_members
        .iter()
        .filter(|member| member.mode == ProcessorMode::IngestCapable)
        .max_by_key(|member| {
            rendezvous_score(
                cluster_id,
                slot,
                member.processor_id.as_str(),
                member.generation.as_str(),
            )
        })
}

#[must_use]
pub fn plan_slot_leases(
    owned_slots: &BTreeSet<SlotId>,
    held_leases: &[SlotLeaseState],
    local_processor: &ProcessorMember,
) -> SlotLeasePlan {
    let held_by_slot = held_leases
        .iter()
        .map(|lease| (lease.slot, lease))
        .collect::<BTreeMap<_, _>>();
    let mut actions = Vec::new();

    for slot in owned_slots {
        match held_by_slot.get(slot) {
            Some(lease)
                if lease.processor_id == local_processor.processor_id
                    && lease.generation == local_processor.generation =>
            {
                actions.push(SlotLeaseAction::Renew(*slot));
            }
            _ => actions.push(SlotLeaseAction::Acquire(*slot)),
        }
    }

    for lease in held_leases {
        if !owned_slots.contains(&lease.slot)
            && lease.processor_id == local_processor.processor_id
            && lease.generation == local_processor.generation
        {
            actions.push(SlotLeaseAction::Release(lease.slot));
        }
    }

    SlotLeasePlan { actions }
}

#[must_use]
pub fn progress_decision(
    lease_held: bool,
    ingest_succeeded: bool,
    progress_save_succeeded: bool,
) -> ProgressDecision {
    if lease_held && ingest_succeeded && progress_save_succeeded {
        ProgressDecision::Save
    } else {
        ProgressDecision::Block
    }
}

#[must_use]
pub fn trim_decision(
    progress_saved: bool,
    progress_version: &ChangeVersionstamp,
    trim_version: &ChangeVersionstamp,
) -> TrimDecision {
    if progress_saved && progress_version >= trim_version {
        TrimDecision::Trim
    } else {
        TrimDecision::Block
    }
}

#[must_use]
pub fn marker_scan_decision(
    previous_progress_version: Option<&ChangeVersionstamp>,
    marker_version: &ChangeVersionstamp,
) -> MarkerScanDecision {
    match previous_progress_version {
        Some(progress_version) if progress_version >= marker_version => MarkerScanDecision::Skip,
        _ => MarkerScanDecision::Poll,
    }
}

fn rendezvous_score(cluster_id: &str, slot: SlotId, processor_id: &str, generation: &str) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET;
    for bytes in [
        cluster_id.as_bytes(),
        &slot.get().to_be_bytes(),
        processor_id.as_bytes(),
        generation.as_bytes(),
    ] {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
    }
    hash
}

#[cfg(test)]
mod tests {
    use crate::ingest_hashed_range::{
        ChangeVersionstamp, LeaseToken, MarkerScanDecision, ProcessorGeneration, ProcessorId,
        ProcessorMember, ProcessorMode, ProgressDecision, SlotId, SlotLeaseAction, SlotLeaseState,
        TrimDecision, marker_scan_decision, owned_slots, plan_slot_leases, progress_decision,
        trim_decision,
    };

    #[test]
    fn given_one_ingest_processor_when_slots_are_assigned_then_it_owns_all_slots() {
        let processor = ingest_processor("a", "g1");
        let active = vec![processor.clone()];

        let slots = owned_slots("cluster", &processor, &active, 16);

        assert_eq!(slots.len(), 16);
    }

    #[test]
    fn given_query_only_processor_when_slots_are_assigned_then_it_owns_none() {
        let processor = ProcessorMember {
            processor_id: ProcessorId::new("query").expect("processor id"),
            generation: ProcessorGeneration::new("g1").expect("generation"),
            mode: ProcessorMode::QueryOnly,
        };
        let active = vec![processor.clone(), ingest_processor("a", "g1")];

        let slots = owned_slots("cluster", &processor, &active, 16);

        assert!(slots.is_empty());
    }

    #[test]
    fn given_two_ingest_processors_when_slots_are_assigned_then_each_gets_work() {
        let first = ingest_processor("a", "g1");
        let second = ingest_processor("b", "g1");
        let active = vec![first.clone(), second.clone()];

        let first_slots = owned_slots("cluster", &first, &active, 256);
        let second_slots = owned_slots("cluster", &second, &active, 256);

        assert!(!first_slots.is_empty());
        assert!(!second_slots.is_empty());
        assert_eq!(first_slots.len() + second_slots.len(), 256);
        assert!(first_slots.is_disjoint(&second_slots));
    }

    #[test]
    fn given_owned_and_stale_leases_when_planned_then_actions_acquire_renew_and_release() {
        let processor = ingest_processor("a", "g1");
        let owned = BTreeSet::from([SlotId::new(1), SlotId::new(2)]);
        let held = vec![
            lease(1, "a", "g1"),
            lease(3, "a", "g1"),
            lease(2, "b", "g1"),
        ];

        let plan = plan_slot_leases(&owned, &held, &processor);

        assert_eq!(
            plan.actions,
            vec![
                SlotLeaseAction::Renew(SlotId::new(1)),
                SlotLeaseAction::Acquire(SlotId::new(2)),
                SlotLeaseAction::Release(SlotId::new(3)),
            ]
        );
    }

    #[test]
    fn given_failed_ingest_or_missing_lease_when_progress_is_evaluated_then_save_is_blocked() {
        assert_eq!(progress_decision(true, true, true), ProgressDecision::Save);
        assert_eq!(
            progress_decision(false, true, true),
            ProgressDecision::Block
        );
        assert_eq!(
            progress_decision(true, false, true),
            ProgressDecision::Block
        );
    }

    #[test]
    fn given_trim_version_beyond_progress_when_trim_is_evaluated_then_trim_is_blocked() {
        let progress = ChangeVersionstamp::new("0002").expect("progress version");
        let before = ChangeVersionstamp::new("0001").expect("trim version");
        let after = ChangeVersionstamp::new("0003").expect("trim version");

        assert_eq!(trim_decision(true, &progress, &before), TrimDecision::Trim);
        assert_eq!(trim_decision(true, &progress, &after), TrimDecision::Block);
        assert_eq!(
            trim_decision(false, &progress, &before),
            TrimDecision::Block
        );
    }

    #[test]
    fn given_no_progress_when_marker_is_seen_then_table_is_polled() {
        assert_eq!(
            marker_scan_decision(None, &version("0001")),
            MarkerScanDecision::Poll
        );
    }

    #[test]
    fn given_newer_marker_when_marker_is_seen_then_table_is_polled() {
        assert_eq!(
            marker_scan_decision(Some(&version("0001")), &version("0002")),
            MarkerScanDecision::Poll
        );
    }

    #[test]
    fn given_old_marker_when_marker_is_seen_then_table_is_skipped() {
        assert_eq!(
            marker_scan_decision(Some(&version("0002")), &version("0001")),
            MarkerScanDecision::Skip
        );
    }

    fn ingest_processor(id: &str, generation: &str) -> ProcessorMember {
        ProcessorMember::ingest_capable(
            ProcessorId::new(id).expect("processor id"),
            ProcessorGeneration::new(generation).expect("generation"),
        )
    }

    fn lease(slot: u16, id: &str, generation: &str) -> SlotLeaseState {
        SlotLeaseState {
            slot: SlotId::new(slot),
            processor_id: ProcessorId::new(id).expect("processor id"),
            generation: ProcessorGeneration::new(generation).expect("generation"),
            lease_token: LeaseToken::new(format!("{id}/{generation}/1")).expect("lease token"),
            lease_until_ms: 10_000,
        }
    }

    fn version(value: &str) -> ChangeVersionstamp {
        ChangeVersionstamp::new(value).expect("change version")
    }

    use std::collections::BTreeSet;
}
