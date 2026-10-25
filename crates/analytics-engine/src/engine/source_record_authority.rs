use analytics_contract::SourcePosition;

use super::SourceRecordMutationPlan;

/// An ownership plan always carries the position that authorized it. The batch
/// owns these values; row mutation requests borrow the same authority.
#[derive(Clone, Copy, Default)]
pub(crate) enum SourceRecordAuthority<Position, Plan> {
    #[default]
    Unpositioned,
    Positioned(Position),
    Owned {
        position: Position,
        plan: Plan,
    },
}

impl<Position, Plan> SourceRecordAuthority<Position, Plan> {
    pub(super) fn borrow(&self) -> SourceRecordAuthority<&Position, &Plan> {
        match self {
            Self::Unpositioned => SourceRecordAuthority::Unpositioned,
            Self::Positioned(position) => SourceRecordAuthority::Positioned(position),
            Self::Owned { position, plan } => SourceRecordAuthority::Owned { position, plan },
        }
    }
}

impl<'a> SourceRecordAuthority<&'a SourcePosition, &'a SourceRecordMutationPlan> {
    pub(super) fn position(self) -> Option<&'a SourcePosition> {
        match self {
            Self::Unpositioned => None,
            Self::Positioned(position) | Self::Owned { position, .. } => Some(position),
        }
    }

    pub(super) fn owner(self) -> Option<(&'a SourcePosition, &'a SourceRecordMutationPlan)> {
        match self {
            Self::Owned { position, plan } => Some((position, plan)),
            Self::Unpositioned | Self::Positioned(_) => None,
        }
    }
}
