use analytics_contract::SourcePosition;

use crate::CheckSourcePosition;

pub(crate) fn backup_position(offset: u64) -> CheckSourcePosition {
    CheckSourcePosition::from_source_position(SourcePosition::RawBackupObjectOffset {
        object_id: "check-fixture".to_string(),
        offset,
    })
    .unwrap()
}
