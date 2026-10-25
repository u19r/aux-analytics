use crate::{OperationId, PrivacyFixReport, RawBackupWriteReport, TableFixReport, TrimReport};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaintenanceExecutionReport {
    TableFix(TableFixReport),
    PrivacyFix(PrivacyFixReport),
    Trim(TrimReport),
    RawBackup(RawBackupWriteReport),
}

impl MaintenanceExecutionReport {
    #[must_use]
    pub fn operation_id(&self) -> &OperationId {
        match self {
            Self::TableFix(report) => &report.operation_id,
            Self::PrivacyFix(report) => &report.operation_id,
            Self::Trim(report) => &report.operation_id,
            Self::RawBackup(report) => &report.index.operation_id,
        }
    }
}
