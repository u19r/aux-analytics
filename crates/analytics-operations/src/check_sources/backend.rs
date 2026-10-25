use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckBackendKind {
    LocalFixture,
    AuxStorage,
    DynamoDb,
    DuckDb,
    DuckLake,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CheckBackendSupport {
    Supported,
    Partial { reason: String },
    Unsupported { reason: String },
}

impl CheckBackendSupport {
    #[must_use]
    pub fn is_supported(&self) -> bool {
        matches!(self, Self::Supported)
    }

    pub(super) fn ensure_supported(&self, kind: CheckBackendKind) -> CheckResult<()> {
        match self {
            Self::Supported => Ok(()),
            Self::Partial { reason } => Err(CheckError::PartiallySupportedBackend {
                backend: kind,
                reason: reason.clone(),
            }),
            Self::Unsupported { reason } => Err(CheckError::UnsupportedBackend {
                backend: kind,
                reason: reason.clone(),
            }),
        }
    }
}

pub trait CheckRowSource {
    fn backend_kind(&self) -> CheckBackendKind;
    fn support(&self) -> CheckBackendSupport;
    fn rows(&self, target_tables: &[String]) -> CheckResult<Vec<CheckRow>>;

    fn row_stream(&self, target_tables: &[String]) -> CheckResult<CheckRowStream> {
        Ok(CheckRowStream::from_unsorted(self.rows(target_tables)?))
    }
}
