use super::*;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SourceJobKind {
    AuxStorage,
    StorageStream,
}

impl SourceJobKind {
    pub(crate) const ALL: [Self; 2] = [Self::AuxStorage, Self::StorageStream];

    pub(crate) const fn from_stream_type(stream_type: config::AnalyticsStreamType) -> Self {
        match stream_type {
            config::AnalyticsStreamType::AuxStorage => Self::AuxStorage,
            config::AnalyticsStreamType::StorageStream => Self::StorageStream,
        }
    }

    pub(crate) const fn stream_type(self) -> config::AnalyticsStreamType {
        match self {
            Self::AuxStorage => config::AnalyticsStreamType::AuxStorage,
            Self::StorageStream => config::AnalyticsStreamType::StorageStream,
        }
    }

    pub(crate) const fn queue_name(self) -> &'static str {
        match self {
            Self::AuxStorage => SOURCE_AUX_QUEUE,
            Self::StorageStream => SOURCE_STREAM_QUEUE,
        }
    }

    pub(crate) const fn job_kind(self) -> &'static str {
        match self {
            Self::AuxStorage => SOURCE_AUX_JOB_KIND,
            Self::StorageStream => SOURCE_STREAM_JOB_KIND,
        }
    }
}
