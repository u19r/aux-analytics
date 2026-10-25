use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum BackfillSourceKind {
    DynamoDb,
    AuxStorage,
    RawBackup,
    LocalFixture,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct BackfillSourceCapabilities {
    pub source_kind: BackfillSourceKind,
    pub stream_retention_hours: Option<u16>,
    pub supports_pitr_export: bool,
    pub supports_parallel_scan: bool,
    pub supports_full_enumeration: bool,
    pub supports_raw_replay: bool,
    pub can_prove_deletes: bool,
}

impl BackfillSourceCapabilities {
    #[must_use]
    pub const fn dynamodb_export() -> Self {
        Self {
            source_kind: BackfillSourceKind::DynamoDb,
            stream_retention_hours: Some(24),
            supports_pitr_export: true,
            supports_parallel_scan: true,
            supports_full_enumeration: false,
            supports_raw_replay: false,
            can_prove_deletes: false,
        }
    }

    #[must_use]
    pub const fn aux_storage() -> Self {
        Self {
            source_kind: BackfillSourceKind::AuxStorage,
            stream_retention_hours: None,
            supports_pitr_export: false,
            supports_parallel_scan: false,
            supports_full_enumeration: true,
            supports_raw_replay: false,
            can_prove_deletes: true,
        }
    }

    #[must_use]
    pub const fn raw_backup() -> Self {
        Self {
            source_kind: BackfillSourceKind::RawBackup,
            stream_retention_hours: None,
            supports_pitr_export: false,
            supports_parallel_scan: false,
            supports_full_enumeration: false,
            supports_raw_replay: true,
            can_prove_deletes: false,
        }
    }

    #[must_use]
    pub const fn local_fixture() -> Self {
        Self {
            source_kind: BackfillSourceKind::LocalFixture,
            stream_retention_hours: None,
            supports_pitr_export: false,
            supports_parallel_scan: false,
            supports_full_enumeration: true,
            supports_raw_replay: false,
            can_prove_deletes: true,
        }
    }
}

pub trait BackfillCapabilityDiscovery {
    fn discover(
        &self,
        source_kind: BackfillSourceKind,
    ) -> BackfillPlanResult<BackfillSourceCapabilities>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynamoDbBackfillMetadata {
    pub metadata_reachable: bool,
    pub pitr_enabled: bool,
    pub scan_enabled: bool,
    pub stream_retention_hours: Option<u16>,
}

pub trait DynamoDbBackfillMetadataAdapter {
    fn discover_dynamodb_metadata(&self) -> BackfillPlanResult<DynamoDbBackfillMetadata>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuxStorageBackfillMetadata {
    pub metadata_reachable: bool,
    pub enumeration_enabled: bool,
    pub can_prove_deletes: bool,
}

pub trait AuxStorageBackfillMetadataAdapter {
    fn discover_aux_storage_metadata(&self) -> BackfillPlanResult<AuxStorageBackfillMetadata>;
}

#[derive(Debug, Clone)]
pub struct BackfillDiscoveryAdapters<DynamoDbAdapter, AuxStorageAdapter> {
    dynamodb: DynamoDbAdapter,
    aux_storage: AuxStorageAdapter,
}

impl<DynamoDbAdapter, AuxStorageAdapter>
    BackfillDiscoveryAdapters<DynamoDbAdapter, AuxStorageAdapter>
{
    #[must_use]
    pub const fn new(dynamodb: DynamoDbAdapter, aux_storage: AuxStorageAdapter) -> Self {
        Self {
            dynamodb,
            aux_storage,
        }
    }
}

impl<DynamoDbAdapter, AuxStorageAdapter> BackfillCapabilityDiscovery
    for BackfillDiscoveryAdapters<DynamoDbAdapter, AuxStorageAdapter>
where
    DynamoDbAdapter: DynamoDbBackfillMetadataAdapter,
    AuxStorageAdapter: AuxStorageBackfillMetadataAdapter,
{
    fn discover(
        &self,
        source_kind: BackfillSourceKind,
    ) -> BackfillPlanResult<BackfillSourceCapabilities> {
        match source_kind {
            BackfillSourceKind::DynamoDb => self.dynamodb.discover_dynamodb_metadata()?.try_into(),
            BackfillSourceKind::AuxStorage => {
                self.aux_storage.discover_aux_storage_metadata()?.try_into()
            }
            BackfillSourceKind::RawBackup => Ok(BackfillSourceCapabilities::raw_backup()),
            BackfillSourceKind::LocalFixture => Ok(BackfillSourceCapabilities::local_fixture()),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StaticBackfillCapabilityDiscovery;

impl BackfillCapabilityDiscovery for StaticBackfillCapabilityDiscovery {
    fn discover(
        &self,
        source_kind: BackfillSourceKind,
    ) -> BackfillPlanResult<BackfillSourceCapabilities> {
        Ok(match source_kind {
            BackfillSourceKind::DynamoDb => BackfillSourceCapabilities::dynamodb_export(),
            BackfillSourceKind::AuxStorage => BackfillSourceCapabilities::aux_storage(),
            BackfillSourceKind::RawBackup => BackfillSourceCapabilities::raw_backup(),
            BackfillSourceKind::LocalFixture => BackfillSourceCapabilities::local_fixture(),
        })
    }
}

impl TryFrom<DynamoDbBackfillMetadata> for BackfillSourceCapabilities {
    type Error = BackfillPlanError;

    fn try_from(metadata: DynamoDbBackfillMetadata) -> BackfillPlanResult<Self> {
        if !metadata.metadata_reachable {
            return Err(BackfillPlanError::PartialCapabilitySupport {
                source_kind: BackfillSourceKind::DynamoDb,
                reason: "DynamoDB metadata is not reachable".to_string(),
            });
        }
        if !metadata.pitr_enabled && !metadata.scan_enabled {
            return Err(BackfillPlanError::PartialCapabilitySupport {
                source_kind: BackfillSourceKind::DynamoDb,
                reason: "neither PITR export nor parallel scan is available".to_string(),
            });
        }
        Ok(Self {
            source_kind: BackfillSourceKind::DynamoDb,
            stream_retention_hours: metadata.stream_retention_hours.or(Some(24)),
            supports_pitr_export: metadata.pitr_enabled,
            supports_parallel_scan: metadata.scan_enabled,
            supports_full_enumeration: false,
            supports_raw_replay: false,
            can_prove_deletes: false,
        })
    }
}

impl TryFrom<AuxStorageBackfillMetadata> for BackfillSourceCapabilities {
    type Error = BackfillPlanError;

    fn try_from(metadata: AuxStorageBackfillMetadata) -> BackfillPlanResult<Self> {
        if !metadata.metadata_reachable {
            return Err(BackfillPlanError::PartialCapabilitySupport {
                source_kind: BackfillSourceKind::AuxStorage,
                reason: "aux-storage metadata is not reachable".to_string(),
            });
        }
        if !metadata.enumeration_enabled {
            return Err(BackfillPlanError::PartialCapabilitySupport {
                source_kind: BackfillSourceKind::AuxStorage,
                reason: "full enumeration is not available".to_string(),
            });
        }
        Ok(Self {
            source_kind: BackfillSourceKind::AuxStorage,
            stream_retention_hours: None,
            supports_pitr_export: false,
            supports_parallel_scan: false,
            supports_full_enumeration: true,
            supports_raw_replay: false,
            can_prove_deletes: metadata.can_prove_deletes,
        })
    }
}
