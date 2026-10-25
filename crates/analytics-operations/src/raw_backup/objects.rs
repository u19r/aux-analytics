use std::{fmt, fmt::Write as _};

use analytics_contract::{PrivacyPolicy, SourcePosition, StorageStreamRecord};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::{
    RAW_BACKUP_SCHEMA_VERSION, RawBackupError, RawBackupResult, append_sanitized_object_id_part,
    filter_raw_backup_record,
};
use crate::operation_contract::OperationId;

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema, ToSchema,
)]
#[serde(try_from = "String", into = "String")]
pub struct RawBackupObjectId(String);

impl RawBackupObjectId {
    pub fn try_new(value: impl Into<String>) -> RawBackupResult<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(RawBackupError::EmptyObjectId);
        }
        if value == "."
            || value == ".."
            || value.contains('/')
            || value.contains('\\')
            || value.chars().any(char::is_control)
        {
            return Err(RawBackupError::UnsafeObjectId);
        }
        Ok(Self(value))
    }

    pub fn for_operation(
        operation_id: &OperationId,
        source_identity: &str,
        ordinal: u64,
    ) -> RawBackupResult<Self> {
        if source_identity.trim().is_empty() {
            return Err(RawBackupError::EmptySourceIdentity);
        }
        let mut value = String::with_capacity(
            operation_id
                .as_str()
                .len()
                .saturating_add(source_identity.len())
                .saturating_add(22),
        );
        append_sanitized_object_id_part(&mut value, operation_id.as_str());
        value.push('_');
        append_sanitized_object_id_part(&mut value, source_identity);
        value.push('_');
        write!(&mut value, "{ordinal}").expect("writing an object id cannot fail");
        Self::try_new(value)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for RawBackupObjectId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl TryFrom<String> for RawBackupObjectId {
    type Error = RawBackupError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl From<RawBackupObjectId> for String {
    fn from(value: RawBackupObjectId) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RawBackupCompression {
    None,
    RunLength,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RawBackupChecksumAlgorithm {
    Fnv1a64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub enum RawBackupEncryption {
    None,
    ExternalKey { key_id: String },
}

impl RawBackupEncryption {
    pub fn validate(&self) -> RawBackupResult<()> {
        match self {
            Self::ExternalKey { key_id } if key_id.trim().is_empty() => {
                Err(RawBackupError::EmptyEncryptionKeyId)
            }
            Self::None | Self::ExternalKey { .. } => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct RawBackupObjectLayout {
    pub schema_version: u16,
    pub compression: RawBackupCompression,
    pub checksum_algorithm: RawBackupChecksumAlgorithm,
    pub encryption: RawBackupEncryption,
}

impl Default for RawBackupObjectLayout {
    fn default() -> Self {
        Self {
            schema_version: RAW_BACKUP_SCHEMA_VERSION,
            compression: RawBackupCompression::None,
            checksum_algorithm: RawBackupChecksumAlgorithm::Fnv1a64,
            encryption: RawBackupEncryption::None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct RawBackupRecord {
    pub table_name: String,
    pub record_key: String,
    pub source_position: Option<SourcePosition>,
    pub record: StorageStreamRecord,
}

impl RawBackupRecord {
    pub fn try_new(
        table_name: impl Into<String>,
        record_key: impl Into<String>,
        source_position: Option<SourcePosition>,
        record: StorageStreamRecord,
    ) -> RawBackupResult<Self> {
        let record = Self {
            table_name: table_name.into(),
            record_key: record_key.into(),
            source_position,
            record,
        };
        record.validate()?;
        Ok(record)
    }

    pub(super) fn validate(&self) -> RawBackupResult<()> {
        if self.table_name.trim().is_empty() {
            return Err(RawBackupError::EmptyTableName);
        }
        if self.record_key.trim().is_empty() {
            return Err(RawBackupError::EmptyRecordKey);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawBackupObjectIndex {
    pub object_id: RawBackupObjectId,
    pub operation_id: OperationId,
    pub source_identity: String,
    pub object_path: String,
    pub index_path: String,
    pub layout: RawBackupObjectLayout,
    pub record_count: u64,
    pub byte_count: u64,
    pub uncompressed_byte_count: u64,
    pub checksum: String,
    pub privacy_policy_version: Option<String>,
    pub compacted_from_object_ids: Vec<RawBackupObjectId>,
}

impl RawBackupObjectIndex {
    pub(super) fn validate_requested_object_id(
        &self,
        requested: &RawBackupObjectId,
    ) -> RawBackupResult<()> {
        if &self.object_id != requested {
            return Err(RawBackupError::ObjectIdMismatch {
                requested: requested.clone(),
                actual: self.object_id.clone(),
            });
        }
        Ok(())
    }

    pub(super) fn validate_schema_version(&self) -> RawBackupResult<()> {
        if self.layout.schema_version != RAW_BACKUP_SCHEMA_VERSION {
            return Err(RawBackupError::UnsupportedSchemaVersion {
                expected: RAW_BACKUP_SCHEMA_VERSION,
                actual: self.layout.schema_version,
            });
        }
        self.layout.encryption.validate()?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct RawBackupWriteRequest {
    pub operation_id: OperationId,
    pub object_id: RawBackupObjectId,
    pub source_identity: String,
    pub privacy_policy_version: Option<String>,
    pub policy_dropped_records: u64,
    pub layout: RawBackupObjectLayout,
    pub compacted_from_object_ids: Vec<RawBackupObjectId>,
    pub records: Vec<RawBackupRecord>,
}

impl RawBackupWriteRequest {
    pub fn try_new(
        operation_id: OperationId,
        object_id: RawBackupObjectId,
        source_identity: impl Into<String>,
        privacy_policy_version: Option<String>,
        records: Vec<RawBackupRecord>,
    ) -> RawBackupResult<Self> {
        let request = Self {
            operation_id,
            object_id,
            source_identity: source_identity.into(),
            privacy_policy_version,
            policy_dropped_records: 0,
            layout: RawBackupObjectLayout::default(),
            compacted_from_object_ids: Vec::new(),
            records,
        };
        request.validate()?;
        Ok(request)
    }

    #[must_use]
    pub fn with_policy_dropped_records(mut self, policy_dropped_records: u64) -> Self {
        self.policy_dropped_records = policy_dropped_records;
        self
    }

    pub fn with_privacy_policy(mut self, policy: &PrivacyPolicy) -> RawBackupResult<Self> {
        let matcher = policy.validated()?;
        let mut policy_drops = 0_u64;
        let mut records = Vec::with_capacity(self.records.len());
        for record in self.records {
            let filtered = filter_raw_backup_record(record, &matcher, &mut policy_drops)?;
            records.push(filtered);
        }
        self.privacy_policy_version = Some(policy.version.clone());
        self.policy_dropped_records += policy_drops;
        self.records = records;
        self.validate()?;
        Ok(self)
    }

    #[must_use]
    pub fn with_layout(mut self, layout: RawBackupObjectLayout) -> Self {
        self.layout = layout;
        self
    }

    #[must_use]
    pub fn compacted_from(mut self, object_ids: Vec<RawBackupObjectId>) -> Self {
        self.compacted_from_object_ids = object_ids;
        self
    }

    /// Validate the complete write request before any object-store mutation.
    pub fn validate(&self) -> RawBackupResult<()> {
        if self.source_identity.trim().is_empty() {
            return Err(RawBackupError::EmptySourceIdentity);
        }
        if self.records.is_empty() {
            return Err(RawBackupError::EmptyObject);
        }
        if self.layout.schema_version != RAW_BACKUP_SCHEMA_VERSION {
            return Err(RawBackupError::UnsupportedSchemaVersion {
                expected: RAW_BACKUP_SCHEMA_VERSION,
                actual: self.layout.schema_version,
            });
        }
        self.layout.encryption.validate()?;
        for record in &self.records {
            record.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawBackupReplayRecord {
    pub table_name: String,
    pub record_key: Vec<u8>,
    pub source_position: Option<SourcePosition>,
    pub record: StorageStreamRecord,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
pub struct RawBackupMetrics {
    pub bytes_written: u64,
    pub records_written: u64,
    pub compression_ratio_per_mille: u64,
    pub checksum_failures: u64,
    pub policy_drops: u64,
    pub replayed_records: u64,
}

impl RawBackupMetrics {
    #[must_use]
    pub fn operation_counts(&self) -> serde_json::Value {
        serde_json::json!({
            "bytes_written": self.bytes_written,
            "records_written": self.records_written,
            "compression_ratio_per_mille": self.compression_ratio_per_mille,
            "checksum_failures": self.checksum_failures,
            "policy_drops": self.policy_drops,
            "replayed_records": self.replayed_records,
        })
    }
}
