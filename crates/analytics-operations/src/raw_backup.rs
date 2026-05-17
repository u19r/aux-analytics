use std::{
    fmt,
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use analytics_contract::{PrivacyPolicy, SourcePosition, StorageStreamRecord};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{OperationId, OperationTypeError};

mod checksum;
mod codec;

use checksum::checksum_hex;
use codec::{compression_ratio_per_mille, decode_object_bytes, encode_object_bytes};

pub const RAW_BACKUP_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Error)]
pub enum RawBackupError {
    #[error(transparent)]
    OperationType(#[from] OperationTypeError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    PrivacyPolicy(#[from] analytics_contract::PrivacyPolicyError),
    #[error("raw backup object id cannot be empty")]
    EmptyObjectId,
    #[error("raw backup table name cannot be empty")]
    EmptyTableName,
    #[error("raw backup source identity cannot be empty")]
    EmptySourceIdentity,
    #[error("raw backup record key cannot be empty")]
    EmptyRecordKey,
    #[error("raw backup object must contain at least one record")]
    EmptyObject,
    #[error("raw backup encryption key id cannot be empty")]
    EmptyEncryptionKeyId,
    #[error("raw backup compression failed: {0}")]
    Compression(String),
    #[error("raw backup checksum mismatch for {object_id}: expected {expected}, actual {actual}")]
    ChecksumMismatch {
        object_id: RawBackupObjectId,
        expected: String,
        actual: String,
    },
    #[error("raw backup schema version {actual} is not supported; expected {expected}")]
    UnsupportedSchemaVersion { expected: u16, actual: u16 },
    #[error("raw backup object already exists: {0}")]
    ObjectAlreadyExists(String),
    #[error("raw backup object store error: {0}")]
    ObjectStore(String),
}

pub type RawBackupResult<T> = Result<T, RawBackupError>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct RawBackupObjectId(String);

impl RawBackupObjectId {
    pub fn new(value: impl Into<String>) -> RawBackupResult<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(RawBackupError::EmptyObjectId);
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
        Self::new(format!(
            "{}_{}_{}",
            sanitize_object_id_part(operation_id.as_str()),
            sanitize_object_id_part(source_identity),
            ordinal
        ))
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
        Self::new(value)
    }
}

impl From<RawBackupObjectId> for String {
    fn from(value: RawBackupObjectId) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RawBackupCompression {
    None,
    RunLength,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RawBackupChecksumAlgorithm {
    Fnv1a64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RawBackupEncryption {
    None,
    ExternalKey { key_id: String },
}

impl RawBackupEncryption {
    fn validate(&self) -> RawBackupResult<()> {
        match self {
            Self::ExternalKey { key_id } if key_id.trim().is_empty() => {
                Err(RawBackupError::EmptyEncryptionKeyId)
            }
            Self::None | Self::ExternalKey { .. } => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawBackupRecord {
    pub table_name: String,
    pub record_key: String,
    pub source_position: Option<SourcePosition>,
    pub record: StorageStreamRecord,
}

impl RawBackupRecord {
    pub fn new(
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

    fn validate(&self) -> RawBackupResult<()> {
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
    fn validate_schema_version(&self) -> RawBackupResult<()> {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    pub fn new(
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
        let mut policy_drops = 0_u64;
        let mut records = Vec::with_capacity(self.records.len());
        for record in self.records {
            let filtered = filter_raw_backup_record(record, policy, &mut policy_drops)?;
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

    fn validate(&self) -> RawBackupResult<()> {
        if self.source_identity.trim().is_empty() {
            return Err(RawBackupError::EmptySourceIdentity);
        }
        if self.records.is_empty() {
            return Err(RawBackupError::EmptyObject);
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawBackupWriteReport {
    pub index: RawBackupObjectIndex,
    pub metrics: RawBackupMetrics,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawBackupReplayReport {
    pub records: Vec<RawBackupReplayRecord>,
    pub metrics: RawBackupMetrics,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawBackupVerifyReport {
    pub object_id: RawBackupObjectId,
    pub checksum_valid: bool,
    pub expected_checksum: String,
    pub actual_checksum: String,
    pub byte_count: u64,
    pub metrics: RawBackupMetrics,
}

pub trait RawBackupObjectStore {
    fn write_object_report(
        &self,
        request: &RawBackupWriteRequest,
    ) -> RawBackupResult<RawBackupWriteReport>;

    fn verify_object_report(
        &self,
        index: &RawBackupObjectIndex,
    ) -> RawBackupResult<RawBackupVerifyReport>;

    fn replay_object_report(
        &self,
        index: &RawBackupObjectIndex,
    ) -> RawBackupResult<RawBackupReplayReport>;

    fn read_index(&self, object_id: &RawBackupObjectId) -> RawBackupResult<RawBackupObjectIndex>;
}

pub trait S3CompatibleObjectClient {
    fn put_object_if_absent(&self, key: &str, bytes: &[u8]) -> RawBackupResult<()>;
    fn get_object(&self, key: &str) -> RawBackupResult<Vec<u8>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3CompatibleRawBackupStore<Client> {
    bucket: String,
    prefix: String,
    client: Client,
}

impl<Client> S3CompatibleRawBackupStore<Client> {
    pub fn new(
        bucket: impl Into<String>,
        prefix: impl Into<String>,
        client: Client,
    ) -> RawBackupResult<Self> {
        let bucket = bucket.into();
        if bucket.trim().is_empty() {
            return Err(RawBackupError::ObjectStore(
                "raw backup S3 bucket cannot be empty".to_string(),
            ));
        }
        Ok(Self {
            bucket,
            prefix: normalize_s3_prefix(prefix.into().as_str()),
            client,
        })
    }

    fn object_key(&self, object_id: &RawBackupObjectId) -> String {
        self.key(format!("{}.jsonl", object_id.as_str()).as_str())
    }

    fn index_key(&self, object_id: &RawBackupObjectId) -> String {
        self.key(format!("{}.index.json", object_id.as_str()).as_str())
    }

    fn key(&self, suffix: &str) -> String {
        if self.prefix.is_empty() {
            suffix.to_string()
        } else {
            format!("{}/{}", self.prefix, suffix)
        }
    }

    fn uri(&self, key: &str) -> String {
        format!("s3://{}/{}", self.bucket, key)
    }
}

impl<Client> RawBackupObjectStore for S3CompatibleRawBackupStore<Client>
where Client: S3CompatibleObjectClient
{
    fn write_object_report(
        &self,
        request: &RawBackupWriteRequest,
    ) -> RawBackupResult<RawBackupWriteReport> {
        request.validate()?;
        let encoded = encode_records(request)?;
        let checksum = checksum_hex(encoded.bytes.as_slice());
        let object_key = self.object_key(&request.object_id);
        let index_key = self.index_key(&request.object_id);
        let index = RawBackupObjectIndex {
            object_id: request.object_id.clone(),
            operation_id: request.operation_id.clone(),
            source_identity: request.source_identity.clone(),
            object_path: self.uri(object_key.as_str()),
            index_path: self.uri(index_key.as_str()),
            layout: request.layout.clone(),
            record_count: request.records.len() as u64,
            byte_count: encoded.bytes.len() as u64,
            uncompressed_byte_count: encoded.uncompressed_len as u64,
            checksum,
            privacy_policy_version: request.privacy_policy_version.clone(),
            compacted_from_object_ids: request.compacted_from_object_ids.clone(),
        };
        self.client
            .put_object_if_absent(object_key.as_str(), encoded.bytes.as_slice())?;
        let index_bytes = serde_json::to_vec_pretty(&index)?;
        self.client
            .put_object_if_absent(index_key.as_str(), index_bytes.as_slice())?;
        Ok(RawBackupWriteReport {
            metrics: RawBackupMetrics {
                bytes_written: encoded.bytes.len() as u64,
                records_written: request.records.len() as u64,
                compression_ratio_per_mille: compression_ratio_per_mille(
                    encoded.bytes.len(),
                    encoded.uncompressed_len,
                ),
                policy_drops: request.policy_dropped_records,
                ..RawBackupMetrics::default()
            },
            index,
        })
    }

    fn verify_object_report(
        &self,
        index: &RawBackupObjectIndex,
    ) -> RawBackupResult<RawBackupVerifyReport> {
        verify_object_bytes(
            index,
            self.client
                .get_object(self.object_key(&index.object_id).as_str())?
                .as_slice(),
        )
    }

    fn replay_object_report(
        &self,
        index: &RawBackupObjectIndex,
    ) -> RawBackupResult<RawBackupReplayReport> {
        let bytes = self
            .client
            .get_object(self.object_key(&index.object_id).as_str())?;
        let verify = verify_object_bytes(index, bytes.as_slice())?;
        if !verify.checksum_valid {
            return Err(RawBackupError::ChecksumMismatch {
                object_id: index.object_id.clone(),
                expected: index.checksum.clone(),
                actual: verify.actual_checksum,
            });
        }
        replay_verified_bytes(index, bytes.as_slice())
    }

    fn read_index(&self, object_id: &RawBackupObjectId) -> RawBackupResult<RawBackupObjectIndex> {
        Ok(serde_json::from_slice(
            self.client
                .get_object(self.index_key(object_id).as_str())?
                .as_slice(),
        )?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilesystemRawBackupStore {
    root: PathBuf,
}

impl FilesystemRawBackupStore {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn write_object(
        &self,
        request: &RawBackupWriteRequest,
    ) -> RawBackupResult<RawBackupObjectIndex> {
        Ok(self.write_object_report(request)?.index)
    }

    pub fn write_object_report(
        &self,
        request: &RawBackupWriteRequest,
    ) -> RawBackupResult<RawBackupWriteReport> {
        request.validate()?;
        fs::create_dir_all(self.root.as_path())?;
        let object_path = self.object_path(&request.object_id);
        let index_path = self.index_path(&request.object_id);
        let encoded = encode_records(request)?;
        let checksum = checksum_hex(encoded.bytes.as_slice());
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(object_path.as_path())?;
        file.write_all(encoded.bytes.as_slice())?;
        file.sync_all()?;
        let index = RawBackupObjectIndex {
            object_id: request.object_id.clone(),
            operation_id: request.operation_id.clone(),
            source_identity: request.source_identity.clone(),
            object_path: object_path.to_string_lossy().into_owned(),
            index_path: index_path.to_string_lossy().into_owned(),
            layout: request.layout.clone(),
            record_count: request.records.len() as u64,
            byte_count: encoded.bytes.len() as u64,
            uncompressed_byte_count: encoded.uncompressed_len as u64,
            checksum,
            privacy_policy_version: request.privacy_policy_version.clone(),
            compacted_from_object_ids: request.compacted_from_object_ids.clone(),
        };
        let index_bytes = serde_json::to_vec_pretty(&index)?;
        let mut index_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(index_path.as_path())?;
        index_file.write_all(&index_bytes)?;
        index_file.sync_all()?;
        Ok(RawBackupWriteReport {
            metrics: RawBackupMetrics {
                bytes_written: encoded.bytes.len() as u64,
                records_written: request.records.len() as u64,
                compression_ratio_per_mille: compression_ratio_per_mille(
                    encoded.bytes.len(),
                    encoded.uncompressed_len,
                ),
                policy_drops: request.policy_dropped_records,
                ..RawBackupMetrics::default()
            },
            index,
        })
    }

    pub fn replay_object(
        &self,
        index: &RawBackupObjectIndex,
    ) -> RawBackupResult<Vec<RawBackupReplayRecord>> {
        Ok(self.replay_object_report(index)?.records)
    }

    pub fn replay_object_report(
        &self,
        index: &RawBackupObjectIndex,
    ) -> RawBackupResult<RawBackupReplayReport> {
        let (bytes, verify) = Self::read_verified_object(index)?;
        if !verify.checksum_valid {
            return Err(RawBackupError::ChecksumMismatch {
                object_id: index.object_id.clone(),
                expected: index.checksum.clone(),
                actual: verify.actual_checksum,
            });
        }
        replay_verified_bytes(index, bytes.as_slice())
    }

    pub fn replay_object_report_with_privacy_policy(
        &self,
        index: &RawBackupObjectIndex,
        policy: &PrivacyPolicy,
    ) -> RawBackupResult<RawBackupReplayReport> {
        let replay = self.replay_object_report(index)?;
        let mut policy_drops = 0_u64;
        let mut records = Vec::with_capacity(replay.records.len());
        for record in replay.records {
            records.push(filter_raw_backup_replay_record(
                record,
                policy,
                &mut policy_drops,
            )?);
        }
        Ok(RawBackupReplayReport {
            metrics: RawBackupMetrics {
                replayed_records: records.len() as u64,
                policy_drops,
                ..replay.metrics
            },
            records,
        })
    }

    pub fn verify_object_report(
        &self,
        index: &RawBackupObjectIndex,
    ) -> RawBackupResult<RawBackupVerifyReport> {
        Ok(Self::read_verified_object(index)?.1)
    }

    pub fn read_index(
        &self,
        object_id: &RawBackupObjectId,
    ) -> RawBackupResult<RawBackupObjectIndex> {
        Ok(serde_json::from_slice(
            fs::read(self.index_path(object_id))?.as_slice(),
        )?)
    }

    pub fn delete_object(&self, index: &RawBackupObjectIndex) -> RawBackupResult<()> {
        fs::remove_file(Path::new(index.object_path.as_str()))?;
        fs::remove_file(Path::new(index.index_path.as_str()))?;
        Ok(())
    }

    pub fn quarantine_object(
        &self,
        index: &RawBackupObjectIndex,
        quarantine_store: &FilesystemRawBackupStore,
    ) -> RawBackupResult<RawBackupObjectIndex> {
        let quarantine_object_id =
            RawBackupObjectId::new(format!("quarantine_{}", index.object_id.as_str()))?;
        fs::create_dir_all(quarantine_store.root.as_path())?;
        let object_path = quarantine_store.object_path(&quarantine_object_id);
        let index_path = quarantine_store.index_path(&quarantine_object_id);
        fs::copy(Path::new(index.object_path.as_str()), object_path.as_path())?;
        let mut quarantine_index = index.clone();
        quarantine_index.object_id = quarantine_object_id;
        quarantine_index.object_path = object_path.to_string_lossy().into_owned();
        quarantine_index.index_path = index_path.to_string_lossy().into_owned();
        let index_bytes = serde_json::to_vec_pretty(&quarantine_index)?;
        let mut index_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(index_path.as_path())?;
        index_file.write_all(&index_bytes)?;
        index_file.sync_all()?;
        Ok(quarantine_index)
    }

    pub fn compact_objects(
        &self,
        operation_id: OperationId,
        object_id: RawBackupObjectId,
        source_identity: impl Into<String>,
        privacy_policy_version: Option<String>,
        indexes: &[RawBackupObjectIndex],
    ) -> RawBackupResult<RawBackupWriteReport> {
        let mut records = Vec::new();
        for index in indexes {
            records.extend(
                self.replay_object(index)?
                    .into_iter()
                    .map(|record| RawBackupRecord {
                        table_name: record.table_name,
                        record_key: String::from_utf8_lossy(record.record_key.as_slice())
                            .into_owned(),
                        source_position: record.source_position,
                        record: record.record,
                    }),
            );
        }
        let source_ids = indexes
            .iter()
            .map(|index| index.object_id.clone())
            .collect();
        let request = RawBackupWriteRequest::new(
            operation_id,
            object_id,
            source_identity,
            privacy_policy_version,
            records,
        )?
        .compacted_from(source_ids);
        self.write_object_report(&request)
    }

    fn object_path(&self, object_id: &RawBackupObjectId) -> PathBuf {
        self.root.join(format!("{}.jsonl", object_id.as_str()))
    }

    fn index_path(&self, object_id: &RawBackupObjectId) -> PathBuf {
        self.root.join(format!("{}.index.json", object_id.as_str()))
    }

    fn read_verified_object(
        index: &RawBackupObjectIndex,
    ) -> RawBackupResult<(Vec<u8>, RawBackupVerifyReport)> {
        index.validate_schema_version()?;
        let object_path = Path::new(index.object_path.as_str());
        let bytes = fs::read(object_path)?;
        let report = verify_object_bytes(index, bytes.as_slice())?;
        Ok((bytes, report))
    }
}

impl RawBackupObjectStore for FilesystemRawBackupStore {
    fn write_object_report(
        &self,
        request: &RawBackupWriteRequest,
    ) -> RawBackupResult<RawBackupWriteReport> {
        FilesystemRawBackupStore::write_object_report(self, request)
    }

    fn replay_object_report(
        &self,
        index: &RawBackupObjectIndex,
    ) -> RawBackupResult<RawBackupReplayReport> {
        FilesystemRawBackupStore::replay_object_report(self, index)
    }

    fn verify_object_report(
        &self,
        index: &RawBackupObjectIndex,
    ) -> RawBackupResult<RawBackupVerifyReport> {
        FilesystemRawBackupStore::verify_object_report(self, index)
    }

    fn read_index(&self, object_id: &RawBackupObjectId) -> RawBackupResult<RawBackupObjectIndex> {
        FilesystemRawBackupStore::read_index(self, object_id)
    }
}

struct EncodedRawBackupObject {
    bytes: Vec<u8>,
    uncompressed_len: usize,
}

fn encode_records(request: &RawBackupWriteRequest) -> RawBackupResult<EncodedRawBackupObject> {
    let mut encoded = Vec::new();
    for record in &request.records {
        serde_json::to_writer(&mut encoded, record)?;
        encoded.push(b'\n');
    }
    let uncompressed_len = encoded.len();
    Ok(EncodedRawBackupObject {
        bytes: encode_object_bytes(&encoded, request.layout.compression),
        uncompressed_len,
    })
}

fn verify_object_bytes(
    index: &RawBackupObjectIndex,
    bytes: &[u8],
) -> RawBackupResult<RawBackupVerifyReport> {
    index.validate_schema_version()?;
    let actual_checksum = checksum_hex(bytes);
    let checksum_valid = actual_checksum == index.checksum;
    Ok(RawBackupVerifyReport {
        object_id: index.object_id.clone(),
        checksum_valid,
        expected_checksum: index.checksum.clone(),
        actual_checksum,
        byte_count: bytes.len() as u64,
        metrics: RawBackupMetrics {
            checksum_failures: u64::from(!checksum_valid),
            ..RawBackupMetrics::default()
        },
    })
}

fn replay_verified_bytes(
    index: &RawBackupObjectIndex,
    bytes: &[u8],
) -> RawBackupResult<RawBackupReplayReport> {
    index.validate_schema_version()?;
    let mut records = Vec::new();
    let decoded = decode_object_bytes(bytes, index.layout.compression)?;
    let reader = BufReader::new(decoded.as_slice());
    for line in reader.lines() {
        let record: RawBackupRecord = serde_json::from_str(line?.as_str())?;
        record.validate()?;
        records.push(RawBackupReplayRecord {
            table_name: record.table_name,
            record_key: record.record_key.into_bytes(),
            source_position: record.source_position,
            record: record.record,
        });
    }
    Ok(RawBackupReplayReport {
        metrics: RawBackupMetrics {
            replayed_records: records.len() as u64,
            ..RawBackupMetrics::default()
        },
        records,
    })
}

fn normalize_s3_prefix(prefix: &str) -> String {
    prefix.trim_matches('/').to_string()
}

fn sanitize_object_id_part(value: &str) -> String {
    value
        .chars()
        .map(|value| {
            if value.is_ascii_alphanumeric() || value == '-' || value == '_' {
                value
            } else {
                '_'
            }
        })
        .collect()
}

fn filter_raw_backup_record(
    mut record: RawBackupRecord,
    policy: &PrivacyPolicy,
    policy_drops: &mut u64,
) -> RawBackupResult<RawBackupRecord> {
    let keys = policy.filter_item(&record.record.keys)?;
    *policy_drops += keys.dropped_fields;
    record.record.keys = keys.item;
    if let Some(old_image) = &record.record.old_image {
        let filtered = policy.filter_item(old_image)?;
        *policy_drops += filtered.dropped_fields;
        record.record.old_image = Some(filtered.item);
    }
    if let Some(new_image) = &record.record.new_image {
        let filtered = policy.filter_item(new_image)?;
        *policy_drops += filtered.dropped_fields;
        record.record.new_image = Some(filtered.item);
    }
    Ok(record)
}

fn filter_raw_backup_replay_record(
    record: RawBackupReplayRecord,
    policy: &PrivacyPolicy,
    policy_drops: &mut u64,
) -> RawBackupResult<RawBackupReplayRecord> {
    let record_key = String::from_utf8_lossy(record.record_key.as_slice()).into_owned();
    let filtered = filter_raw_backup_record(
        RawBackupRecord {
            table_name: record.table_name,
            record_key,
            source_position: record.source_position,
            record: record.record,
        },
        policy,
        policy_drops,
    )?;
    Ok(RawBackupReplayRecord {
        table_name: filtered.table_name,
        record_key: filtered.record_key.into_bytes(),
        source_position: filtered.source_position,
        record: filtered.record,
    })
}
