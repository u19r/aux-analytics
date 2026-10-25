use analytics_contract::{StorageItem, StorageStreamRecord};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Deserialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "record_key": "user-1",
    "record": {
        "Keys": {"pk": {"S": "USER#user-1"}},
        "SequenceNumber": "49668899999999999999999999999999999999999999999999999999",
        "NewImage": {
            "pk": {"S": "USER#user-1"},
            "profile": {"M": {"email": {"S": "ada@example.com"}}},
            "org_id": {"S": "org-a"}
        }
    }
}))]
pub struct IngestStreamRecordRequest {
    /// Optional stable row key. Omitted values use the contract sequence
    /// number, or the event id and then nested sequence number for standard
    /// event envelopes.
    #[serde(default)]
    #[schema(nullable = true, min_length = 1, max_length = 1024, example = "user-1")]
    record_key: Option<String>,
    /// Stream record in the analytics wire format or a standard stream event
    /// envelope.
    record: IngestStreamRecordPayload,
}

impl IngestStreamRecordRequest {
    #[must_use]
    pub fn into_contract_record(self) -> (String, StorageStreamRecord) {
        self.record.into_keyed_contract_record(self.record_key)
    }
}

/// Batch stream ingestion request.
#[derive(Debug, Clone, Deserialize, JsonSchema, ToSchema)]
#[schema(example = json!({
    "records": [{
        "record_key": "user-1",
        "record": {
            "Keys": {"pk": {"S": "USER#user-1"}},
            "SequenceNumber": "49668899999999999999999999999999999999999999999999999999",
            "NewImage": {
                "pk": {"S": "USER#user-1"},
                "profile": {"M": {"email": {"S": "ada@example.com"}}},
                "org_id": {"S": "org-a"}
            }
        }
    }]
}))]
pub(crate) struct IngestStreamRecordBatchRequest {
    /// Stream records from one source poll response. Keep payloads within the
    /// source service response limit, such as `DynamoDB` Streams' 1 MiB page
    /// cap.
    #[schema(min_items = 1)]
    pub(crate) records: Vec<IngestStreamRecordRequest>,
}

/// Batch stream ingestion outcome.
#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[schema(example = json!({"record_count": 2, "outcomes": [{"outcome": "inserted"}, {"outcome": "updated"}]}))]
pub(crate) struct IngestBatchResponse {
    /// Number of records accepted by the engine transaction.
    pub(crate) record_count: usize,
    /// Per-record ingest outcomes in request order.
    pub(crate) outcomes: Vec<IngestResponse>,
}

/// Accepted ingestion payload shapes.
#[derive(Debug, Clone, Deserialize, JsonSchema, ToSchema)]
#[serde(untagged)]
pub(crate) enum IngestStreamRecordPayload {
    /// Domain-agnostic analytics wire format.
    Contract(StorageStreamRecord),
    /// Standard stream event envelope containing a DynamoDB-compatible record.
    Standard(StandardStreamEventRecord),
}

impl IngestStreamRecordPayload {
    fn into_keyed_contract_record(
        self,
        record_key: Option<String>,
    ) -> (String, StorageStreamRecord) {
        let (event_id, record) = match self {
            Self::Contract(record) => (None, record),
            Self::Standard(record) => (
                record.event_id,
                StorageStreamRecord {
                    keys: record.storage.keys,
                    sequence_number: record.storage.sequence_number,
                    old_image: record.storage.old_image,
                    new_image: record.storage.new_image,
                },
            ),
        };
        let record_key = record_key
            .or(event_id)
            .unwrap_or_else(|| record.sequence_number.clone());
        (record_key, record)
    }
}

/// Standard stream event envelope.
#[derive(Debug, Clone, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(example = json!({
    "eventID": "event-user-1-insert",
    "eventName": "INSERT",
    "dynamodb": {
        "Keys": {"pk": {"S": "USER#user-1"}},
        "SequenceNumber": "49668899999999999999999999999999999999999999999999999999",
        "NewImage": {
            "pk": {"S": "USER#user-1"},
            "profile": {"M": {"email": {"S": "ada@example.com"}}}
        }
    }
}))]
pub(crate) struct StandardStreamEventRecord {
    /// Source event id used as a fallback ingestion row key.
    #[serde(default, rename = "eventID")]
    #[schema(nullable = true, default = json!(null), min_length = 1, max_length = 1024, example = "event-user-1-insert")]
    event_id: Option<String>,
    /// Source event operation name, such as INSERT, MODIFY, or REMOVE.
    #[serde(default, rename = "eventName")]
    #[schema(nullable = true, default = json!(null), example = "INSERT")]
    _event_name: Option<String>,
    /// DynamoDB-compatible stream record body.
    #[serde(rename = "dynamodb")]
    storage: StandardStreamStorageRecord,
}

/// DynamoDB-compatible stream record body.
#[derive(Debug, Clone, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "PascalCase")]
#[schema(example = json!({
    "Keys": {"pk": {"S": "USER#user-1"}},
    "SequenceNumber": "49668899999999999999999999999999999999999999999999999999",
    "NewImage": {
        "pk": {"S": "USER#user-1"},
        "profile": {"M": {"email": {"S": "ada@example.com"}}}
    }
}))]
pub(crate) struct StandardStreamStorageRecord {
    /// Primary key attributes for the source record.
    #[schema(value_type = Object)]
    #[schemars(with = "std::collections::HashMap<String, analytics_contract::StorageValue>")]
    keys: StorageItem,
    /// Source stream sequence number.
    #[schema(
        min_length = 1,
        max_length = 1024,
        example = "49668899999999999999999999999999999999999999999999999999"
    )]
    sequence_number: String,
    /// Previous record image for update and delete events.
    #[serde(default)]
    #[schema(value_type = Option<std::collections::BTreeMap<String, serde_json::Value>>, nullable = true, default = json!(null), example = json!({
        "pk": {"S": "USER#user-1"},
        "profile": {"M": {"email": {"S": "old@example.com"}}}
    }))]
    #[schemars(
        with = "Option<std::collections::HashMap<String, analytics_contract::StorageValue>>"
    )]
    old_image: Option<StorageItem>,
    /// New record image for insert and update events.
    #[serde(default)]
    #[schema(value_type = Option<std::collections::BTreeMap<String, serde_json::Value>>, nullable = true, default = json!(null), example = json!({
        "pk": {"S": "USER#user-1"},
        "profile": {"M": {"email": {"S": "ada@example.com"}}}
    }))]
    #[schemars(
        with = "Option<std::collections::HashMap<String, analytics_contract::StorageValue>>"
    )]
    new_image: Option<StorageItem>,
}

/// Stream ingestion outcome.
#[derive(Debug, Clone, Serialize, JsonSchema, ToSchema)]
#[schema(example = json!({"outcome": "inserted"}))]
pub(crate) struct IngestResponse {
    /// Final ingestion action performed by the engine.
    #[schema(example = "inserted")]
    pub outcome: String,
    /// Active privacy policy version, when privacy mode filtered this request.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = true, default = json!(null), example = "privacy-v1")]
    pub privacy_policy_version: Option<String>,
    /// Count of fields or scalar values removed by the privacy policy.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(nullable = true, default = json!(null), example = 2)]
    pub privacy_dropped_fields: Option<u64>,
}
