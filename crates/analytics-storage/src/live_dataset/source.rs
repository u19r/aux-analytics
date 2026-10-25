//! Supported application source protocol. The application owns its private data
//! service and must use these transactions for every indexed record mutation.
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use storage_types::{AttributeMap, AttributeValue};

use super::{LiveDatasetError, LiveDatasetManager};

/// An authoritative source marker read from the application's private table.
#[derive(Clone, Debug)]
pub struct ApplicationSource {
    tenant: String,
    application: String,
    incarnation: uuid::Uuid,
    sequence: u64,
}

/// A complete indexed application record. Fields cannot replace protocol keys.
#[derive(Clone, Debug)]
pub struct ApplicationRecord {
    pub id: String,
    pub status: String,
    pub event_time: u64,
    pub fields: AttributeMap,
}

/// Expected previous state makes retries and concurrent changes conditional.
#[derive(Clone, Debug)]
pub struct ApplicationMutation {
    pub previous: Option<ApplicationRecord>,
    pub next: Option<ApplicationRecord>,
}

impl ApplicationSource {
    pub fn from_marker(
        tenant: &str,
        application: &str,
        item: &AttributeMap,
    ) -> Result<Self, LiveDatasetError> {
        let expected = LiveDatasetManager::source_index_marker(tenant, application)?;
        if !expected
            .iter()
            .all(|(key, value)| item.get(key) == Some(value))
        {
            return Err(LiveDatasetError::SourceUnavailable);
        }
        let Some(AttributeValue::S(incarnation)) = item.get("source_incarnation") else {
            return Err(LiveDatasetError::SourceUnavailable);
        };
        let Some(AttributeValue::N(sequence)) = item.get("mutation_sequence") else {
            return Err(LiveDatasetError::SourceUnavailable);
        };
        let incarnation =
            uuid::Uuid::parse_str(incarnation).map_err(|_| LiveDatasetError::SourceUnavailable)?;
        if incarnation.get_version_num() != 4 {
            return Err(LiveDatasetError::SourceUnavailable);
        }
        let sequence = sequence
            .parse()
            .map_err(|_| LiveDatasetError::SourceUnavailable)?;
        Ok(Self {
            tenant: tenant.into(),
            application: application.into(),
            incarnation,
            sequence,
        })
    }

    /// Only for a newly created empty table. Never overwrite an existing
    /// marker.
    pub fn initialize_request(tenant: &str, application: &str) -> Result<Value, LiveDatasetError> {
        let table = LiveDatasetManager::source_table(tenant, application)?;
        let mut item = LiveDatasetManager::source_index_marker(tenant, application)?;
        item.insert(
            "source_incarnation",
            AttributeValue::S(uuid::Uuid::new_v4().to_string()),
        );
        item.insert("mutation_sequence", AttributeValue::N("0".into()));
        Ok(json!({"TableName":table,"Item":item,"ConditionExpression":"attribute_not_exists(pk)"}))
    }

    pub fn watermark(&self) -> String {
        format!("{}:{}", self.incarnation, self.sequence)
    }

    pub(super) fn incarnation(&self) -> String {
        self.incarnation.to_string()
    }

    pub fn marker_read_request(tenant: &str, application: &str) -> Result<Value, LiveDatasetError> {
        Ok(
            json!({"TableName":LiveDatasetManager::source_table(tenant, application)?,
            "Key":LiveDatasetManager::source_marker_key(),"ConsistentRead":true}),
        )
    }

    /// Stable request_id is a DynamoDB transaction idempotency token. A failed
    /// condition rolls back both records and marker. On conflict, read a fresh
    /// marker and retry the same logical expected-state mutation with a new
    /// token.
    pub fn transaction(
        &self,
        request_id: uuid::Uuid,
        mutations: &[ApplicationMutation],
    ) -> Result<Value, LiveDatasetError> {
        if mutations.is_empty() || mutations.len() > 25 || self.sequence == u64::MAX {
            return Err(LiveDatasetError::InvalidQuery);
        }
        let table = self.table()?;
        let mut items = Vec::new();
        for mutation in mutations {
            self.append_mutation(&table, mutation, &mut items)?;
        }
        items.push(json!({"Update":{"TableName":table,"Key":LiveDatasetManager::source_marker_key(),
            "UpdateExpression":"SET mutation_sequence = :next",
            "ConditionExpression":"source_incarnation = :incarnation AND mutation_sequence = :previous",
            "ExpressionAttributeValues":{":incarnation":{"S":self.incarnation.to_string()},
                ":previous":{"N":self.sequence.to_string()},":next":{"N":(self.sequence+1).to_string()}}}}));
        Ok(json!({"TransactItems":items,"ClientRequestToken":request_id.to_string()}))
    }

    fn table(&self) -> Result<String, LiveDatasetError> {
        Ok(LiveDatasetManager::source_table(&self.tenant, &self.application)?.to_string())
    }

    fn append_mutation(
        &self,
        table: &str,
        mutation: &ApplicationMutation,
        items: &mut Vec<Value>,
    ) -> Result<(), LiveDatasetError> {
        let record = mutation
            .next
            .as_ref()
            .or(mutation.previous.as_ref())
            .ok_or(LiveDatasetError::InvalidQuery)?;
        let locator_key = json!({"pk":{"S":format!("RECORD#{}",record.id)},"sk":{"S":"UNIQUE"}});
        let mut locator = locator_key.clone();
        let mut write = self.locator_write(table, mutation, &record.id)?;
        if let Some(previous) = &mutation.previous
            && mutation.next.as_ref().is_none_or(|next| {
                previous.status != next.status || previous.event_time != next.event_time
            })
        {
            let row = self.row(previous)?;
            items.push(json!({"Delete":{"TableName":table,"Key":{"pk":row["pk"],"sk":row["sk"]}}}));
        }
        if let Some(next) = &mutation.next {
            let row = self.row(next)?;
            locator["record_fingerprint"] = json!({"S":self.record_fingerprint(next)?});
            locator["status"] = json!({"S":next.status});
            locator["event_time"] = json!({"N":next.event_time.to_string()});
            write["Item"] = locator;
            items.push(json!({"Put":write}));
            items.push(json!({"Put":{"TableName":table,"Item":row}}));
        } else {
            write["Key"] = locator_key;
            items.push(json!({"Delete":write}));
        }
        Ok(())
    }

    fn locator_write(
        &self,
        table: &str,
        mutation: &ApplicationMutation,
        id: &str,
    ) -> Result<Value, LiveDatasetError> {
        let mut write = json!({"TableName":table});
        if let Some(previous) = &mutation.previous {
            if previous.id != id {
                return Err(LiveDatasetError::InvalidQuery);
            }
            write["ConditionExpression"] = json!("record_fingerprint = :fingerprint");
            write["ExpressionAttributeValues"] =
                json!({":fingerprint":{"S":self.record_fingerprint(previous)?}});
        } else {
            write["ConditionExpression"] = json!("attribute_not_exists(pk)");
        }
        Ok(write)
    }

    fn record_fingerprint(&self, record: &ApplicationRecord) -> Result<String, LiveDatasetError> {
        let mut value = self.row(record)?;
        value.sort_all_objects();
        let encoded = serde_json::to_vec(&value).map_err(|_| LiveDatasetError::Storage)?;
        Ok(hex::encode(Sha256::digest(encoded)))
    }

    fn row(&self, record: &ApplicationRecord) -> Result<Value, LiveDatasetError> {
        LiveDatasetManager::validate_identifier(&record.id)?;
        LiveDatasetManager::validate_identifier(&record.status)?;
        if record
            .fields
            .keys()
            .any(|key| ["pk", "sk", "id", "status", "event_time"].contains(&key))
        {
            return Err(LiveDatasetError::InvalidQuery);
        }
        let mut row = record.fields.clone();
        row.normalize_numbers_for_write();
        row.insert(
            "pk",
            AttributeValue::S(format!(
                "APPLICATION#{}#STATUS#{}",
                self.application, record.status
            )),
        );
        row.insert(
            "sk",
            AttributeValue::S(format!("{:020}#{}", record.event_time, record.id)),
        );
        row.insert("id", AttributeValue::S(record.id.clone()));
        row.insert("status", AttributeValue::S(record.status.clone()));
        row.insert(
            "event_time",
            AttributeValue::N(record.event_time.to_string()),
        );
        serde_json::to_value(row).map_err(|_| LiveDatasetError::Storage)
    }
}
