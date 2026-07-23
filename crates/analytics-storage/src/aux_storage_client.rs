use std::{collections::HashMap, time::Duration};

use reqwest::{
    Method, Url,
    header::{CONTENT_TYPE, HeaderMap, HeaderValue},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use storage_types::{
    AttributeDefinition, AttributeMap, AttributeValue, BillingMode, CreateTableRequest,
    GetItemRequest, GetItemResponse, GetStreamRecordsRequest, GetStreamRecordsResponse,
    KeyAttributeType, KeyAttributes, KeySchemaElement, KeyType, PutItemRequest, QueryRequest,
    QueryResponse, TableName, TransactConditionCheckRequest, TransactUpdateRequest,
    TransactWriteItem, TransactWriteItemsRequest, UpdateItemRequest, UpdateItemResponse,
};

use crate::error::{
    AnalyticsStorageError, AnalyticsStorageErrorDebug, AnalyticsStorageErrorKind,
    AnalyticsStorageResult,
};

#[derive(Debug)]
pub(crate) struct AuxStorageStreamClient {
    client: reqwest::Client,
    base_url: Url,
}

#[derive(Debug)]
pub struct AuxStorageLeaseClient {
    client: AuxStorageStreamClient,
}

#[derive(Debug)]
pub struct AuxStorageCoordinationClient {
    client: AuxStorageStreamClient,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuxStorageLeaseOutcome {
    Acquired,
    HeldByAnotherWorker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotLeaseOutcome {
    Acquired,
    HeldByAnotherProcessor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessorHeartbeat {
    pub processor_id: String,
    pub generation: String,
    pub started_at_ms: i64,
    pub last_seen_ms: i64,
    pub expires_at_ms: i64,
    pub state: ProcessorState,
    pub slot_count: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessorState {
    Active,
    Leaving,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotLease {
    pub slot: u16,
    pub processor_id: String,
    pub generation: String,
    pub lease_token: String,
    pub lease_until_ms: i64,
    pub assignment_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapLease {
    pub processor_id: String,
    pub generation: String,
    pub lease_token: String,
    pub lease_until_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceProgress {
    pub source_table_id: String,
    pub cursor: String,
    pub versionstamp: String,
    pub updated_at_ms: i64,
    pub updated_by: String,
    pub generation: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestRateRollup {
    pub source_table_id: String,
    pub tenant_id: String,
    pub window_started_at_ms: i64,
    pub updated_at_ms: i64,
    pub records_total: u64,
    pub bytes_total: u64,
    pub cursor: String,
    pub versionstamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct ChangeIndexMarker {
    pub slot: u16,
    pub versionstamp: String,
    pub table_id: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListChangeIndexMarkersRequest {
    pub slot: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after_versionstamp: Option<String>,
    pub limit: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListChangeIndexMarkersResponse {
    pub markers: Vec<ChangeIndexMarker>,
}

impl AuxStorageStreamClient {
    pub(crate) fn new(base_url: &str, timeout: Duration) -> AnalyticsStorageResult<Self> {
        let mut normalized = base_url.to_string();
        if !normalized.ends_with('/') {
            normalized.push('/');
        }
        Ok(Self {
            client: reqwest::Client::builder().timeout(timeout).build()?,
            base_url: Url::parse(&normalized)?,
        })
    }

    pub(crate) async fn get_stream_records(
        &self,
        table_name: &str,
        last_evaluated_key: Option<String>,
        limit: u32,
    ) -> AnalyticsStorageResult<GetStreamRecordsResponse> {
        self.dynamo(
            "GetStreamRecords",
            &GetStreamRecordsRequest {
                table_name: Some(TableName::new(table_name)),
                system_stream: false,
                last_evaluated_key,
                limit: Some(limit),
            },
        )
        .await
    }

    pub(crate) async fn update_item(
        &self,
        request: &UpdateItemRequest,
    ) -> AnalyticsStorageResult<UpdateItemResponse> {
        self.dynamo("UpdateItem", request).await
    }

    async fn dynamo<TRequest, TResponse>(
        &self,
        action: &str,
        request: &TRequest,
    ) -> AnalyticsStorageResult<TResponse>
    where
        TRequest: Serialize + ?Sized,
        TResponse: DeserializeOwned,
    {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-amz-json-1.0"),
        );
        headers.insert(
            "x-amz-target",
            HeaderValue::from_str(&format!("DynamoDB_20120810.{action}"))?,
        );
        self.send_json(Method::POST, "", Some(headers), Some(request))
            .await
    }

    async fn send_json<TRequest, TResponse>(
        &self,
        method: Method,
        path: &str,
        headers: Option<HeaderMap>,
        request: Option<&TRequest>,
    ) -> AnalyticsStorageResult<TResponse>
    where
        TRequest: Serialize + ?Sized,
        TResponse: DeserializeOwned,
    {
        let url = if path.is_empty() {
            let mut url = self.base_url.clone();
            let current_path = url.path().to_string();
            if current_path.len() > 1 && current_path.ends_with('/') {
                url.set_path(current_path.trim_end_matches('/'));
            }
            url
        } else {
            self.base_url.join(path)?
        };
        let mut builder = self.client.request(method, url);
        if let Some(headers) = headers {
            builder = builder.headers(headers);
        }
        if let Some(request) = request {
            builder = builder.json(request);
        }
        let response = builder.send().await?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(AnalyticsStorageError::with_debug(
                AnalyticsStorageErrorKind::HttpStatus,
                AnalyticsStorageErrorDebug::HttpStatus { status, body },
            ));
        }
        Ok(serde_json::from_str(body.as_str())?)
    }
}

impl AuxStorageCoordinationClient {
    pub fn new(base_url: &str, timeout: Duration) -> AnalyticsStorageResult<Self> {
        Ok(Self {
            client: AuxStorageStreamClient::new(base_url, timeout)?,
        })
    }

    pub async fn ensure_coordination_schema(&self) -> AnalyticsStorageResult<()> {
        for request in [
            processor_table_request(),
            ingest_jobs_table_request(),
            ingest_progress_table_request(),
        ] {
            if let Err(error) = self
                .client
                .dynamo::<_, serde_json::Value>("CreateTable", &request)
                .await
                && !is_resource_in_use(&error)
            {
                return Err(error);
            }
        }
        Ok(())
    }

    pub async fn ensure_bootstrap_schema(&self) -> AnalyticsStorageResult<()> {
        for request in [processor_table_request(), ingest_jobs_table_request()] {
            if let Err(error) = self
                .client
                .dynamo::<_, serde_json::Value>("CreateTable", &request)
                .await
                && !is_resource_in_use(&error)
            {
                return Err(error);
            }
        }
        Ok(())
    }

    pub async fn acquire_bootstrap_lease(
        &self,
        lease: &BootstrapLease,
        now_ms: i64,
    ) -> AnalyticsStorageResult<SlotLeaseOutcome> {
        match self
            .client
            .update_item(&bootstrap_lease_acquire_request(lease, now_ms))
            .await
        {
            Ok(_) => Ok(SlotLeaseOutcome::Acquired),
            Err(error) if is_conditional_check_failed(&error) => {
                Ok(SlotLeaseOutcome::HeldByAnotherProcessor)
            }
            Err(error) => Err(error),
        }
    }

    pub async fn release_bootstrap_lease(
        &self,
        lease: &BootstrapLease,
        released_until_ms: i64,
    ) -> AnalyticsStorageResult<bool> {
        match self
            .client
            .update_item(&bootstrap_lease_release_request(lease, released_until_ms))
            .await
        {
            Ok(_) => Ok(true),
            Err(error) if is_conditional_check_failed(&error) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub async fn write_heartbeat(
        &self,
        heartbeat: &ProcessorHeartbeat,
    ) -> AnalyticsStorageResult<()> {
        self.client
            .dynamo::<_, serde_json::Value>("PutItem", &heartbeat_put_request(heartbeat))
            .await
            .map(|_| ())
    }

    pub async fn read_active_heartbeats(
        &self,
        active_after_ms: i64,
    ) -> AnalyticsStorageResult<Vec<ProcessorHeartbeat>> {
        let response: QueryResponse = self
            .client
            .dynamo("Query", &active_heartbeats_query(active_after_ms))
            .await?;
        Ok(response
            .items
            .unwrap_or_default()
            .into_iter()
            .filter_map(processor_heartbeat_from_item)
            .collect())
    }

    pub async fn acquire_slot_lease(
        &self,
        lease: &SlotLease,
        now_ms: i64,
    ) -> AnalyticsStorageResult<SlotLeaseOutcome> {
        match self
            .client
            .update_item(&slot_lease_acquire_request(lease, now_ms))
            .await
        {
            Ok(_) => Ok(SlotLeaseOutcome::Acquired),
            Err(error) if is_conditional_check_failed(&error) => {
                Ok(SlotLeaseOutcome::HeldByAnotherProcessor)
            }
            Err(error) => Err(error),
        }
    }

    pub async fn renew_slot_lease(&self, lease: &SlotLease) -> AnalyticsStorageResult<bool> {
        match self
            .client
            .update_item(&slot_lease_renew_request(lease))
            .await
        {
            Ok(_) => Ok(true),
            Err(error) if is_conditional_check_failed(&error) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub async fn release_slot_lease(
        &self,
        lease: &SlotLease,
        released_until_ms: i64,
    ) -> AnalyticsStorageResult<bool> {
        match self
            .client
            .update_item(&slot_lease_release_request(lease, released_until_ms))
            .await
        {
            Ok(_) => Ok(true),
            Err(error) if is_conditional_check_failed(&error) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub async fn load_progress(
        &self,
        table_id: &str,
    ) -> AnalyticsStorageResult<Option<SourceProgress>> {
        let response: GetItemResponse = self
            .client
            .dynamo("GetItem", &progress_get_request(table_id))
            .await?;
        Ok(response.item.and_then(source_progress_from_item))
    }

    pub async fn save_progress(
        &self,
        progress: &SourceProgress,
        lease: &SlotLease,
        now_ms: i64,
    ) -> AnalyticsStorageResult<bool> {
        match self
            .client
            .dynamo::<_, serde_json::Value>(
                "TransactWriteItems",
                &progress_save_request(progress, lease, now_ms),
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(error) if is_conditional_check_failed(&error) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub async fn scan_change_index_slot(
        &self,
        request: &ListChangeIndexMarkersRequest,
    ) -> AnalyticsStorageResult<ListChangeIndexMarkersResponse> {
        self.client.dynamo("ListChangeIndexMarkers", request).await
    }

    pub async fn write_ingest_rate_rollup(
        &self,
        rollup: &IngestRateRollup,
    ) -> AnalyticsStorageResult<()> {
        self.client
            .dynamo::<_, serde_json::Value>("PutItem", &ingest_rate_rollup_put_request(rollup))
            .await
            .map(|_| ())
    }
}

impl AuxStorageLeaseClient {
    pub fn new(base_url: &str, timeout: Duration) -> AnalyticsStorageResult<Self> {
        Ok(Self {
            client: AuxStorageStreamClient::new(base_url, timeout)?,
        })
    }

    pub async fn ensure_source_polling_lease_table(&self) -> AnalyticsStorageResult<()> {
        self.client
            .dynamo::<_, serde_json::Value>("CreateTable", &source_polling_lease_table_request())
            .await
            .map(|_| ())
    }

    pub async fn try_acquire_source_polling_lease(
        &self,
        worker_id: &str,
        lease_token: &str,
        now_ms: i64,
        lease_until_ms: i64,
    ) -> AnalyticsStorageResult<AuxStorageLeaseOutcome> {
        let request =
            source_polling_lease_acquire_request(worker_id, lease_token, now_ms, lease_until_ms);
        match self.client.update_item(&request).await {
            Ok(_) => Ok(AuxStorageLeaseOutcome::Acquired),
            Err(error) if is_conditional_check_failed(&error) => {
                Ok(AuxStorageLeaseOutcome::HeldByAnotherWorker)
            }
            Err(error) => Err(error),
        }
    }

    pub async fn renew_source_polling_lease(
        &self,
        worker_id: &str,
        lease_token: &str,
        lease_until_ms: i64,
    ) -> AnalyticsStorageResult<bool> {
        let request = source_polling_lease_renew_request(worker_id, lease_token, lease_until_ms);
        match self.client.update_item(&request).await {
            Ok(_) => Ok(true),
            Err(error) if is_conditional_check_failed(&error) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub async fn release_source_polling_lease(
        &self,
        worker_id: &str,
        lease_token: &str,
        released_until_ms: i64,
    ) -> AnalyticsStorageResult<bool> {
        let request =
            source_polling_lease_release_request(worker_id, lease_token, released_until_ms);
        match self.client.update_item(&request).await {
            Ok(_) => Ok(true),
            Err(error) if is_conditional_check_failed(&error) => Ok(false),
            Err(error) => Err(error),
        }
    }
}

fn source_polling_lease_table_request() -> CreateTableRequest {
    CreateTableRequest::new(
        TableName::new("sys_jobs"),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        BillingMode::PayPerRequest,
    )
}

fn processor_table_request() -> CreateTableRequest {
    coordination_table_request("__analytics_processors")
}

fn ingest_jobs_table_request() -> CreateTableRequest {
    coordination_table_request("__analytics_ingest_jobs")
}

fn ingest_progress_table_request() -> CreateTableRequest {
    coordination_table_request("__analytics_ingest_progress")
}

fn coordination_table_request(table_name: &str) -> CreateTableRequest {
    CreateTableRequest::new(
        TableName::new(table_name),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        BillingMode::PayPerRequest,
    )
}

fn heartbeat_put_request(heartbeat: &ProcessorHeartbeat) -> PutItemRequest {
    let mut item = HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("processor".to_string()));
    item.insert(
        "sk".to_string(),
        AttributeValue::S(heartbeat.processor_id.clone()),
    );
    item.insert(
        "processor_id".to_string(),
        AttributeValue::S(heartbeat.processor_id.clone()),
    );
    item.insert(
        "generation".to_string(),
        AttributeValue::S(heartbeat.generation.clone()),
    );
    item.insert(
        "started_at_ms".to_string(),
        AttributeValue::N(heartbeat.started_at_ms.to_string()),
    );
    item.insert(
        "last_seen_ms".to_string(),
        AttributeValue::N(heartbeat.last_seen_ms.to_string()),
    );
    item.insert(
        "expires_at_ms".to_string(),
        AttributeValue::N(heartbeat.expires_at_ms.to_string()),
    );
    item.insert(
        "state".to_string(),
        AttributeValue::S(processor_state_value(heartbeat.state).to_string()),
    );
    item.insert(
        "source".to_string(),
        AttributeValue::S("aux_storage".to_string()),
    );
    item.insert(
        "slot_count".to_string(),
        AttributeValue::N(heartbeat.slot_count.to_string()),
    );
    PutItemRequest::new(TableName::new("__analytics_processors"), item)
}

fn active_heartbeats_query(active_after_ms: i64) -> QueryRequest {
    let mut values = HashMap::new();
    values.insert(
        ":pk".to_string(),
        AttributeValue::S("processor".to_string()),
    );
    values.insert(
        ":active".to_string(),
        AttributeValue::S("active".to_string()),
    );
    values.insert(
        ":last_seen".to_string(),
        AttributeValue::N(active_after_ms.to_string()),
    );
    let mut names = HashMap::new();
    names.insert("#state".to_string(), "state".to_string());
    let mut request = QueryRequest::new(
        TableName::new("__analytics_processors"),
        "pk = :pk".to_string(),
    )
    .with_expression_attribute_names(Some(names))
    .with_expression_attribute_values(Some(values))
    .with_limit(Some(1000))
    .with_scan_index_forward(Some(true));
    request.filter_expression = Some("#state = :active AND last_seen_ms >= :last_seen".to_string());
    request
}

fn slot_lease_acquire_request(lease: &SlotLease, now_ms: i64) -> UpdateItemRequest {
    slot_lease_update_request(
        lease,
        lease.lease_until_ms,
        Some(now_ms),
        Some(lease.assignment_epoch),
        "(attribute_not_exists(lease_until_ms) OR lease_until_ms < :now OR (leased_by = \
         :processor AND generation = :generation))"
            .to_string(),
    )
}

fn slot_lease_renew_request(lease: &SlotLease) -> UpdateItemRequest {
    slot_lease_update_request(
        lease,
        lease.lease_until_ms,
        None,
        Some(lease.assignment_epoch),
        "leased_by = :processor AND generation = :generation AND lease_token = :lease_token"
            .to_string(),
    )
}

fn slot_lease_release_request(lease: &SlotLease, released_until_ms: i64) -> UpdateItemRequest {
    slot_lease_update_request(
        lease,
        released_until_ms,
        None,
        None,
        "leased_by = :processor AND generation = :generation AND lease_token = :lease_token"
            .to_string(),
    )
}

fn bootstrap_lease_acquire_request(lease: &BootstrapLease, now_ms: i64) -> UpdateItemRequest {
    bootstrap_lease_update_request(
        lease,
        lease.lease_until_ms,
        Some(now_ms),
        "(attribute_not_exists(lease_until_ms) OR lease_until_ms < :now OR (leased_by = \
         :processor AND generation = :generation))"
            .to_string(),
    )
}

fn bootstrap_lease_release_request(
    lease: &BootstrapLease,
    released_until_ms: i64,
) -> UpdateItemRequest {
    bootstrap_lease_update_request(
        lease,
        released_until_ms,
        None,
        "leased_by = :processor AND generation = :generation AND lease_token = :lease_token"
            .to_string(),
    )
}

fn bootstrap_lease_update_request(
    lease: &BootstrapLease,
    lease_until_ms: i64,
    now_ms: Option<i64>,
    condition_expression: String,
) -> UpdateItemRequest {
    let mut key = KeyAttributes::new();
    key.insert("pk".to_string(), AttributeValue::S("bootstrap".to_string()));
    key.insert(
        "sk".to_string(),
        AttributeValue::S("analytics_ingest_v1".to_string()),
    );

    let mut values = HashMap::new();
    values.insert(
        ":processor".to_string(),
        AttributeValue::S(lease.processor_id.clone()),
    );
    values.insert(
        ":generation".to_string(),
        AttributeValue::S(lease.generation.clone()),
    );
    values.insert(
        ":lease_token".to_string(),
        AttributeValue::S(lease.lease_token.clone()),
    );
    values.insert(
        ":lease_until".to_string(),
        AttributeValue::N(lease_until_ms.to_string()),
    );
    values.insert(
        ":job_id".to_string(),
        AttributeValue::S("analytics_ingest_v1".to_string()),
    );
    values.insert(
        ":state".to_string(),
        AttributeValue::S("bootstrap".to_string()),
    );
    if let Some(now_ms) = now_ms {
        values.insert(":now".to_string(), AttributeValue::N(now_ms.to_string()));
    }

    UpdateItemRequest::builder()
        .table_name(TableName::new("__analytics_ingest_jobs"))
        .key(key)
        .update_expression(
            "SET leased_by = :processor, generation = :generation, lease_token = :lease_token, \
             lease_until_ms = :lease_until, job_id = :job_id, job_state = :state"
                .to_string(),
        )
        .condition_expression(Some(condition_expression))
        .expression_attribute_values(Some(values))
        .build()
}

fn slot_lease_update_request(
    lease: &SlotLease,
    lease_until_ms: i64,
    now_ms: Option<i64>,
    assignment_epoch: Option<u64>,
    condition_expression: String,
) -> UpdateItemRequest {
    let mut key = KeyAttributes::new();
    key.insert(
        "pk".to_string(),
        AttributeValue::S("slot_lease".to_string()),
    );
    key.insert("sk".to_string(), AttributeValue::S(slot_sk(lease.slot)));

    let mut values = HashMap::new();
    values.insert(
        ":processor".to_string(),
        AttributeValue::S(lease.processor_id.clone()),
    );
    values.insert(
        ":generation".to_string(),
        AttributeValue::S(lease.generation.clone()),
    );
    values.insert(
        ":lease_token".to_string(),
        AttributeValue::S(lease.lease_token.clone()),
    );
    values.insert(
        ":lease_until".to_string(),
        AttributeValue::N(lease_until_ms.to_string()),
    );
    values.insert(
        ":slot".to_string(),
        AttributeValue::N(lease.slot.to_string()),
    );
    if let Some(now_ms) = now_ms {
        values.insert(":now".to_string(), AttributeValue::N(now_ms.to_string()));
    }
    if let Some(epoch) = assignment_epoch {
        values.insert(
            ":assignment_epoch".to_string(),
            AttributeValue::N(epoch.to_string()),
        );
    }

    let update_expression = if assignment_epoch.is_some() {
        "SET leased_by = :processor, generation = :generation, lease_token = :lease_token, \
         lease_until_ms = :lease_until, slot = :slot, assignment_epoch = :assignment_epoch"
            .to_string()
    } else {
        "SET leased_by = :processor, generation = :generation, lease_token = :lease_token, \
         lease_until_ms = :lease_until, slot = :slot"
            .to_string()
    };

    UpdateItemRequest::builder()
        .table_name(TableName::new("__analytics_ingest_jobs"))
        .key(key)
        .update_expression(update_expression)
        .condition_expression(Some(condition_expression))
        .expression_attribute_values(Some(values))
        .build()
}

fn progress_get_request(table_id: &str) -> GetItemRequest {
    GetItemRequest::new(
        TableName::new("__analytics_ingest_progress"),
        progress_key(table_id),
    )
}

fn progress_save_request(
    progress: &SourceProgress,
    lease: &SlotLease,
    now_ms: i64,
) -> TransactWriteItemsRequest {
    let mut values = HashMap::new();
    values.insert(
        ":cursor".to_string(),
        AttributeValue::S(progress.cursor.clone()),
    );
    values.insert(
        ":versionstamp".to_string(),
        AttributeValue::S(progress.versionstamp.clone()),
    );
    values.insert(
        ":updated_at".to_string(),
        AttributeValue::N(progress.updated_at_ms.to_string()),
    );
    values.insert(
        ":updated_by".to_string(),
        AttributeValue::S(progress.updated_by.clone()),
    );
    values.insert(
        ":generation".to_string(),
        AttributeValue::S(progress.generation.clone()),
    );
    let progress_names = HashMap::from([
        ("#cursor".to_string(), "cursor".to_string()),
        ("#versionstamp".to_string(), "versionstamp".to_string()),
        ("#updated_at_ms".to_string(), "updated_at_ms".to_string()),
        ("#updated_by".to_string(), "updated_by".to_string()),
        ("#generation".to_string(), "generation".to_string()),
    ]);
    let mut lease_values = HashMap::new();
    lease_values.insert(
        ":lease_processor".to_string(),
        AttributeValue::S(lease.processor_id.clone()),
    );
    lease_values.insert(
        ":lease_generation".to_string(),
        AttributeValue::S(lease.generation.clone()),
    );
    lease_values.insert(
        ":lease_token".to_string(),
        AttributeValue::S(lease.lease_token.clone()),
    );
    lease_values.insert(":now".to_string(), AttributeValue::N(now_ms.to_string()));

    TransactWriteItemsRequest {
        transact_items: vec![
            TransactWriteItem {
                condition_check: Some(TransactConditionCheckRequest {
                    table_name: TableName::new("__analytics_ingest_jobs"),
                    key: slot_lease_key(lease.slot),
                    condition_expression: "leased_by = :lease_processor AND generation = \
                                           :lease_generation AND lease_token = :lease_token AND \
                                           lease_until_ms > :now"
                        .to_string(),
                    expression_attribute_names: None,
                    expression_attribute_values: Some(lease_values),
                    return_values_on_condition_check_failure: None,
                }),
                ..Default::default()
            },
            TransactWriteItem {
                update: Some(TransactUpdateRequest {
                    table_name: TableName::new("__analytics_ingest_progress"),
                    key: progress_key(&progress.source_table_id),
                    update_expression: "SET #cursor = :cursor, #versionstamp = :versionstamp, \
                                        #updated_at_ms = :updated_at, #updated_by = :updated_by, \
                                        #generation = :generation"
                        .to_string(),
                    condition_expression: Some(
                        "attribute_not_exists(#versionstamp) OR #versionstamp <= :versionstamp"
                            .to_string(),
                    ),
                    expression_attribute_names: Some(progress_names),
                    expression_attribute_values: Some(values),
                    return_values_on_condition_check_failure: None,
                    aux_item_stream_ttl_hours: None,
                }),
                ..Default::default()
            },
        ],
        client_request_token: None,
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    }
}

fn ingest_rate_rollup_put_request(rollup: &IngestRateRollup) -> PutItemRequest {
    let mut item = HashMap::new();
    item.insert(
        "pk".to_string(),
        AttributeValue::S(format!("ingest_rate/{}", rollup.source_table_id)),
    );
    item.insert(
        "sk".to_string(),
        AttributeValue::S(format!(
            "tenant/{}/window/{}",
            rollup.tenant_id, rollup.window_started_at_ms
        )),
    );
    item.insert(
        "table_id".to_string(),
        AttributeValue::S(rollup.source_table_id.clone()),
    );
    item.insert(
        "tenant_id".to_string(),
        AttributeValue::S(rollup.tenant_id.clone()),
    );
    item.insert(
        "window_started_at_ms".to_string(),
        AttributeValue::N(rollup.window_started_at_ms.to_string()),
    );
    item.insert(
        "updated_at_ms".to_string(),
        AttributeValue::N(rollup.updated_at_ms.to_string()),
    );
    item.insert(
        "records_total".to_string(),
        AttributeValue::N(rollup.records_total.to_string()),
    );
    item.insert(
        "bytes_total".to_string(),
        AttributeValue::N(rollup.bytes_total.to_string()),
    );
    item.insert(
        "cursor".to_string(),
        AttributeValue::S(rollup.cursor.clone()),
    );
    item.insert(
        "versionstamp".to_string(),
        AttributeValue::S(rollup.versionstamp.clone()),
    );
    PutItemRequest::new(TableName::new("__analytics_ingest_jobs"), item)
}

fn progress_key(table_id: &str) -> KeyAttributes {
    let mut key = KeyAttributes::new();
    key.insert(
        "pk".to_string(),
        AttributeValue::S(format!("progress/{table_id}")),
    );
    key.insert("sk".to_string(), AttributeValue::S("cursor".to_string()));
    key
}

fn slot_sk(slot: u16) -> String {
    format!("slot/{slot}")
}

fn slot_lease_key(slot: u16) -> KeyAttributes {
    let mut key = KeyAttributes::new();
    key.insert(
        "pk".to_string(),
        AttributeValue::S("slot_lease".to_string()),
    );
    key.insert("sk".to_string(), AttributeValue::S(slot_sk(slot)));
    key
}

fn processor_state_value(state: ProcessorState) -> &'static str {
    match state {
        ProcessorState::Active => "active",
        ProcessorState::Leaving => "leaving",
    }
}

fn processor_state_from_value(value: &str) -> Option<ProcessorState> {
    match value {
        "active" => Some(ProcessorState::Active),
        "leaving" => Some(ProcessorState::Leaving),
        _ => None,
    }
}

fn processor_heartbeat_from_item(item: AttributeMap) -> Option<ProcessorHeartbeat> {
    Some(ProcessorHeartbeat {
        processor_id: string_attr(&item, "processor_id")?.to_string(),
        generation: string_attr(&item, "generation")?.to_string(),
        started_at_ms: number_attr(&item, "started_at_ms")?,
        last_seen_ms: number_attr(&item, "last_seen_ms")?,
        expires_at_ms: number_attr(&item, "expires_at_ms")?,
        state: processor_state_from_value(string_attr(&item, "state")?)?,
        slot_count: number_attr::<u16>(&item, "slot_count")?,
    })
}

fn source_progress_from_item(item: AttributeMap) -> Option<SourceProgress> {
    let source_table_id = string_attr(&item, "pk")?
        .strip_prefix("progress/")?
        .to_string();
    Some(SourceProgress {
        source_table_id,
        cursor: string_attr(&item, "cursor")?.to_string(),
        versionstamp: string_attr(&item, "versionstamp")?.to_string(),
        updated_at_ms: number_attr(&item, "updated_at_ms")?,
        updated_by: string_attr(&item, "updated_by")?.to_string(),
        generation: string_attr(&item, "generation")?.to_string(),
    })
}

fn string_attr<'a>(item: &'a AttributeMap, name: &str) -> Option<&'a str> {
    match item.get(name)? {
        AttributeValue::S(value) => Some(value),
        _ => None,
    }
}

fn number_attr<T>(item: &AttributeMap, name: &str) -> Option<T>
where T: std::str::FromStr {
    match item.get(name)? {
        AttributeValue::N(value) => value.parse().ok(),
        _ => None,
    }
}

fn source_polling_lease_acquire_request(
    worker_id: &str,
    lease_token: &str,
    now_ms: i64,
    lease_until_ms: i64,
) -> UpdateItemRequest {
    source_polling_lease_update_request(
        worker_id,
        lease_token,
        lease_until_ms,
        Some(now_ms),
        "(attribute_not_exists(lease_until_ms) OR lease_until_ms < :now)".to_string(),
        "acquired",
    )
}

fn source_polling_lease_renew_request(
    worker_id: &str,
    lease_token: &str,
    lease_until_ms: i64,
) -> UpdateItemRequest {
    source_polling_lease_update_request(
        worker_id,
        lease_token,
        lease_until_ms,
        None,
        "leased_by = :worker AND lease_token = :lease_token".to_string(),
        "renewed",
    )
}

fn source_polling_lease_release_request(
    worker_id: &str,
    lease_token: &str,
    released_until_ms: i64,
) -> UpdateItemRequest {
    source_polling_lease_update_request(
        worker_id,
        lease_token,
        released_until_ms,
        None,
        "leased_by = :worker AND lease_token = :lease_token".to_string(),
        "released",
    )
}

fn source_polling_lease_update_request(
    worker_id: &str,
    lease_token: &str,
    lease_until_ms: i64,
    now_ms: Option<i64>,
    condition_expression: String,
    state: &str,
) -> UpdateItemRequest {
    let mut key = KeyAttributes::new();
    key.insert(
        "pk".to_string(),
        AttributeValue::S("JOB#analytics_source_polling".to_string()),
    );
    key.insert("sk".to_string(), AttributeValue::S("LOCK".to_string()));

    let mut values = HashMap::new();
    values.insert(
        ":worker".to_string(),
        AttributeValue::S(worker_id.to_string()),
    );
    values.insert(
        ":lease".to_string(),
        AttributeValue::N(lease_until_ms.to_string()),
    );
    if let Some(now_ms) = now_ms {
        values.insert(":now".to_string(), AttributeValue::N(now_ms.to_string()));
    }
    values.insert(
        ":lease_token".to_string(),
        AttributeValue::S(lease_token.to_string()),
    );
    values.insert(
        ":job_id".to_string(),
        AttributeValue::S("analytics_source_polling".to_string()),
    );
    values.insert(":state".to_string(), AttributeValue::S(state.to_string()));

    UpdateItemRequest::builder()
        .table_name(TableName::new("sys_jobs"))
        .key(key)
        .update_expression(
            "SET leased_by = :worker, lease_until_ms = :lease, lease_token = :lease_token, job_id \
             = :job_id, job_state = :state"
                .to_string(),
        )
        .condition_expression(Some(condition_expression))
        .expression_attribute_values(Some(values))
        .build()
}

fn is_conditional_check_failed(error: &AnalyticsStorageError) -> bool {
    error.to_string().contains("ConditionalCheckFailed")
}

fn is_resource_in_use(error: &AnalyticsStorageError) -> bool {
    if error.kind() != AnalyticsStorageErrorKind::HttpStatus {
        return false;
    }
    let AnalyticsStorageErrorDebug::HttpStatus { body, .. } = error.debug() else {
        return false;
    };
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|value| {
            value
                .get("__type")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .is_some_and(|error_type| error_type.ends_with("#ResourceInUseException"))
}
