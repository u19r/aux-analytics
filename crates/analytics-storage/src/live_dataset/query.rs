use analytics_contract::{DatasetQuery, DatasetQueryMode, DatasetQueryResponse};
use storage::QueryTableInput;
use storage_types::{AttributeMap, AttributeValue, TableName};

use super::{
    LiveDatasetError, LiveDatasetManager,
    catalog::StoredDataset,
    cursor::{DatasetCursor, fingerprint_query},
};
use crate::constants::{MAX_LIVE_DATASET_EXAMINED_ROWS, MAX_LIVE_DATASET_WINDOW_MS};

struct DatasetRead {
    fingerprint: String,
    watermark: String,
    source_cursor: Option<String>,
}

impl LiveDatasetManager {
    pub async fn query_dataset(
        &self,
        tenant: &str,
        metadata_table: TableName,
        id: &str,
        request: DatasetQuery,
    ) -> Result<DatasetQueryResponse, LiveDatasetError> {
        let stored = self.get_dataset(tenant, metadata_table.clone(), id).await?;
        Self::validate_query(&stored, &request)?;
        let read = self.prepare_read(&stored, &request).await?;
        let response = self.read_dataset(&stored, &request, read).await?;
        self.verify_dataset_read(tenant, metadata_table, &stored, &response)
            .await?;
        Ok(response)
    }

    async fn prepare_read(
        &self,
        stored: &StoredDataset,
        request: &DatasetQuery,
    ) -> Result<DatasetRead, LiveDatasetError> {
        let fingerprint = fingerprint_query(request)?;
        let watermark = self.get_source_watermark(stored).await?;
        let cursor = request
            .cursor
            .as_deref()
            .map(|value| DatasetCursor::decode(value, stored, &fingerprint))
            .transpose()?;
        if cursor
            .as_ref()
            .is_some_and(|cursor| cursor.watermark != watermark)
        {
            return Err(LiveDatasetError::SourceChanged);
        }
        Ok(DatasetRead {
            fingerprint,
            watermark,
            source_cursor: cursor.map(|cursor| cursor.source_cursor),
        })
    }

    async fn verify_dataset_read(
        &self,
        tenant: &str,
        metadata_table: TableName,
        stored: &StoredDataset,
        response: &DatasetQueryResponse,
    ) -> Result<(), LiveDatasetError> {
        if self.get_source_watermark(stored).await? != response.source_watermark {
            return Err(LiveDatasetError::SourceChanged);
        }
        let current = self
            .get_dataset(tenant, metadata_table, &stored.dataset.dataset_id)
            .await
            .map_err(|error| match error {
                LiveDatasetError::NotFound => LiveDatasetError::VersionConflict,
                other => other,
            })?;
        if current.dataset.version != stored.dataset.version
            || current.cursor_secret != stored.cursor_secret
        {
            return Err(LiveDatasetError::VersionConflict);
        }
        Ok(())
    }

    fn validate_query(
        stored: &StoredDataset,
        request: &DatasetQuery,
    ) -> Result<(), LiveDatasetError> {
        if request.expected_dataset_version != stored.dataset.version {
            return Err(LiveDatasetError::VersionConflict);
        }
        Self::validate_identifier(&request.status)?;
        if !(1..=100).contains(&request.page_size)
            || request.from_ms > request.through_ms
            || request.through_ms - request.from_ms > MAX_LIVE_DATASET_WINDOW_MS
        {
            return Err(LiveDatasetError::InvalidQuery);
        }
        match &request.mode {
            DatasetQueryMode::Count if request.cursor.is_some() => {
                return Err(LiveDatasetError::InvalidQuery);
            }
            DatasetQueryMode::Rows { fields } => {
                if fields.is_empty()
                    || fields.len() > 32
                    || fields
                        .iter()
                        .any(|field| !stored.dataset.allowed_fields.contains(field))
                {
                    return Err(LiveDatasetError::InvalidQuery);
                }
            }
            DatasetQueryMode::Count => {}
        }
        Ok(())
    }

    async fn get_source_watermark(
        &self,
        stored: &StoredDataset,
    ) -> Result<String, LiveDatasetError> {
        let source = self.validate_source(&stored.dataset).await?;
        if source.incarnation() != stored.source_incarnation {
            return Err(LiveDatasetError::SourceChanged);
        }
        Ok(source.watermark())
    }

    async fn read_dataset(
        &self,
        stored: &StoredDataset,
        request: &DatasetQuery,
        mut read: DatasetRead,
    ) -> Result<DatasetQueryResponse, LiveDatasetError> {
        let mut source_cursor = read.source_cursor.take();
        let mut response = read.start_response(stored, request);
        loop {
            let page = self
                .db
                .query_table(Self::build_source_query(
                    stored,
                    request,
                    source_cursor.take(),
                ))
                .await
                .map_err(|_| LiveDatasetError::SourceUnavailable)?;
            response.examined_rows += page.progress.evaluated_count;
            response.source_pages += 1;
            #[cfg(test)]
            if let Some(pause) = &self.page_pause {
                pause.observe_page().await;
            }
            if response.examined_rows > MAX_LIVE_DATASET_EXAMINED_ROWS {
                return Err(LiveDatasetError::QueryTooBroad);
            }
            Self::append_rows(&mut response, &request.mode, page.items)?;
            source_cursor = page.continuation;
            if matches!(request.mode, DatasetQueryMode::Rows { .. }) || source_cursor.is_none() {
                break;
            }
            if response.examined_rows == MAX_LIVE_DATASET_EXAMINED_ROWS {
                return Err(LiveDatasetError::QueryTooBroad);
            }
        }
        response.next_cursor = Self::encode_next_cursor(stored, &response, source_cursor)?;
        Ok(response)
    }

    fn build_source_query(
        stored: &StoredDataset,
        request: &DatasetQuery,
        cursor: Option<String>,
    ) -> QueryTableInput {
        let values: AttributeMap = [
            (
                ":partition".into(),
                AttributeValue::S(format!(
                    "APPLICATION#{}#STATUS#{}",
                    stored.dataset.application_id, request.status
                )),
            ),
            (
                ":from".into(),
                AttributeValue::S(format!("{:020}#", request.from_ms)),
            ),
            (
                ":through".into(),
                AttributeValue::S(format!("{:020}#~", request.through_ms)),
            ),
        ]
        .into_iter()
        .collect();
        let mut query = QueryTableInput::builder()
            .table_name(TableName::new(&stored.dataset.source_table))
            .key_condition_expression("pk = :partition AND sk BETWEEN :from AND :through")
            .expression_attribute_values(values)
            .limit(u32::from(request.page_size))
            .consistent_read(true)
            .build();
        query.exclusive_start_key = cursor;
        query
    }

    fn append_rows(
        response: &mut DatasetQueryResponse,
        mode: &DatasetQueryMode,
        items: Vec<storage_types::WireItem>,
    ) -> Result<(), LiveDatasetError> {
        match mode {
            DatasetQueryMode::Count => {
                response.count = Some(response.count.unwrap_or(0) + items.len() as u64)
            }
            DatasetQueryMode::Rows { fields } => {
                for item in items {
                    response.rows.push(Self::project_row(&item, fields)?);
                }
            }
        }
        Ok(())
    }

    fn project_row(
        item: &storage_types::WireItem,
        fields: &[String],
    ) -> Result<serde_json::Value, LiveDatasetError> {
        let attributes = item
            .to_attribute_map()
            .map_err(|_| LiveDatasetError::Storage)?;
        let mut row = serde_json::Map::new();
        for field in fields {
            if let Some(value) = attributes.get(field) {
                row.insert(
                    field.clone(),
                    value
                        .to_json_value()
                        .map_err(|_| LiveDatasetError::Storage)?,
                );
            }
        }
        Ok(serde_json::Value::Object(row))
    }

    fn encode_next_cursor(
        stored: &StoredDataset,
        response: &DatasetQueryResponse,
        source_cursor: Option<String>,
    ) -> Result<Option<String>, LiveDatasetError> {
        source_cursor
            .map(|source_cursor| {
                DatasetCursor {
                    tenant: stored.dataset.tenant.clone(),
                    dataset_id: stored.dataset.dataset_id.clone(),
                    version: stored.dataset.version,
                    fingerprint: response.query_fingerprint.clone(),
                    watermark: response.source_watermark.clone(),
                    source_cursor,
                }
                .encode(&stored.cursor_secret)
            })
            .transpose()
    }
}

impl DatasetRead {
    fn start_response(
        self,
        stored: &StoredDataset,
        request: &DatasetQuery,
    ) -> DatasetQueryResponse {
        DatasetQueryResponse {
            dataset_id: stored.dataset.dataset_id.clone(),
            dataset_version: stored.dataset.version,
            source_watermark: self.watermark,
            query_fingerprint: self.fingerprint,
            from_ms: request.from_ms,
            through_ms: request.through_ms,
            count: None,
            rows: Vec::new(),
            next_cursor: None,
            examined_rows: 0,
            source_pages: 0,
        }
    }
}
