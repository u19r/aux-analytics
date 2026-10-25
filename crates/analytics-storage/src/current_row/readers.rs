use analytics_operations::{
    CheckError, CurrentRowPageRequest, CurrentRowStreamOrder, ProductionCurrentRowPageReader,
    ProjectedCurrentRowPage,
};

use super::{
    AuxStorageCurrentRowClient, AuxStorageCurrentRowPageReader, DynamoDbCurrentRowClient,
    DynamoDbCurrentRowPageReader,
    attributes::string_attribute,
    projection::{
        aux_storage_scan_request, dynamodb_page_partition, dynamodb_scan_request,
        merge_page_partition, projected_row_from_dynamodb_item, projected_row_from_item,
        wire_item_to_attribute_map,
    },
};

impl<Client> AuxStorageCurrentRowPageReader<Client> {
    #[must_use]
    pub fn new(client: Client) -> Self {
        Self {
            client,
            stream_order: CurrentRowStreamOrder::Unordered,
            partition_column: None,
        }
    }

    #[must_use]
    pub fn new_globally_sorted(client: Client) -> Self {
        Self {
            client,
            stream_order: CurrentRowStreamOrder::GloballySortedByKey,
            partition_column: None,
        }
    }

    #[must_use]
    pub fn new_partition_sorted(client: Client, partition_column: impl Into<String>) -> Self {
        Self {
            client,
            stream_order: CurrentRowStreamOrder::PartitionSortedByKey,
            partition_column: Some(partition_column.into()),
        }
    }
}

impl<Client> DynamoDbCurrentRowPageReader<Client> {
    #[must_use]
    pub fn new(client: Client) -> Self {
        Self {
            client,
            stream_order: CurrentRowStreamOrder::Unordered,
            partition_column: None,
        }
    }

    #[must_use]
    pub fn new_globally_sorted(client: Client) -> Self {
        Self {
            client,
            stream_order: CurrentRowStreamOrder::GloballySortedByKey,
            partition_column: None,
        }
    }

    #[must_use]
    pub fn new_partition_sorted(client: Client, partition_column: impl Into<String>) -> Self {
        Self {
            client,
            stream_order: CurrentRowStreamOrder::PartitionSortedByKey,
            partition_column: Some(partition_column.into()),
        }
    }
}

impl<Client> ProductionCurrentRowPageReader for AuxStorageCurrentRowPageReader<Client>
where Client: AuxStorageCurrentRowClient
{
    fn current_row_page(
        &self,
        request: &CurrentRowPageRequest,
    ) -> Result<ProjectedCurrentRowPage, CheckError> {
        let scan_request = aux_storage_scan_request(request, self.partition_column.as_deref())?;
        let response = self.client.scan_current_rows(&scan_request)?;
        let items = response.items.unwrap_or_default();
        let mut partition = None;
        let mut rows = Vec::with_capacity(items.len());
        for item in items {
            let item = wire_item_to_attribute_map(item)?;
            if let Some(column) = self.partition_column.as_deref() {
                merge_page_partition(
                    &mut partition,
                    string_attribute(&item, column)?,
                    column,
                    &request.projection.table_name,
                )?;
            }
            rows.push(projected_row_from_item(request, item)?);
        }
        Ok(ProjectedCurrentRowPage {
            rows,
            next_cursor: response
                .last_evaluated_key
                .as_ref()
                .map(super::cursor::aux_storage_cursor_from_key)
                .transpose()?,
            partition,
        })
    }

    fn stream_order(&self) -> CurrentRowStreamOrder {
        self.stream_order
    }
}

impl<Client> ProductionCurrentRowPageReader for DynamoDbCurrentRowPageReader<Client>
where Client: DynamoDbCurrentRowClient
{
    fn current_row_page(
        &self,
        request: &CurrentRowPageRequest,
    ) -> Result<ProjectedCurrentRowPage, CheckError> {
        let scan_request = dynamodb_scan_request(request, self.partition_column.as_deref())?;
        let response = self.client.scan_current_rows(&scan_request)?;
        let partition = dynamodb_page_partition(
            &response.items,
            self.partition_column.as_deref(),
            &request.projection.table_name,
        )?;
        let rows = response
            .items
            .iter()
            .map(|item| projected_row_from_dynamodb_item(request, item))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ProjectedCurrentRowPage {
            rows,
            next_cursor: response
                .last_evaluated_key
                .as_ref()
                .map(super::cursor::dynamodb_cursor_from_key)
                .transpose()?,
            partition,
        })
    }

    fn stream_order(&self) -> CurrentRowStreamOrder {
        self.stream_order
    }
}
