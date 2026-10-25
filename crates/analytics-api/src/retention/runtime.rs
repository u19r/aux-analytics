use super::*;

#[derive(Debug)]
pub struct RetentionRuntime {
    delete_batch_size: u64,
    sweep_interval_ms: u64,
    tenant_resolver: TenantResolver,
    tables: HashMap<String, RetentionTableRuntime>,
}

impl RetentionRuntime {
    pub async fn from_config(
        config: &AnalyticsRetentionConfig,
    ) -> Result<Option<Self>, analytics_storage::AnalyticsStorageError> {
        if !config.enabled || config.tables.is_empty() {
            return Ok(None);
        }
        let mut tables = HashMap::new();
        for table in &config.tables {
            tables.insert(
                table.analytics_table_name.clone(),
                RetentionTableRuntime::try_from_config(table).await?,
            );
        }
        Ok(Some(Self {
            delete_batch_size: config.delete_batch_size,
            sweep_interval_ms: config.sweep_interval_ms,
            tenant_resolver: TenantResolver::default(),
            tables,
        }))
    }

    pub fn table_names(&self) -> impl Iterator<Item = &str> {
        self.tables.keys().map(String::as_str)
    }

    pub(crate) fn table_names_sorted(&self) -> Vec<String> {
        let mut names = self.tables.keys().cloned().collect::<Vec<_>>();
        names.sort();
        names
    }

    pub(crate) fn is_table_configured(&self, table_name: &str) -> bool {
        self.tables.contains_key(table_name)
    }

    pub(crate) fn delete_batch_size(&self) -> u64 {
        self.delete_batch_size
    }

    pub(crate) fn sweep_interval_ms(&self) -> u64 {
        self.sweep_interval_ms
    }

    pub async fn retention_for_record(
        &self,
        manifest: &AnalyticsManifest,
        analytics_table_name: &str,
        source_table_name: &str,
        record: &StorageStreamRecord,
    ) -> Option<IngestRetention> {
        let table = self.tables.get(analytics_table_name)?;
        let item = record.new_image.as_ref()?;
        let registration = manifest
            .tables
            .iter()
            .find(|table| table.analytics_table_name == analytics_table_name)?;
        let tenant_id = self
            .tenant_resolver
            .resolve(registration, source_table_name, item)
            .ok()
            .flatten()
            .unwrap_or_default();
        if tenant_id.is_empty() {
            return Some(table.lookup_failed_retention());
        }
        let started = Instant::now();
        match table.lookup_period_ms(tenant_id.as_str()).await {
            Ok(period_ms) => {
                table.record_lookup_metrics("success", started);
                Some(IngestRetention {
                    period_ms: Some(period_ms),
                    timestamp: table.timestamp.clone(),
                    missing_retention: false,
                })
            }
            Err(error) => {
                table.record_lookup_failure(&tenant_id, &error, started);
                Some(table.lookup_failed_retention())
            }
        }
    }
}
