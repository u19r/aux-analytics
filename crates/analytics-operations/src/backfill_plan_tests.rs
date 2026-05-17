use crate::{
    AuxStorageBackfillMetadata, AuxStorageBackfillMetadataAdapter, BackfillCapabilityDiscovery,
    BackfillDiscoveryAdapters, BackfillPlanError, BackfillPlanner, BackfillRequest,
    BackfillRequestInput, BackfillSourceCapabilities, BackfillSourceKind,
    ConservativeBackfillPlanner, DestinationBackend, DynamoDbBackfillMetadata,
    DynamoDbBackfillMetadataAdapter, RateLimitPolicy, SnapshotMethod,
    StaticBackfillCapabilityDiscovery, ValidationMode, build_backfill_request,
};

fn request(source: BackfillSourceCapabilities) -> BackfillRequest {
    BackfillRequest {
        target_tables: vec!["users".to_string()],
        source,
        requested_snapshot_method: None,
        filters: Vec::new(),
        chunk_target_rows: 100,
        rate_limit: RateLimitPolicy::default(),
        destination_backend: DestinationBackend::DuckDb,
        dry_run: true,
        validation_mode: ValidationMode::CountsAndHashes,
        all_tables_have_row_identity: true,
        estimated_table_rows: 250,
    }
}

#[test]
fn given_dynamodb_with_pitr_when_planned_then_export_is_selected() {
    let planner = ConservativeBackfillPlanner;

    let plan = planner
        .plan(&request(BackfillSourceCapabilities::dynamodb_export()))
        .unwrap();

    assert_eq!(plan.snapshot_method, SnapshotMethod::DynamoDbPitrExport);
    assert_eq!(plan.chunk_count, 3);
}

#[test]
fn given_aux_storage_when_planned_then_full_enumeration_is_selected() {
    let planner = ConservativeBackfillPlanner;

    let plan = planner
        .plan(&request(BackfillSourceCapabilities::aux_storage()))
        .unwrap();

    assert_eq!(plan.snapshot_method, SnapshotMethod::AuxStorageEnumeration);
    assert!(plan.deletes_can_be_proven);
}

#[test]
fn given_missing_row_identity_when_planned_then_plan_is_rejected() {
    let planner = ConservativeBackfillPlanner;
    let mut request = request(BackfillSourceCapabilities::local_fixture());
    request.all_tables_have_row_identity = false;

    let error = planner.plan(&request).unwrap_err();

    assert!(matches!(
        error,
        BackfillPlanError::MissingRowIdentity { .. }
    ));
}

#[test]
fn given_stream_only_dynamodb_request_when_planned_then_it_is_rejected() {
    let planner = ConservativeBackfillPlanner;
    let mut request = request(BackfillSourceCapabilities::dynamodb_export());
    request.requested_snapshot_method = Some(SnapshotMethod::AuxStorageEnumeration);

    let error = planner.plan(&request).unwrap_err();

    assert!(matches!(
        error,
        BackfillPlanError::UnsafeStreamOnlyBackfill { .. }
    ));
}

#[test]
fn given_local_fixture_when_aux_storage_enumeration_requested_then_plan_is_rejected() {
    let planner = ConservativeBackfillPlanner;
    let mut request = request(BackfillSourceCapabilities::local_fixture());
    request.requested_snapshot_method = Some(SnapshotMethod::AuxStorageEnumeration);

    let error = planner.plan(&request).unwrap_err();

    assert!(matches!(
        error,
        BackfillPlanError::HistoricalEnumerationUnsupported
    ));
}

#[test]
fn given_local_fixture_when_fixture_snapshot_requested_then_plan_is_accepted() {
    let planner = ConservativeBackfillPlanner;
    let mut request = request(BackfillSourceCapabilities::local_fixture());
    request.requested_snapshot_method = Some(SnapshotMethod::LocalFixture);

    let plan = planner.plan(&request).unwrap();

    assert_eq!(plan.snapshot_method, SnapshotMethod::LocalFixture);
}

#[test]
fn given_source_kind_when_capability_discovered_then_matching_capabilities_are_returned() {
    let discovery = StaticBackfillCapabilityDiscovery;

    let capabilities = discovery.discover(BackfillSourceKind::AuxStorage).unwrap();

    assert!(capabilities.supports_full_enumeration);
    assert!(capabilities.can_prove_deletes);
}

#[test]
fn given_backfill_input_when_request_built_then_capabilities_are_attached() {
    let input = BackfillRequestInput {
        target_tables: vec!["users".to_string()],
        source_kind: BackfillSourceKind::LocalFixture,
        destination_backend: DestinationBackend::DuckDb,
        dry_run: true,
        validation_mode: ValidationMode::CountsAndHashes,
        all_tables_have_row_identity: true,
        estimated_table_rows: 10,
        chunk_target_rows: 5,
        rate_limit: RateLimitPolicy::default(),
    };

    let request = build_backfill_request(input, &StaticBackfillCapabilityDiscovery).unwrap();

    assert_eq!(request.source.source_kind, BackfillSourceKind::LocalFixture);
    assert_eq!(request.target_tables, vec!["users"]);
}

#[test]
fn given_adapter_backed_dynamodb_discovery_when_pitr_is_disabled_then_scan_only_capabilities_return()
 {
    let discovery = BackfillDiscoveryAdapters::new(
        FakeDynamoDbMetadataAdapter {
            metadata: DynamoDbBackfillMetadata {
                metadata_reachable: true,
                pitr_enabled: false,
                scan_enabled: true,
                stream_retention_hours: Some(12),
            },
        },
        FakeAuxStorageMetadataAdapter::full(),
    );

    let capabilities = discovery.discover(BackfillSourceKind::DynamoDb).unwrap();

    assert_eq!(capabilities.source_kind, BackfillSourceKind::DynamoDb);
    assert!(!capabilities.supports_pitr_export);
    assert!(capabilities.supports_parallel_scan);
    assert_eq!(capabilities.stream_retention_hours, Some(12));
}

#[test]
fn given_adapter_backed_aux_storage_discovery_when_enumeration_missing_then_rejected() {
    let discovery = BackfillDiscoveryAdapters::new(
        FakeDynamoDbMetadataAdapter::full(),
        FakeAuxStorageMetadataAdapter {
            metadata: AuxStorageBackfillMetadata {
                metadata_reachable: true,
                enumeration_enabled: false,
                can_prove_deletes: true,
            },
        },
    );

    let error = discovery
        .discover(BackfillSourceKind::AuxStorage)
        .unwrap_err();

    assert!(matches!(
        error,
        BackfillPlanError::PartialCapabilitySupport {
            source_kind: BackfillSourceKind::AuxStorage,
            ..
        }
    ));
}

#[test]
fn given_adapter_backed_dynamodb_discovery_when_metadata_unreachable_then_rejected() {
    let discovery = BackfillDiscoveryAdapters::new(
        FakeDynamoDbMetadataAdapter {
            metadata: DynamoDbBackfillMetadata {
                metadata_reachable: false,
                pitr_enabled: true,
                scan_enabled: true,
                stream_retention_hours: Some(24),
            },
        },
        FakeAuxStorageMetadataAdapter::full(),
    );

    let error = discovery
        .discover(BackfillSourceKind::DynamoDb)
        .unwrap_err();

    assert!(matches!(
        error,
        BackfillPlanError::PartialCapabilitySupport {
            source_kind: BackfillSourceKind::DynamoDb,
            ..
        }
    ));
}

#[test]
fn given_adapter_backed_dynamodb_discovery_without_export_or_scan_then_rejected() {
    let discovery = BackfillDiscoveryAdapters::new(
        FakeDynamoDbMetadataAdapter {
            metadata: DynamoDbBackfillMetadata {
                metadata_reachable: true,
                pitr_enabled: false,
                scan_enabled: false,
                stream_retention_hours: Some(24),
            },
        },
        FakeAuxStorageMetadataAdapter::full(),
    );

    let error = discovery
        .discover(BackfillSourceKind::DynamoDb)
        .unwrap_err();

    assert!(matches!(
        error,
        BackfillPlanError::PartialCapabilitySupport {
            source_kind: BackfillSourceKind::DynamoDb,
            ..
        }
    ));
}

#[test]
fn given_adapter_backed_aux_storage_discovery_when_supported_then_capabilities_return() {
    let discovery = BackfillDiscoveryAdapters::new(
        FakeDynamoDbMetadataAdapter::full(),
        FakeAuxStorageMetadataAdapter::full(),
    );

    let capabilities = discovery.discover(BackfillSourceKind::AuxStorage).unwrap();

    assert_eq!(capabilities.source_kind, BackfillSourceKind::AuxStorage);
    assert!(capabilities.supports_full_enumeration);
    assert!(capabilities.can_prove_deletes);
}

#[derive(Debug, Clone)]
struct FakeDynamoDbMetadataAdapter {
    metadata: DynamoDbBackfillMetadata,
}

impl FakeDynamoDbMetadataAdapter {
    fn full() -> Self {
        Self {
            metadata: DynamoDbBackfillMetadata {
                metadata_reachable: true,
                pitr_enabled: true,
                scan_enabled: true,
                stream_retention_hours: Some(24),
            },
        }
    }
}

impl DynamoDbBackfillMetadataAdapter for FakeDynamoDbMetadataAdapter {
    fn discover_dynamodb_metadata(&self) -> crate::BackfillPlanResult<DynamoDbBackfillMetadata> {
        Ok(self.metadata.clone())
    }
}

#[derive(Debug, Clone)]
struct FakeAuxStorageMetadataAdapter {
    metadata: AuxStorageBackfillMetadata,
}

impl FakeAuxStorageMetadataAdapter {
    fn full() -> Self {
        Self {
            metadata: AuxStorageBackfillMetadata {
                metadata_reachable: true,
                enumeration_enabled: true,
                can_prove_deletes: true,
            },
        }
    }
}

impl AuxStorageBackfillMetadataAdapter for FakeAuxStorageMetadataAdapter {
    fn discover_aux_storage_metadata(
        &self,
    ) -> crate::BackfillPlanResult<AuxStorageBackfillMetadata> {
        Ok(self.metadata.clone())
    }
}
