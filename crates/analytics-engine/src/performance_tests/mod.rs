use std::{
    collections::BTreeMap,
    hint::black_box,
    time::{Duration, Instant},
};

use analytics_contract::{
    AnalyticsColumnType, AnalyticsManifest, PrimitiveColumnType, ProjectionColumn, QueryExpression,
    QueryOrder, QuerySelect, RowIdentity, StorageItem, StorageStreamRecord, StorageValue,
    StructuredQuery, TableRegistration, TenantSelector,
};
use storage_types::VecMap;
use test_support::*;

use crate::{
    AnalyticsEngine, IngestOutcome, StreamRecordBatchItem,
    constants::{
        CANDIDATE_REPETITIONS, MIN_ALLOCATION_CPU_DELTA_PERCENT, PRODUCTION_PAIRED_SAMPLES,
        PRODUCTION_TIMING_TRIALS, PRODUCTION_WARMUP_RUNS, REALISTIC_ATTRIBUTE_COUNT,
        REALISTIC_PAYLOAD_BYTES, REALISTIC_QUERY_LIMITS, REALISTIC_RECORD_COUNT,
    },
    engine::columns_for_registration,
    row::RowSource,
    sql::quote_identifier,
};

#[cfg(test)]
mod row_materialization_tests;
mod source_position_tests;
mod test_support;
mod workload_tests;

#[cfg(test)]
mod condition_tests;

#[cfg(test)]
mod query_materialization_tests;

#[cfg(test)]
mod digest_tests;
