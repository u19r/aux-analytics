use std::{
    cell::RefCell,
    collections::HashMap,
    hash::{Hash, Hasher},
};

use analytics_contract::{
    AnalyticsColumnType, AnalyticsManifest, PrimitiveColumnType, QueryColumnComparison,
    QueryComparisonOperator, QueryConditionalBranch, QueryDocumentPredicate, QueryExpression,
    QueryJoin, QueryJoinKind, QueryOrder, QueryPredicate, QuerySelect, QueryStringOperator,
    SortOrder, StructuredQuery, TableRegistration, TableScope,
};

use crate::{
    engine::{AnalyticsEngineError, AnalyticsEngineResult, columns_for_registration},
    sql,
};

mod clauses;
mod compiler;
mod compiler_resolution;
mod document;
mod metadata;
mod parameters;
mod validation;

pub(crate) use document::{
    alias_sql, document_json_sql, document_leaf_predicate_sql, escape_sql_string, literal_sql,
};
pub(crate) use metadata::{
    find_registered_table, parameter_fingerprint, prepared_metadata, prepared_result_value_type,
    select_result_source,
};
pub(crate) use validation::{
    is_aggregate_select, validate_document_column, validate_grouped_selects, validate_join_policy,
    validate_registered_column,
};

mod compiler_scope;
mod preparation;
use compiler_scope::{ExpressionSignature, QueryCompiler, QueryTable, ResolvedExpression};
pub use metadata::{
    PreparedQueryMetadata, PreparedQueryResultColumn, PreparedQueryResultValueType,
};
pub use preparation::{
    PreparedStructuredQuery, prepare_tenant_structured_query, prepare_unscoped_structured_query,
};
use validation::{reject_conflicting_tenant_predicates, validate_structured_query};
#[cfg(test)]
mod test_support;
#[cfg(test)]
pub(crate) use test_support::{
    performance_resolve_table_for_column, performance_resolve_table_for_document_path,
    structured_query_sql, structured_query_sql_for_manifest, tenant_scoped_structured_query_sql,
    tenant_scoped_structured_query_sql_for_manifest,
};

#[cfg(test)]
mod clauses_tests;
#[cfg(test)]
mod parameters_tests;
#[cfg(test)]
mod structured_query_tests;
