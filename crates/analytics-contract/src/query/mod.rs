use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub(super) use crate::StructuredQueryValidationError;
use crate::constants::*;

mod request;
pub use request::StructuredQuery;

mod canonical;
mod document;
mod expression;
mod order;
mod validation;

pub(super) use canonical::hex_lower;
pub use document::{QueryDocumentPredicate, QueryPredicate, QueryStringOperator};
pub use expression::{
    QueryColumnComparison, QueryComparisonOperator, QueryConditionalBranch, QueryExpression,
    QueryJoin, QueryJoinKind, QueryJoinPredicate, QuerySelect,
};
pub use order::QueryOrder;
pub(super) use validation::{
    validate_optional_query_identifier, validate_query_identifier, validate_query_path,
};

#[cfg(test)]
mod query_tests;

#[cfg(test)]
mod query_canonical_tests;
