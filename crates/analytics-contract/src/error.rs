#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ManifestValidationError {
    #[error("manifest version {actual} is not supported; expected {expected}")]
    UnsupportedVersion { actual: u32, expected: u32 },
    #[error("manifest field {0} must not be empty")]
    EmptyField(&'static str),
    #[error("duplicate analytics table name {0}")]
    DuplicateAnalyticsTableName(String),
    #[error("duplicate column {column} in analytics table {table}")]
    DuplicateColumnName { table: String, column: String },
    #[error("global reference table {table} must not declare tenant scope")]
    GlobalReferenceHasTenantScope { table: String },
    #[error("join policy for analytics table {table} is invalid: {reason}")]
    InvalidJoinPolicy { table: String, reason: &'static str },
    #[error("row identity is invalid: {reason}")]
    InvalidRowIdentity { reason: &'static str },
    #[error("row expansion for analytics table {table} is invalid: {reason}")]
    InvalidRowExpansion { table: String, reason: &'static str },
    #[error("row expansion attribute {attribute} is duplicated in analytics table {table}")]
    DuplicateRowExpansionAttribute { table: String, attribute: String },
    #[error("row expansion output {attribute} required by analytics table {table} is not declared")]
    UnknownRowExpansionOutput { table: String, attribute: String },
    #[error("column {column} in analytics table {table} is reserved by aux-analytics")]
    ReservedOutputColumn { table: String, column: String },
    #[error("layout column {column} is not declared in analytics table {table}")]
    UnknownLayoutColumn { table: String, column: String },
    #[error("retention period for analytics table {table} must be greater than zero")]
    InvalidRetentionPeriod { table: String },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StructuredQueryValidationError {
    #[error("document predicate exceeds the maximum nesting depth of {maximum}")]
    DocumentPredicateTooDeep { maximum: usize },
    #[error("query alias {0} is declared more than once")]
    DuplicateAlias(String),
    #[error("conditional query branch must contain at least one comparison")]
    EmptyConditionalBranch,
    #[error("conditional query expression must contain at least one branch")]
    EmptyConditionalExpression,
    #[error("document predicate branch must not be empty")]
    EmptyDocumentPredicate,
    #[error("query field {0} must not be empty")]
    EmptyField(&'static str),
    #[error("structured query must select at least one field")]
    EmptySelect,
    #[error("conditional query result literals must have the same scalar type")]
    IncompatibleConditionalLiteral,
    #[error("query field {field} has invalid identifier {value}")]
    InvalidIdentifier { field: &'static str, value: String },
    #[error("document path {0} is invalid")]
    InvalidPath(String),
    #[error("join {0} must declare at least one on predicate")]
    MissingJoinPredicate(String),
    #[error("conditional query expression has {actual} branches but supports at most {maximum}")]
    TooManyConditionalBranches { actual: usize, maximum: usize },
    #[error("conditional query branch has {actual} comparisons but supports at most {maximum}")]
    TooManyConditionalComparisons { actual: usize, maximum: usize },
    #[error("document predicate has {actual} branches but supports at most {maximum}")]
    TooManyDocumentPredicateBranches { actual: usize, maximum: usize },
    #[error("document predicate has {actual} nodes but supports at most {maximum}")]
    TooManyDocumentPredicateNodes { actual: usize, maximum: usize },
    #[error("structured query has {actual} joins but supports at most {maximum}")]
    TooManyJoins { actual: usize, maximum: usize },
    #[error("structured query {field} value {actual} exceeds the maximum of {maximum}")]
    QueryBoundExceeded {
        field: &'static str,
        actual: u32,
        maximum: u32,
    },
    #[error("conditional query result literals must be strings, numbers, or booleans")]
    UnsupportedConditionalLiteral,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageValueJsonError {
    UnsupportedNullFalse,
}

impl std::fmt::Display for StorageValueJsonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedNullFalse => write!(f, "NULL(false) cannot be converted to JSON"),
        }
    }
}

impl std::error::Error for StorageValueJsonError {}
