#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestValidationError {
    UnsupportedVersion { actual: u32, expected: u32 },
    EmptyField(&'static str),
    DuplicateAnalyticsTableName(String),
    DuplicateColumnName { table: String, column: String },
    GlobalReferenceHasTenantScope { table: String },
    InvalidJoinPolicy { table: String, reason: &'static str },
    ReservedOutputColumn { table: String, column: String },
    UnknownLayoutColumn { table: String, column: String },
    InvalidRetentionPeriod { table: String },
}

impl std::fmt::Display for ManifestValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedVersion { actual, expected } => {
                write!(
                    f,
                    "manifest version {actual} is not supported; expected {expected}"
                )
            }
            Self::EmptyField(field) => write!(f, "manifest field {field} must not be empty"),
            Self::DuplicateAnalyticsTableName(table) => {
                write!(f, "duplicate analytics table name {table}")
            }
            Self::DuplicateColumnName { table, column } => {
                write!(f, "duplicate column {column} in analytics table {table}")
            }
            Self::GlobalReferenceHasTenantScope { table } => {
                write!(
                    f,
                    "global reference table {table} must not declare tenant scope"
                )
            }
            Self::InvalidJoinPolicy { table, reason } => {
                write!(
                    f,
                    "join policy for analytics table {table} is invalid: {reason}"
                )
            }
            Self::ReservedOutputColumn { table, column } => {
                write!(
                    f,
                    "column {column} in analytics table {table} is reserved by aux-analytics"
                )
            }
            Self::UnknownLayoutColumn { table, column } => {
                write!(
                    f,
                    "layout column {column} is not declared in analytics table {table}"
                )
            }
            Self::InvalidRetentionPeriod { table } => {
                write!(
                    f,
                    "retention period for analytics table {table} must be greater than zero"
                )
            }
        }
    }
}

impl std::error::Error for ManifestValidationError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StructuredQueryValidationError {
    DuplicateAlias(String),
    EmptyConditionalBranch,
    EmptyConditionalExpression,
    EmptyField(&'static str),
    EmptySelect,
    IncompatibleConditionalLiteral,
    InvalidIdentifier { field: &'static str, value: String },
    InvalidPath(String),
    MissingJoinPredicate(String),
    TooManyConditionalBranches { actual: usize, maximum: usize },
    TooManyConditionalComparisons { actual: usize, maximum: usize },
    TooManyJoins { actual: usize, maximum: usize },
    UnsupportedConditionalLiteral,
}

impl std::fmt::Display for StructuredQueryValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateAlias(alias) => {
                write!(f, "query alias {alias} is declared more than once")
            }
            Self::EmptyConditionalBranch => {
                write!(
                    f,
                    "conditional query branch must contain at least one comparison"
                )
            }
            Self::EmptyConditionalExpression => {
                write!(
                    f,
                    "conditional query expression must contain at least one branch"
                )
            }
            Self::EmptyField(field) => write!(f, "query field {field} must not be empty"),
            Self::EmptySelect => write!(f, "structured query must select at least one field"),
            Self::IncompatibleConditionalLiteral => write!(
                f,
                "conditional query result literals must have the same scalar type"
            ),
            Self::InvalidIdentifier { field, value } => {
                write!(f, "query field {field} has invalid identifier {value}")
            }
            Self::InvalidPath(path) => write!(f, "document path {path} is invalid"),
            Self::MissingJoinPredicate(alias) => {
                write!(f, "join {alias} must declare at least one on predicate")
            }
            Self::TooManyConditionalBranches { actual, maximum } => write!(
                f,
                "conditional query expression has {actual} branches but supports at most {maximum}"
            ),
            Self::TooManyConditionalComparisons { actual, maximum } => write!(
                f,
                "conditional query branch has {actual} comparisons but supports at most {maximum}"
            ),
            Self::TooManyJoins { actual, maximum } => {
                write!(
                    f,
                    "structured query has {actual} joins but supports at most {maximum}"
                )
            }
            Self::UnsupportedConditionalLiteral => write!(
                f,
                "conditional query result literals must be strings, numbers, or booleans"
            ),
        }
    }
}

impl std::error::Error for StructuredQueryValidationError {}
