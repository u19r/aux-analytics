#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestValidationError {
    UnsupportedVersion { actual: u32, expected: u32 },
    EmptyField(&'static str),
    DuplicateAnalyticsTableName(String),
    DuplicateColumnName { table: String, column: String },
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
    EmptyField(&'static str),
    EmptySelect,
    InvalidPath(String),
}

impl std::fmt::Display for StructuredQueryValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyField(field) => write!(f, "query field {field} must not be empty"),
            Self::EmptySelect => write!(f, "structured query must select at least one field"),
            Self::InvalidPath(path) => write!(f, "document path {path} is invalid"),
        }
    }
}

impl std::error::Error for StructuredQueryValidationError {}
