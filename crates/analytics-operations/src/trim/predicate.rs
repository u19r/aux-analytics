//! Bounded deletion predicates compiled into one parameterized SQL buffer.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::{TrimError, TrimResult};
use crate::constants::{MAX_TRIM_PREDICATE_DEPTH, MAX_TRIM_PREDICATE_NODES};

/// Closed, bounded predicate language for row deletion.
///
/// Columns are validated identifiers and literals are always supplied as
/// bound parameters. There is deliberately no SQL fragment variant: callers
/// cannot add a statement, comment, table reference, or unbounded expression
/// through this contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub enum TrimPredicate {
    Eq {
        column: String,
        value: TrimPredicateValue,
    },
    NotEq {
        column: String,
        value: TrimPredicateValue,
    },
    Gt {
        column: String,
        value: TrimPredicateValue,
    },
    Gte {
        column: String,
        value: TrimPredicateValue,
    },
    Lt {
        column: String,
        value: TrimPredicateValue,
    },
    Lte {
        column: String,
        value: TrimPredicateValue,
    },
    IsNull {
        column: String,
    },
    IsNotNull {
        column: String,
    },
    All {
        #[schema(no_recursion)]
        predicates: Vec<Self>,
    },
    Any {
        #[schema(no_recursion)]
        predicates: Vec<Self>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
#[schemars(deny_unknown_fields)]
pub enum TrimPredicateValue {
    Text(String),
    Integer(i64),
    Boolean(bool),
}

impl TrimPredicate {
    pub fn validate(&self) -> TrimResult<()> {
        let mut nodes = 0;
        self.validate_at(0, &mut nodes)
    }

    fn validate_at(&self, depth: usize, nodes: &mut usize) -> TrimResult<()> {
        if depth > MAX_TRIM_PREDICATE_DEPTH {
            return Err(TrimError::UnsafePredicate);
        }
        *nodes = nodes.saturating_add(1);
        if *nodes > MAX_TRIM_PREDICATE_NODES {
            return Err(TrimError::UnsafePredicate);
        }
        match self {
            Self::Eq { column, .. }
            | Self::NotEq { column, .. }
            | Self::Gt { column, .. }
            | Self::Gte { column, .. }
            | Self::Lt { column, .. }
            | Self::Lte { column, .. }
            | Self::IsNull { column }
            | Self::IsNotNull { column } => validate_predicate_column(column),
            Self::All { predicates } | Self::Any { predicates } => {
                if predicates.is_empty() {
                    return Err(TrimError::EmptyPredicate);
                }
                for predicate in predicates {
                    predicate.validate_at(depth + 1, nodes)?;
                }
                Ok(())
            }
        }
    }

    pub(super) fn compile(&self, values: &mut Vec<duckdb::types::Value>) -> TrimResult<String> {
        self.validate()?;
        let mut sql = String::new();
        self.append_compiled(&mut sql, values);
        Ok(sql)
    }

    fn append_compiled(&self, sql: &mut String, values: &mut Vec<duckdb::types::Value>) {
        match self {
            Self::Eq { column, value } => value.append_comparison(sql, column, "=", values),
            Self::NotEq { column, value } => value.append_comparison(sql, column, "<>", values),
            Self::Gt { column, value } => value.append_comparison(sql, column, ">", values),
            Self::Gte { column, value } => value.append_comparison(sql, column, ">=", values),
            Self::Lt { column, value } => value.append_comparison(sql, column, "<", values),
            Self::Lte { column, value } => value.append_comparison(sql, column, "<=", values),
            Self::IsNull { column } => append_column_suffix(sql, column, " IS NULL"),
            Self::IsNotNull { column } => append_column_suffix(sql, column, " IS NOT NULL"),
            Self::All { predicates } => append_group(sql, predicates, " AND ", values),
            Self::Any { predicates } => append_group(sql, predicates, " OR ", values),
        }
    }
}

impl TrimPredicateValue {
    fn push_duckdb_value(&self, values: &mut Vec<duckdb::types::Value>) {
        values.push(match self {
            Self::Text(value) => duckdb::types::Value::Text(value.clone()),
            Self::Integer(value) => duckdb::types::Value::BigInt(*value),
            Self::Boolean(value) => duckdb::types::Value::Boolean(*value),
        });
    }
}

fn validate_predicate_column(column: &str) -> TrimResult<()> {
    if !column.is_empty()
        && column
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Ok(());
    }
    Err(TrimError::UnsafePredicate)
}

fn append_column_suffix(sql: &mut String, column: &str, suffix: &str) {
    // Validation restricts columns to ASCII letters, digits, and underscores.
    sql.push('"');
    sql.push_str(column);
    sql.push('"');
    sql.push_str(suffix);
}

fn append_group(
    sql: &mut String,
    predicates: &[TrimPredicate],
    separator: &str,
    values: &mut Vec<duckdb::types::Value>,
) {
    sql.push('(');
    for (index, predicate) in predicates.iter().enumerate() {
        if index > 0 {
            sql.push_str(separator);
        }
        predicate.append_compiled(sql, values);
    }
    sql.push(')');
}

impl TrimPredicateValue {
    fn append_comparison(
        &self,
        sql: &mut String,
        column: &str,
        operator: &str,
        values: &mut Vec<duckdb::types::Value>,
    ) {
        self.push_duckdb_value(values);
        append_column_suffix(sql, column, " ");
        sql.push_str(operator);
        sql.push_str(" ?");
    }
}
