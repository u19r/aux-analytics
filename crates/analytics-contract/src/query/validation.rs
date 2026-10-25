use sha2::{Digest, Sha256};

use super::*;

impl StructuredQuery {
    #[must_use]
    pub const fn effective_limit(&self) -> u32 {
        match self.limit {
            Some(limit) => limit,
            None => DEFAULT_STRUCTURED_QUERY_LIMIT,
        }
    }

    pub fn canonical_json(&self) -> Result<String, serde_json::Error> {
        // Keep the serde conversion as the public contract boundary, then sort
        // that single value in place before emitting canonical bytes.
        let mut value = serde_json::to_value(self)?;
        value.sort_all_objects();
        serde_json::to_string(&value)
    }

    pub fn query_hash(&self) -> Result<String, serde_json::Error> {
        let canonical_json = self.canonical_json()?;
        let digest = Sha256::digest(canonical_json.as_bytes());
        Ok(format!("sha256:{}", hex_lower(digest.as_ref())))
    }

    pub fn validate_shape(&self) -> Result<(), StructuredQueryValidationError> {
        validate_query_identifier("analytics_table_name", self.analytics_table_name.as_str())?;
        if let Some(table_alias) = self.table_alias.as_deref() {
            validate_query_identifier("table_alias", table_alias)?;
        }
        validate_query_bound("limit", self.limit)?;
        validate_query_bound("offset", self.offset)?;
        validate_join_count(self.joins.len())?;
        let mut aliases = std::collections::HashSet::new();
        let primary_alias = self
            .table_alias
            .as_deref()
            .unwrap_or(self.analytics_table_name.as_str());
        insert_unique_alias(&mut aliases, primary_alias)?;
        for join in &self.joins {
            join.validate_shape()?;
            insert_unique_alias(&mut aliases, join.table_alias.as_str())?;
        }
        if self.select.is_empty() {
            return Err(StructuredQueryValidationError::EmptySelect);
        }
        let mut output_aliases = std::collections::HashSet::new();
        for select in &self.select {
            select.validate_shape()?;
            insert_unique_alias(&mut output_aliases, select.output_alias().as_str())?;
        }
        for filter in &self.filters {
            filter.validate_shape()?;
        }
        for expression in &self.group_by {
            expression.validate_shape()?;
        }
        for order in &self.order_by {
            order.validate_shape()?;
        }
        Ok(())
    }
}

fn validate_query_bound(
    field: &'static str,
    value: Option<u32>,
) -> Result<(), StructuredQueryValidationError> {
    if let Some(actual) = value.filter(|value| *value > MAX_STRUCTURED_QUERY_BOUND) {
        return Err(StructuredQueryValidationError::QueryBoundExceeded {
            field,
            actual,
            maximum: MAX_STRUCTURED_QUERY_BOUND,
        });
    }
    Ok(())
}

fn validate_query_non_empty(
    field: &'static str,
    value: &str,
) -> Result<(), StructuredQueryValidationError> {
    if value.trim().is_empty() {
        return Err(StructuredQueryValidationError::EmptyField(field));
    }
    Ok(())
}

pub fn validate_optional_query_identifier(
    field: &'static str,
    value: Option<&str>,
) -> Result<(), StructuredQueryValidationError> {
    if let Some(value) = value {
        validate_query_identifier(field, value)?;
    }
    Ok(())
}

pub fn validate_query_identifier(
    field: &'static str,
    value: &str,
) -> Result<(), StructuredQueryValidationError> {
    validate_query_non_empty(field, value)?;
    if value.len() > MAX_QUERY_IDENTIFIER_LEN {
        return Err(StructuredQueryValidationError::InvalidIdentifier {
            field,
            value: value.to_string(),
        });
    }
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err(StructuredQueryValidationError::EmptyField(field));
    };
    if !(first == '_' || first.is_ascii_alphabetic())
        || chars.any(|character| !(character == '_' || character.is_ascii_alphanumeric()))
    {
        return Err(StructuredQueryValidationError::InvalidIdentifier {
            field,
            value: value.to_string(),
        });
    }
    Ok(())
}

fn validate_join_count(count: usize) -> Result<(), StructuredQueryValidationError> {
    if count > MAX_STRUCTURED_QUERY_JOINS {
        return Err(StructuredQueryValidationError::TooManyJoins {
            actual: count,
            maximum: MAX_STRUCTURED_QUERY_JOINS,
        });
    }
    Ok(())
}

fn insert_unique_alias(
    aliases: &mut std::collections::HashSet<String>,
    alias: &str,
) -> Result<(), StructuredQueryValidationError> {
    validate_query_identifier("alias", alias)?;
    if aliases.insert(alias.to_string()) {
        return Ok(());
    }
    Err(StructuredQueryValidationError::DuplicateAlias(
        alias.to_string(),
    ))
}

pub fn validate_query_path(path: &str) -> Result<(), StructuredQueryValidationError> {
    validate_query_non_empty("document path", path)?;
    if path.split('.').any(|segment| segment.trim().is_empty()) {
        return Err(StructuredQueryValidationError::InvalidPath(
            path.to_string(),
        ));
    }
    Ok(())
}
