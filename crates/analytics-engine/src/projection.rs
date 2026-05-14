use std::{collections::HashMap, fmt};

use analytics_contract::{ProjectionColumn, StorageItem, StorageValue};
use thiserror::Error;

pub(crate) type ProjectionResult<T> = Result<T, ProjectionError>;

#[derive(Debug, Error)]
#[error("{kind}{debug}")]
pub struct ProjectionError {
    kind: ProjectionErrorKind,
    debug: ProjectionErrorDebug,
}

impl ProjectionError {
    fn with_path(kind: ProjectionErrorKind, path: &str) -> Self {
        Self {
            kind,
            debug: ProjectionErrorDebug::Path(path.to_string()),
        }
    }

    fn with_name(kind: ProjectionErrorKind, name: &str) -> Self {
        Self {
            kind,
            debug: ProjectionErrorDebug::AttributeName(name.to_string()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProjectionErrorKind {
    EmptyAttributePath,
    InvalidAttributePath,
    MissingExpressionAttributeName,
    MissingExpressionAttributeNames,
}

impl fmt::Display for ProjectionErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyAttributePath => write!(f, "projection attribute path is empty"),
            Self::InvalidAttributePath => write!(f, "projection attribute path is invalid"),
            Self::MissingExpressionAttributeName => {
                write!(
                    f,
                    "projection attribute name is not found in expression attribute names"
                )
            }
            Self::MissingExpressionAttributeNames => {
                write!(
                    f,
                    "projection attribute name requires expression attribute names"
                )
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProjectionErrorDebug {
    Path(String),
    AttributeName(String),
}

impl fmt::Display for ProjectionErrorDebug {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Path(path) => write!(f, ": path={path}"),
            Self::AttributeName(name) => write!(f, ": attribute_name={name}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PathSegment {
    key: String,
    expand: bool,
}

pub(crate) fn project_item(
    item: &StorageItem,
    columns: &[ProjectionColumn],
    expression_attribute_names: Option<&HashMap<String, String>>,
) -> ProjectionResult<StorageItem> {
    let mut projected = HashMap::new();
    for column in columns {
        let segments = parse_attribute_path(&column.attribute_path, expression_attribute_names)?;
        if let Some(value) = extract_from_item(item, &segments) {
            projected.insert(column.column_name.clone(), value);
        }
    }
    Ok(projected)
}

pub(crate) fn parse_attribute_path(
    path: &str,
    expression_attribute_names: Option<&HashMap<String, String>>,
) -> ProjectionResult<Vec<PathSegment>> {
    if path.trim().is_empty() {
        return Err(ProjectionError::with_path(
            ProjectionErrorKind::EmptyAttributePath,
            path,
        ));
    }
    let mut segments = Vec::new();
    for raw_segment in path.split('.') {
        if raw_segment.is_empty() {
            return Err(ProjectionError::with_path(
                ProjectionErrorKind::InvalidAttributePath,
                path,
            ));
        }
        let (key, expand) = if let Some(key) = raw_segment.strip_suffix("[]") {
            if key.is_empty() || key.contains("[]") {
                return Err(ProjectionError::with_path(
                    ProjectionErrorKind::InvalidAttributePath,
                    path,
                ));
            }
            (key, true)
        } else if raw_segment.contains("[]") {
            return Err(ProjectionError::with_path(
                ProjectionErrorKind::InvalidAttributePath,
                path,
            ));
        } else {
            (raw_segment, false)
        };
        let resolved_key = resolve_attribute_name(key, expression_attribute_names)?;
        segments.push(PathSegment {
            key: resolved_key,
            expand,
        });
    }
    Ok(segments)
}

fn resolve_attribute_name(
    name: &str,
    expression_attribute_names: Option<&HashMap<String, String>>,
) -> ProjectionResult<String> {
    if let Some(names) = expression_attribute_names {
        if let Some(resolved) = names.get(name) {
            Ok(resolved.clone())
        } else if name.starts_with('#') {
            Err(ProjectionError::with_name(
                ProjectionErrorKind::MissingExpressionAttributeName,
                name,
            ))
        } else {
            Ok(name.to_string())
        }
    } else if name.starts_with('#') {
        Err(ProjectionError::with_name(
            ProjectionErrorKind::MissingExpressionAttributeNames,
            name,
        ))
    } else {
        Ok(name.to_string())
    }
}

pub(crate) fn extract_from_item(
    item: &StorageItem,
    segments: &[PathSegment],
) -> Option<StorageValue> {
    let (first, rest) = segments.split_first()?;
    let value = item.get(&first.key)?;
    if first.expand {
        extract_from_list(value, rest)
    } else {
        extract_from_value(value, rest)
    }
}

fn extract_from_value(value: &StorageValue, segments: &[PathSegment]) -> Option<StorageValue> {
    if segments.is_empty() {
        return Some(value.clone());
    }
    let (next, rest) = segments.split_first()?;
    match value {
        StorageValue::M(map) => {
            let child = map.get(&next.key)?;
            if next.expand {
                extract_from_list(child, rest)
            } else {
                extract_from_value(child, rest)
            }
        }
        _ => None,
    }
}

fn extract_from_list(value: &StorageValue, segments: &[PathSegment]) -> Option<StorageValue> {
    let StorageValue::L(list) = value else {
        return None;
    };
    if segments.is_empty() {
        return Some(StorageValue::L(list.clone()));
    }
    let mut results = Vec::new();
    for element in list {
        if let Some(value) = extract_from_value(element, segments) {
            results.push(value);
        }
    }
    Some(StorageValue::L(results))
}
