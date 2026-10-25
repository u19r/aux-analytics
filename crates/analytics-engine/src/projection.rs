use std::{borrow::Cow, fmt};

use analytics_contract::{ProjectionColumn, StorageItem, StorageValue, TableRegistration};
use lru_ttl_cache::CacheRead;
use storage_types::VecMap;
use thiserror::Error;

use crate::cache::EngineCaches;

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

#[derive(Debug, PartialEq, Eq)]
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

#[derive(Clone)]
pub(crate) struct CachedProjection {
    aliases: Option<VecMap<String, String>>,
    pub(crate) columns: Vec<(ProjectionColumn, Vec<PathSegment>)>,
}

impl CachedProjection {
    fn is_current_for_table(&self, table: &TableRegistration) -> bool {
        let columns = table.projection_columns.as_deref().unwrap_or_default();
        self.aliases == table.expression_attribute_names
            && self.columns.len() == columns.len()
            && self
                .columns
                .iter()
                .zip(columns)
                .all(|((compiled, _), column)| compiled == column)
    }

    fn compile_columns(table: &TableRegistration) -> ProjectionResult<Self> {
        let columns = table.projection_columns.as_deref().unwrap_or_default();
        let columns = columns
            .iter()
            .map(|column| {
                parse_attribute_path(
                    &column.attribute_path,
                    table.expression_attribute_names.as_ref(),
                )
                .map(|segments| (column.clone(), segments))
            })
            .collect::<ProjectionResult<_>>()?;
        Ok(Self {
            aliases: table.expression_attribute_names.clone(),
            columns,
        })
    }

    pub(crate) fn compile_for_table(
        table: &TableRegistration,
        caches: &mut EngineCaches,
    ) -> ProjectionResult<CacheRead<Self>> {
        if let Some(cached) = caches.projections.get_ref(&table.analytics_table_name)
            && cached.is_current_for_table(table)
        {
            return Ok(cached);
        }
        let projection = Self::compile_columns(table)?;
        Ok(caches
            .projections
            .insert_ref(table.analytics_table_name.clone(), projection)
            .expect("projection cache has nonzero capacity"))
    }
}

pub(crate) fn parse_attribute_path(
    path: &str,
    expression_attribute_names: Option<&VecMap<String, String>>,
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
    expression_attribute_names: Option<&VecMap<String, String>>,
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

pub(crate) fn extract_from_item<'item>(
    item: &'item StorageItem,
    segments: &[PathSegment],
) -> Option<Cow<'item, StorageValue>> {
    let (first, rest) = segments.split_first()?;
    let value = item.get(&first.key)?;
    if first.expand {
        extract_from_list(value, rest)
    } else {
        extract_from_value(value, rest)
    }
}

fn extract_from_value<'item>(
    value: &'item StorageValue,
    segments: &[PathSegment],
) -> Option<Cow<'item, StorageValue>> {
    if segments.is_empty() {
        return Some(Cow::Borrowed(value));
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

fn extract_from_list<'item>(
    value: &'item StorageValue,
    segments: &[PathSegment],
) -> Option<Cow<'item, StorageValue>> {
    let StorageValue::L(list) = value else {
        return None;
    };
    if segments.is_empty() {
        return Some(Cow::Borrowed(value));
    }
    let mut results = Vec::new();
    for element in list {
        if let Some(value) = extract_from_value(element, segments) {
            results.push(value.into_owned());
        }
    }
    Some(Cow::Owned(StorageValue::L(results)))
}
