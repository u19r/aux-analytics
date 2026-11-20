use std::{
    fs,
    io::Read,
    path::{Component, Path, PathBuf},
};

use analytics_contract::{AnalyticsManifest, RowIdentity, StorageItem, StorageStreamRecord};
use analytics_engine::AnalyticsEngine;
use clap::{Args, Subcommand};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::error::{CliError, CliErrorDebug, CliErrorKind, CliResult};

const MAX_MANIFEST_BYTES: u64 = 4 * 1024 * 1024;
const MAX_PAGE_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Subcommand)]
pub(crate) enum RecoveryCommand {
    /// Check a completed source archive and effective analytics configuration.
    Inspect(RecoveryInput),
    /// Build a new local inspection database. Failed builds must be removed
    /// before retry.
    Build {
        #[command(flatten)]
        input: RecoveryInput,
        /// New private directory for the inspection database.
        #[arg(long)]
        destination: PathBuf,
    },
}

#[derive(Debug, Args)]
pub(crate) struct RecoveryInput {
    /// Directory containing the completed source archive.
    #[arg(long)]
    archive: PathBuf,
    /// Effective runtime configuration, including privacy and retention
    /// settings.
    #[arg(long)]
    config: PathBuf,
    /// Analytics manifest used by the deployment.
    #[arg(long)]
    manifest: PathBuf,
    /// Analytical table to rebuild from this single source table archive.
    #[arg(long)]
    table: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchiveManifest {
    schema_version: u32,
    kind: String,
    source: ArchiveSource,
    table: ArchiveTable,
    pages: Vec<ArchivePage>,
    row_count: u64,
    complete: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchiveSource {
    database_id: String,
    snapshot_id: String,
    table_name: String,
    table_id: String,
    source_config_sha256: String,
    system_stream_scope: Option<storage_types::SystemStreamScope>,
    stability: ArchiveStability,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchiveStability {
    kind: String,
    evidence_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchiveTable {
    key_schema: Vec<KeyColumn>,
    stored_table_info: serde_json::Value,
    ttl: serde_json::Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyColumn {
    #[serde(rename = "AttributeName")]
    name: String,
    #[serde(rename = "KeyType")]
    kind: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchivePage {
    file: String,
    rows: u64,
    bytes: u64,
    sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchiveRow {
    keys: StorageItem,
    item: StorageItem,
    indexers: Vec<String>,
}

struct RecoveryPlan {
    root: PathBuf,
    archive: ArchiveManifest,
    manifest: AnalyticsManifest,
    table: String,
    revision: String,
}

pub(crate) fn run_recovery_command(command: RecoveryCommand) -> CliResult<String> {
    let (input, destination) = match command {
        RecoveryCommand::Inspect(input) => (input, None),
        RecoveryCommand::Build { input, destination } => (input, Some(destination)),
    };
    let plan = RecoveryPlan::load(&input)?;
    plan.visit_rows(|_, _, _| Ok(()))?;
    let selected_rows = destination
        .as_ref()
        .map(|destination| plan.build(destination))
        .transpose()?;
    Ok(serde_json::to_string_pretty(&serde_json::json!({
        "source_table": plan.archive.source.table_name,
        "analytics_table": plan.table,
        "archive_sha256": plan.revision,
        "source_rows": plan.archive.row_count,
        "selected_rows": selected_rows,
        "built": destination.is_some(),
        "scope": "current source records for inspection; no historical restore or live cutover"
    }))?)
}

impl RecoveryPlan {
    fn load(input: &RecoveryInput) -> CliResult<Self> {
        let root = fs::canonicalize(&input.archive)
            .map_err(|_| recovery_error("archive directory could not be opened"))?;
        let manifest_path = safe_page_path(&root, "manifest.json")
            .map_err(|_| recovery_error("archive manifest path validation failed"))?;
        let archive_bytes = read_bounded(&manifest_path, MAX_MANIFEST_BYTES).map_err(|_| {
            recovery_error("archive manifest could not be read or exceeds the size limit")
        })?;
        let archive: ArchiveManifest = serde_json::from_slice(&archive_bytes).map_err(|error| {
            recovery_error(&format!(
                "archive manifest JSON decoding failed at line {} column {}",
                error.line(),
                error.column()
            ))
        })?;
        validate_archive(&archive)?;
        let config = config::resolve_config(config::ConfigRequest::cli_manifest_only(
            Some(input.config.clone()),
            Some(input.manifest.to_string_lossy().into_owned()),
            Vec::new(),
        ))
        .map_err(|_| recovery_error("analytics configuration or manifest could not be loaded"))?;
        if config.root.analytics.privacy.policy_path.is_some()
            || config.root.analytics.retention.enabled
            || !config.root.analytics.retention.tables.is_empty()
        {
            return Err(recovery_error(
                "configured privacy or retention is not supported by this rebuild",
            ));
        }
        let mut manifest = config.manifest.value;
        manifest
            .validate()
            .map_err(|_| recovery_error("analytics manifest validation failed"))?;
        manifest
            .tables
            .retain(|table| table.analytics_table_name == input.table);
        let Some(table) = manifest.tables.first() else {
            return Err(recovery_error("analytics table is not registered"));
        };
        if !table.source_table_name_prefix.as_deref().map_or(
            table.source_table_name == archive.source.table_name,
            |prefix| archive.source.table_name.starts_with(prefix),
        ) {
            return Err(recovery_error(
                "archive source table does not match the registration",
            ));
        }
        if matches!(table.row_identity, RowIdentity::RecordKey)
            || (matches!(table.row_identity, RowIdentity::StreamKeys)
                && archive.source.system_stream_scope.is_none())
            || table.row_expansion.is_some()
            || table.retention.is_some()
            || table.skip_delete
        {
            return Err(recovery_error(
                "registration requires unavailable event identity or source scope, expansion, \
                 retention, or delete history",
            ));
        }
        Ok(Self {
            root,
            archive,
            manifest,
            table: input.table.clone(),
            revision: hex::encode(Sha256::digest(&archive_bytes)),
        })
    }

    fn visit_rows(
        &self,
        mut visit: impl FnMut(usize, usize, ArchiveRow) -> CliResult<()>,
    ) -> CliResult<()> {
        let mut total = 0_u64;
        for (page_number, page) in self.archive.pages.iter().enumerate() {
            let path = safe_page_path(&self.root, &page.file)
                .map_err(|_| recovery_page_error("path validation", page_number))?;
            let bytes = read_bounded(&path, MAX_PAGE_BYTES)
                .map_err(|_| recovery_page_error("read", page_number))?;
            if bytes.len() as u64 != page.bytes
                || hex::encode(Sha256::digest(&bytes)) != page.sha256
            {
                return Err(recovery_page_error(
                    "size or checksum validation",
                    page_number,
                ));
            }
            let mut count = 0_u64;
            for line in bytes.split_inclusive(|byte| *byte == b'\n') {
                if line.last() != Some(&b'\n') || line.len() == 1 {
                    return Err(recovery_row_error("framing", page_number, count as usize));
                }
                let row: ArchiveRow =
                    serde_json::from_slice(&line[..line.len() - 1]).map_err(|error| {
                        recovery_error(&format!(
                            "archive page {} row {} JSON decoding failed at column {}",
                            page_number + 1,
                            count + 1,
                            error.column()
                        ))
                    })?;
                validate_row(&self.archive.table, &row)
                    .map_err(|_| recovery_row_error("validation", page_number, count as usize))?;
                visit(page_number, count as usize, row)?;
                count += 1;
            }
            if count != page.rows {
                return Err(recovery_page_error("row count validation", page_number));
            }
            total = total
                .checked_add(count)
                .ok_or_else(|| recovery_error("archive row count overflow"))?;
        }
        if total != self.archive.row_count {
            return Err(recovery_error("archive total row count does not match"));
        }
        Ok(())
    }

    fn build(&self, destination: &Path) -> CliResult<u64> {
        let parent = destination
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let parent = fs::canonicalize(parent)
            .map_err(|_| recovery_error("destination parent could not be opened"))?;
        if parent.starts_with(&self.root) {
            return Err(recovery_error(
                "destination must be outside the source archive",
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            fs::DirBuilder::new()
                .mode(0o700)
                .create(destination)
                .map_err(|_| {
                    recovery_error("private destination directory could not be created")
                })?;
        }
        #[cfg(not(unix))]
        return Err(recovery_error(
            "private inspection directories require Unix permissions",
        ));
        let database = destination.join("analytics.duckdb");
        let engine = AnalyticsEngine::connect_duckdb(database.to_string_lossy().as_ref())
            .map_err(|_| recovery_error("destination database could not be opened"))?;
        engine
            .ensure_manifest(&self.manifest)
            .map_err(|_| recovery_error("destination schema initialization failed"))?;
        let write_batch = |records, (start_page, start_row), (end_page, end_row)| {
            engine
                .recover_inspection_page(
                    &self.manifest,
                    &self.table,
                    &self.archive.source.table_name,
                    self.archive.source.system_stream_scope.as_ref(),
                    records,
                )
                .map(|count| count as u64)
                .map_err(|_| recovery_batch_error(start_page, start_row, end_page, end_row))
        };
        let mut records = Vec::with_capacity(256);
        let mut selected_rows = 0_u64;
        let mut batch_start = None;
        let mut batch_end = (0, 0);
        self.visit_rows(|page, row, value| {
            let position = (page, row);
            let start = *batch_start.get_or_insert(position);
            batch_end = position;
            records.push(snapshot_record(value));
            if records.len() == 256 {
                selected_rows += write_batch(std::mem::take(&mut records), start, position)?;
                batch_start = None;
            }
            Ok(())
        })?;
        if let Some(start) = batch_start {
            selected_rows += write_batch(records, start, batch_end)?;
        }
        let sql = format!(
            "SELECT count(*) AS row_count FROM \"{}\"",
            self.table.replace('"', "\"\"")
        );
        let rows = engine
            .query_unscoped_sql_json(&sql)
            .map_err(|_| recovery_error("destination row count query failed"))?;
        if rows
            .first()
            .and_then(|value| value.get("row_count"))
            .and_then(serde_json::Value::as_u64)
            != Some(selected_rows)
        {
            return Err(recovery_error(
                "destination row count does not match the archive",
            ));
        }
        Ok(selected_rows)
    }
}

fn snapshot_record(row: ArchiveRow) -> StorageStreamRecord {
    StorageStreamRecord {
        keys: row.keys,
        sequence_number: String::new(),
        old_image: None,
        new_image: Some(row.item),
    }
}

fn validate_archive(archive: &ArchiveManifest) -> CliResult<()> {
    let source = &archive.source;
    if archive.schema_version != 1
        || archive.kind != "aux_storage_table_snapshot"
        || !archive.complete
        || [
            &source.database_id,
            &source.snapshot_id,
            &source.table_name,
            &source.table_id,
            &source.stability.evidence_id,
        ]
        .iter()
        .any(|value| value.trim().is_empty())
        || !is_sha256(&source.source_config_sha256)
        || !matches!(
            source.stability.kind.as_str(),
            "immutable_clone" | "quiesced_live"
        )
        || !archive.table.stored_table_info.is_object()
        || !archive.table.ttl.is_object()
    {
        return Err(recovery_error(
            "archive manifest is incomplete or unsupported",
        ));
    }
    let keys = &archive.table.key_schema;
    if keys.is_empty()
        || keys.len() > 2
        || keys.iter().filter(|key| key.kind == "HASH").count() != 1
        || keys
            .iter()
            .any(|key| key.name.is_empty() || !matches!(key.kind.as_str(), "HASH" | "RANGE"))
        || (keys.len() == 2 && keys[0].name == keys[1].name)
    {
        return Err(recovery_error("archive key schema is invalid"));
    }
    let mut names = std::collections::HashSet::new();
    for page in &archive.pages {
        if !names.insert(&page.file)
            || page.rows == 0
            || page.bytes > MAX_PAGE_BYTES
            || !is_sha256(&page.sha256)
        {
            return Err(recovery_error("archive page description is invalid"));
        }
    }
    Ok(())
}

fn validate_row(table: &ArchiveTable, row: &ArchiveRow) -> CliResult<()> {
    if row.keys.len() != table.key_schema.len()
        || row.indexers.iter().any(|name| name.trim().is_empty())
    {
        return Err(recovery_error("archive row key or indexer is invalid"));
    }
    for key in &table.key_schema {
        let value = row
            .keys
            .get(&key.name)
            .ok_or_else(|| recovery_error("archive row is missing a primary key"))?;
        if !matches!(
            value,
            analytics_contract::StorageValue::S(_)
                | analytics_contract::StorageValue::N(_)
                | analytics_contract::StorageValue::B(_)
        ) || row.item.get(&key.name) != Some(value)
        {
            return Err(recovery_error(
                "archive row primary key does not match its item",
            ));
        }
    }
    Ok(())
}

fn safe_page_path(root: &Path, name: &str) -> CliResult<PathBuf> {
    let path = Path::new(name);
    if path
        .components()
        .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(recovery_error("archive page path is invalid"));
    }
    let resolved = fs::canonicalize(root.join(path))?;
    if !resolved.starts_with(root) || !resolved.is_file() {
        return Err(recovery_error("archive page is outside its directory"));
    }
    Ok(resolved)
}

fn read_bounded(path: &Path, limit: u64) -> CliResult<Vec<u8>> {
    let mut bytes = Vec::new();
    fs::File::open(path)?
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(recovery_error("archive file exceeds the size limit"));
    }
    Ok(bytes)
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn recovery_error(message: &str) -> CliError {
    CliError::with_debug(
        CliErrorKind::Operations,
        CliErrorDebug::Message(message.to_owned()),
    )
}

fn recovery_page_error(stage: &str, page: usize) -> CliError {
    recovery_error(&format!("archive page {} {stage} failed", page + 1))
}

fn recovery_row_error(stage: &str, page: usize, row: usize) -> CliError {
    recovery_error(&format!(
        "archive page {} row {} {stage} failed",
        page + 1,
        row + 1
    ))
}

fn recovery_batch_error(
    start_page: usize,
    start_row: usize,
    end_page: usize,
    end_row: usize,
) -> CliError {
    recovery_error(&format!(
        "archive batch projection, write, or verification failed for page {} row {} through page \
         {} row {}",
        start_page + 1,
        start_row + 1,
        end_page + 1,
        end_row + 1
    ))
}
