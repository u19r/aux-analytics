use analytics_contract::{
    AnalyticsColumn, ClusteringKey, INTERNAL_EXPIRY_COLUMN, INTERNAL_INGESTED_AT_COLUMN,
    INTERNAL_MISSING_RETENTION_COLUMN, PartitionKey, SortOrder,
};
use config::{AnalyticsObjectStorageConfig, CatalogType, RemoteCredentialsConfig, StorageBackend};
use duckdb::Connection;

use crate::AnalyticsEngineResult;

pub(crate) fn configure_connection(
    conn: &Connection,
    backend: &StorageBackend,
) -> AnalyticsEngineResult<()> {
    conn.execute("LOAD json;", [])?;
    if let StorageBackend::DuckLake {
        catalog,
        catalog_path,
        data_path,
        object_storage,
    } = backend
    {
        conn.execute("LOAD httpfs;", [])?;
        conn.execute("LOAD ducklake;", [])?;
        match catalog {
            CatalogType::Sqlite => conn.execute("LOAD sqlite;", [])?,
            CatalogType::Postgres => conn.execute("LOAD postgres;", [])?,
        };
        if let Some(object_storage) = object_storage {
            configure_object_storage(conn, object_storage)?;
        }
        conn.execute(
            attach_ducklake(*catalog, catalog_path, data_path).as_str(),
            [],
        )?;
        conn.execute("USE dlake;", [])?;
    }
    Ok(())
}

fn configure_object_storage(
    conn: &Connection,
    object_storage: &AnalyticsObjectStorageConfig,
) -> AnalyticsEngineResult<()> {
    if object_storage.bucket.is_none() {
        return Ok(());
    }
    let mut options = Vec::new();
    if let Some(region) = object_storage.region.as_deref() {
        options.push(format!("REGION '{}'", escape_sql_string(region)));
    }
    if let Some(endpoint) = object_storage.endpoint_url.as_deref() {
        options.push(format!("ENDPOINT '{}'", escape_sql_string(endpoint)));
        options.push("URL_STYLE 'path'".to_string());
    }
    if let Some(credentials) = object_storage.credentials.as_ref() {
        append_credential_options(&mut options, credentials);
    } else {
        options.push("PROVIDER credential_chain".to_string());
    }
    let sql = format!(
        "CREATE OR REPLACE SECRET aux_analytics_object_store (TYPE S3, {});",
        options.join(", ")
    );
    conn.execute(sql.as_str(), [])?;
    Ok(())
}

fn append_credential_options(options: &mut Vec<String>, credentials: &RemoteCredentialsConfig) {
    if let Some(static_credentials) = credentials.r#static.as_ref() {
        options.push(format!(
            "KEY_ID '{}'",
            escape_sql_string(static_credentials.access_key.as_str())
        ));
        options.push(format!(
            "SECRET '{}'",
            escape_sql_string(static_credentials.secret_key.as_str())
        ));
        if let Some(token) = static_credentials.session_token.as_deref() {
            options.push(format!("SESSION_TOKEN '{}'", escape_sql_string(token)));
        }
    } else if credentials.instance_keys == Some(true) {
        options.push("PROVIDER credential_chain".to_string());
    }
}

pub(crate) fn quote_identifier(identifier: &str) -> String {
    let escaped = identifier.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

pub(crate) fn create_table(table_name: &str, columns: &[AnalyticsColumn]) -> String {
    let mut entries = vec![
        "tenant_id VARCHAR".to_string(),
        "__id VARCHAR".to_string(),
        "table_name VARCHAR".to_string(),
    ];
    let mut sorted_columns = columns.to_vec();
    sorted_columns.sort_by(|left, right| left.column_name.cmp(&right.column_name));
    for column in sorted_columns {
        if matches!(
            column.column_name.as_str(),
            "tenant_id" | "__id" | "table_name"
        ) {
            continue;
        }
        entries.push(format!(
            "{} {}",
            quote_identifier(column.column_name.as_str()),
            column.column_type.duckdb_type()
        ));
    }
    format!(
        "CREATE TABLE IF NOT EXISTS {} ({});",
        quote_identifier(table_name),
        entries.join(", ")
    )
}

pub(crate) fn retention_column_statements(table_name: &str) -> Vec<String> {
    let table = quote_identifier(table_name);
    vec![
        format!(
            "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {} BIGINT;",
            quote_identifier(INTERNAL_INGESTED_AT_COLUMN)
        ),
        format!(
            "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {} BIGINT;",
            quote_identifier(INTERNAL_EXPIRY_COLUMN)
        ),
        format!(
            "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {} BOOLEAN;",
            quote_identifier(INTERNAL_MISSING_RETENTION_COLUMN)
        ),
    ]
}

pub(crate) fn delete_expired_rows(table_name: &str) -> String {
    format!(
        "DELETE FROM {table}
         WHERE rowid IN (
            SELECT rowid FROM {table}
            WHERE {expiry} IS NOT NULL AND {expiry} <= ?
            LIMIT ?
         )",
        table = quote_identifier(table_name),
        expiry = quote_identifier(INTERNAL_EXPIRY_COLUMN)
    )
}

pub(crate) fn missing_retention_count(table_name: &str) -> String {
    format!(
        "SELECT count(*) FROM {} WHERE {} = true",
        quote_identifier(table_name),
        quote_identifier(INTERNAL_MISSING_RETENTION_COLUMN)
    )
}

pub(crate) fn repair_missing_retention(table_name: &str) -> String {
    format!(
        "UPDATE {table}
         SET {expiry} = {ingested_at} + ?, {missing} = false
         WHERE rowid IN (
            SELECT rowid FROM {table}
            WHERE tenant_id = ? AND {missing} = true
            LIMIT ?
         )",
        table = quote_identifier(table_name),
        expiry = quote_identifier(INTERNAL_EXPIRY_COLUMN),
        ingested_at = quote_identifier(INTERNAL_INGESTED_AT_COLUMN),
        missing = quote_identifier(INTERNAL_MISSING_RETENTION_COLUMN)
    )
}

pub(crate) fn alter_table_partitioned_by(
    table_name: &str,
    partition_keys: &[PartitionKey],
) -> Option<String> {
    let columns = partition_keys
        .iter()
        .map(|key| key.column_name.trim())
        .filter(|column| !column.is_empty())
        .map(quote_identifier)
        .collect::<Vec<_>>();
    if columns.is_empty() {
        return None;
    }
    Some(format!(
        "ALTER TABLE {} SET PARTITIONED BY ({});",
        quote_identifier(table_name),
        columns.join(", ")
    ))
}

pub(crate) fn alter_table_sorted_by(
    table_name: &str,
    clustering_keys: &[ClusteringKey],
) -> Option<String> {
    if clustering_keys.is_empty() {
        return None;
    }

    let clause = clustering_keys
        .iter()
        .map(|key| {
            let order = match key.order {
                Some(SortOrder::Asc) | None => "ASC",
                Some(SortOrder::Desc) => "DESC",
            };
            format!("{} {order}", quote_identifier(key.column_name.as_str()))
        })
        .collect::<Vec<_>>()
        .join(", ");

    Some(format!(
        "ALTER TABLE {} SET SORTED BY ({clause});",
        quote_identifier(table_name)
    ))
}

pub(crate) fn attach_ducklake(catalog: CatalogType, catalog_path: &str, data_path: &str) -> String {
    let prefix = match catalog {
        CatalogType::Sqlite => "sqlite",
        CatalogType::Postgres => "postgres",
    };
    format!(
        "ATTACH 'ducklake:{prefix}:{}' AS dlake (DATA_PATH '{}');",
        escape_sql_string(catalog_path),
        escape_sql_string(data_path)
    )
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}
