use std::{borrow::Cow, env};

use analytics_contract::{
    AnalyticsColumn, ClusteringKey, INTERNAL_EXPIRY_COLUMN, INTERNAL_INGESTED_AT_COLUMN,
    INTERNAL_MISSING_RETENTION_COLUMN, PartitionKey, SortOrder,
};
use aws_credentials::AwsStaticCredentials;
use config::{
    AnalyticsObjectStorageConfig, CatalogType, RemoteStaticCredentialsConfig, StorageBackend,
};
use duckdb::Connection;

use crate::{
    AnalyticsEngineResult,
    constants::{QUOTED_SOURCE_POSITION_COLUMN, SOURCE_POSITION_COLUMN},
};

pub(crate) fn configure_connection(
    conn: &Connection,
    backend: &StorageBackend,
    resolved_credentials: Option<&AwsStaticCredentials>,
) -> AnalyticsEngineResult<()> {
    disable_automatic_extensions(conn)?;
    configure_extension_directory(conn)?;
    if let StorageBackend::DuckLake {
        catalog,
        catalog_path,
        data_path,
        object_storage,
        ..
    } = backend
    {
        load_extension(conn, "httpfs")?;
        load_extension(conn, "ducklake")?;
        match catalog {
            CatalogType::Sqlite => load_extension(conn, "sqlite_scanner")?,
            CatalogType::AuxCatalog => {}
            CatalogType::MotherDuck => load_extension(conn, "motherduck")?,
        };
        if let Some(object_storage) = object_storage
            && let Some(secret) = ObjectStorageSecret::from_config(object_storage)
        {
            secret.create_or_replace_secret(conn, resolved_credentials)?;
        }
        attach_ducklake_catalog(conn, *catalog, catalog_path, data_path)?;
        conn.execute("USE dlake;", [])?;
    }
    Ok(())
}

fn disable_automatic_extensions(conn: &Connection) -> AnalyticsEngineResult<()> {
    conn.execute_batch(
        "SET autoinstall_known_extensions=false; SET autoload_known_extensions=false;",
    )?;
    Ok(())
}

fn attach_ducklake_catalog(
    conn: &Connection,
    catalog: CatalogType,
    catalog_path: &str,
    data_path: &str,
) -> AnalyticsEngineResult<()> {
    conn.execute(
        attach_ducklake(catalog, catalog_path, data_path).as_str(),
        [],
    )?;
    Ok(())
}

fn configure_extension_directory(conn: &Connection) -> AnalyticsEngineResult<()> {
    let Some(extension_directory) = env::var_os("DUCKDB_EXTENSION_DIRECTORY") else {
        return Ok(());
    };
    let extension_directory = extension_directory.to_string_lossy();
    conn.execute(
        format!(
            "SET extension_directory = '{}';",
            escape_sql_string(extension_directory.as_ref())
        )
        .as_str(),
        [],
    )?;
    Ok(())
}

fn load_extension(conn: &Connection, extension: &str) -> AnalyticsEngineResult<()> {
    if let Some(extension_path) = env::var_os("DUCKDB_EXTENSION_PATH") {
        let extension_path = extension_path.to_string_lossy();
        let path = format!(
            "{}/{}.duckdb_extension",
            extension_path.trim_end_matches('/'),
            extension
        );
        conn.execute(
            format!("LOAD '{}';", escape_sql_string(path.as_str())).as_str(),
            [],
        )?;
        return Ok(());
    }
    conn.execute(format!("LOAD {extension};").as_str(), [])?;
    Ok(())
}

#[cfg(test)]
pub(crate) fn load_extension_for_test(
    conn: &Connection,
    extension: &str,
) -> AnalyticsEngineResult<()> {
    disable_automatic_extensions(conn)?;
    load_extension(conn, extension)
}

/// Secret settings borrow during setup and own only native secret inputs for
/// refresh.
#[derive(Debug)]
pub(crate) struct ObjectStorageSecret<'a> {
    region: Option<Cow<'a, str>>,
    endpoint_url: Option<Cow<'a, str>>,
    credentials: Option<Cow<'a, RemoteStaticCredentialsConfig>>,
}

impl<'a> ObjectStorageSecret<'a> {
    pub(crate) fn from_config(config: &'a AnalyticsObjectStorageConfig) -> Option<Self> {
        config.bucket.as_ref()?;
        Some(Self {
            region: config.region.as_deref().map(Cow::Borrowed),
            endpoint_url: config.endpoint_url.as_deref().map(Cow::Borrowed),
            credentials: config
                .credentials
                .as_ref()
                .and_then(|credentials| credentials.r#static.as_ref())
                .map(Cow::Borrowed),
        })
    }

    pub(crate) fn into_owned(self) -> ObjectStorageSecret<'static> {
        ObjectStorageSecret {
            region: self.region.map(|value| Cow::Owned(value.into_owned())),
            endpoint_url: self
                .endpoint_url
                .map(|value| Cow::Owned(value.into_owned())),
            credentials: self.credentials.map(|value| Cow::Owned(value.into_owned())),
        }
    }

    pub(crate) fn create_or_replace_secret(
        &self,
        conn: &Connection,
        resolved_credentials: Option<&AwsStaticCredentials>,
    ) -> AnalyticsEngineResult<()> {
        let Some(sql) = self.build_statement(resolved_credentials) else {
            return Ok(());
        };
        conn.execute(sql.as_str(), [])?;
        Ok(())
    }

    pub(crate) fn build_statement(
        &self,
        resolved_credentials: Option<&AwsStaticCredentials>,
    ) -> Option<String> {
        let mut options = Vec::new();
        if let Some(region) = self.region.as_deref() {
            options.push(format!("REGION '{}'", escape_sql_string(region)));
        }
        if let Some(endpoint) = self.endpoint_url.as_deref() {
            append_endpoint_options(&mut options, endpoint);
            options.push("URL_STYLE 'path'".to_string());
        }
        if let Some(credentials) = resolved_credentials {
            append_static_credential_options(
                &mut options,
                &credentials.access_key_id,
                &credentials.secret_access_key,
                credentials.session_token.as_deref(),
            );
        } else if let Some(credentials) = self.credentials.as_deref() {
            append_static_credential_options(
                &mut options,
                &credentials.access_key,
                &credentials.secret_key,
                credentials.session_token.as_deref(),
            );
        }
        if options.is_empty() {
            return None;
        }
        Some(format!(
            "CREATE OR REPLACE SECRET aux_analytics_object_store (TYPE S3, {});",
            options.join(", ")
        ))
    }
}

fn append_endpoint_options(options: &mut Vec<String>, endpoint: &str) {
    let (endpoint, use_ssl) = endpoint
        .strip_prefix("http://")
        .map(|endpoint| (endpoint, Some(false)))
        .or_else(|| {
            endpoint
                .strip_prefix("https://")
                .map(|endpoint| (endpoint, Some(true)))
        })
        .unwrap_or((endpoint, None));
    options.push(format!("ENDPOINT '{}'", escape_sql_string(endpoint)));
    if let Some(use_ssl) = use_ssl {
        options.push(format!("USE_SSL {}", use_ssl));
    }
}

fn append_static_credential_options(
    options: &mut Vec<String>,
    access_key: &str,
    secret_key: &str,
    session_token: Option<&str>,
) {
    options.push(format!("KEY_ID '{}'", escape_sql_string(access_key)));
    options.push(format!("SECRET '{}'", escape_sql_string(secret_key)));
    if let Some(token) = session_token {
        options.push(format!("SESSION_TOKEN '{}'", escape_sql_string(token)));
    }
}

pub(crate) fn quote_identifier(identifier: &str) -> String {
    let mut quoted = String::with_capacity(identifier.len() + 2);
    quoted.push('"');
    for character in identifier.chars() {
        if character == '"' {
            quoted.push('"');
        }
        quoted.push(character);
    }
    quoted.push('"');
    quoted
}

pub(crate) fn create_table(table_name: &str, columns: &[AnalyticsColumn]) -> String {
    let mut entries = vec![
        "tenant_id VARCHAR".to_string(),
        "__id VARCHAR".to_string(),
        "table_name VARCHAR".to_string(),
        format!("{} JSON", quote_identifier(SOURCE_POSITION_COLUMN)),
    ];
    let mut sorted_columns = columns.iter().collect::<Vec<_>>();
    sorted_columns.sort_by(|left, right| left.column_name.cmp(&right.column_name));
    for column in sorted_columns {
        if matches!(
            column.column_name.as_str(),
            "tenant_id" | "__id" | "table_name" | SOURCE_POSITION_COLUMN
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

pub(crate) fn manifest_column_statements(
    table_name: &str,
    columns: &[AnalyticsColumn],
) -> Vec<String> {
    let table = quote_identifier(table_name);
    let mut sorted_columns = columns.iter().collect::<Vec<_>>();
    sorted_columns.sort_by(|left, right| left.column_name.cmp(&right.column_name));
    let mut statements = Vec::with_capacity(sorted_columns.len());
    statements.extend(
        sorted_columns
            .into_iter()
            .filter(|column| {
                !matches!(
                    column.column_name.as_str(),
                    "tenant_id" | "__id" | "table_name" | SOURCE_POSITION_COLUMN
                )
            })
            .map(|column| {
                format!(
                    "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {} {};",
                    quote_identifier(column.column_name.as_str()),
                    column.column_type.duckdb_type()
                )
            }),
    );
    statements
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

pub(crate) fn source_position_column_statement(table_name: &str) -> String {
    format!(
        "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} JSON;",
        quote_identifier(table_name),
        QUOTED_SOURCE_POSITION_COLUMN
    )
}

pub(crate) fn expired_retention_count(table_name: &str) -> String {
    format!(
        "SELECT count(*) FROM {table}
         WHERE {expiry} IS NOT NULL
           AND {expiry} <= ?
           AND {missing} = false",
        table = quote_identifier(table_name),
        expiry = quote_identifier(INTERNAL_EXPIRY_COLUMN),
        missing = quote_identifier(INTERNAL_MISSING_RETENTION_COLUMN)
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
    let catalog_reference = ducklake_catalog_reference(catalog, catalog_path);
    let metadata_options = ducklake_metadata_options(catalog);
    let data_inlining_row_limit = ducklake_data_inlining_row_limit(catalog);
    format!(
        "ATTACH 'ducklake:{}' AS dlake (DATA_PATH '{}'{metadata_options}, ENCRYPTED, \
         DATA_INLINING_ROW_LIMIT {data_inlining_row_limit}, AUTOMATIC_MIGRATION true);",
        escape_sql_string(catalog_reference.as_str()),
        escape_sql_string(data_path),
    )
}

const fn ducklake_data_inlining_row_limit(catalog: CatalogType) -> usize {
    match catalog {
        CatalogType::AuxCatalog | CatalogType::Sqlite => 100,
        CatalogType::MotherDuck => 0,
    }
}

fn ducklake_catalog_reference(catalog: CatalogType, catalog_path: &str) -> String {
    match catalog {
        CatalogType::Sqlite => format!("sqlite:{catalog_path}"),
        CatalogType::AuxCatalog => catalog_path.to_string(),
        CatalogType::MotherDuck => format!(
            "md:{}",
            catalog_path.strip_prefix("md:").unwrap_or(catalog_path),
        ),
    }
}

fn ducklake_metadata_options(catalog: CatalogType) -> &'static str {
    match catalog {
        CatalogType::AuxCatalog => ", META_TYPE 'aux_catalog'",
        // Metadata reads must not block schema/ingestion commits in another
        // session.
        CatalogType::Sqlite => ", META_JOURNAL_MODE 'WAL'",
        CatalogType::MotherDuck => "",
    }
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}
