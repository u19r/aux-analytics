# Aux Analytics

Aux Analytics is a standalone analytics pipeline and query workspace. It accepts a JSON table
manifest, creates DuckDB or DuckLake tables, ingests storage stream records, and exposes a small
library and CLI for OLAP queries.

The repository is intentionally domain-agnostic. Product-specific entity registration should stay
with the product repo and be exported as an `analytics-contract` manifest.

## Crates

- `analytics-contract`: serializable manifest and JSON schema types.
- `analytics-api`: Axum HTTP API, middleware, and standalone server binary.
- `analytics-engine`: DuckDB/DuckLake connection, table DDL, stream-record ingestion, and query API.
- `analytics-storage`: optional adapter from the storage facade into the analytics contract types.
- `analytics-cli`: maintenance and operations binary for schema export, table initialization, and
  explicit tenant-scoped or unscoped queries.
- `analytics-lambda`: AWS Lambda adapter for direct ETL and query invocation.

## CLI

```sh
cargo run -p analytics-cli -- schema
cargo run -p analytics-cli -- config-schema
cargo run -p analytics-cli -- init --manifest manifest.json --duckdb analytics.duckdb
cargo run -p analytics-api --bin aux-analytics-api -- --manifest manifest.json --duckdb analytics.duckdb --config config.json
```

Query commands use explicit names for tenant-scoped and unscoped access:

```sh
cargo run -p analytics-cli -- tenant-query \
  --manifest manifest.json \
  --duckdb analytics.duckdb \
  --target-tenant-id tenant_01 \
  --query query.json

cargo run -p analytics-cli -- unscoped-structured-query \
  --manifest manifest.json \
  --duckdb analytics.duckdb \
  --query query.json

cargo run -p analytics-cli -- unscoped-sql-query \
  --duckdb analytics.duckdb \
  --sql "select count(*) as rows from users"
```

Use `tenant-query` for caller-facing reads that must be isolated to a single tenant. The
`unscoped-structured-query` and `unscoped-sql-query` commands are for explicitly unscoped
maintenance or administrative reads.

The HTTP server uses the standard config crate for HTTP bind address, CORS, tracing, and metrics.
The analytics manifest, source table list, stream source type, catalog connection string, and
object storage location can also be supplied through the standard config file so the product repo
can own domain table registration while this repo stays agnostic.

```json
{
  "analytics": {
    "manifest_path": "analytics-manifest.json",
    "http": {
      "ingest_endpoint_enabled": false
    },
    "source": {
      "stream_type": "storage_stream",
      "region": "us-east-1",
      "credentials": {
        "instance_keys": true
      },
      "poll_interval_ms": 100,
      "poll_max_shards": 16,
      "poll_max_responses_per_interval": 160,
      "tables": [
        {
          "table_name": "tenant_entities",
          "stream_identifier": "arn:aws:dynamodb:us-east-1:123456789012:table/tenant_entities/stream/2026-05-13T00:00:00.000"
        },
        {
          "table_name": "audit_events",
          "stream_type": "aux_storage",
          "stream_identifier": "audit_events"
        }
      ]
    },
    "catalog": {
      "backend": "ducklake_postgres",
      "connection_string": "dbname=ducklake_catalog host=localhost"
    },
    "object_storage": {
      "provider": "r2",
      "scheme": "s3",
      "bucket": "analytics-lake",
      "path": "customers/prod",
      "endpoint_url": "https://account-id.r2.cloudflarestorage.com",
      "region": "auto",
      "credentials": {
        "instance_keys": true
      }
    }
  }
}
```

CLI backend flags override the configured catalog for local operation. For DuckLake backends,
`object_storage.scheme`, `bucket`, and `path` are resolved to `scheme://bucket/path`; omitting the
bucket leaves `path` as a local or fully qualified data path. The object store can be AWS S3,
Cloudflare R2, or a generic S3-compatible endpoint.

Credential resolution follows the same configuration shape as aux-storage remote services:

- omit `analytics.source.credentials` to let the AWS SDK use its default credential chain for
  standard stream polling;
- set `analytics.source.credentials.instance_keys: true` to use instance/task/workload identity;
- set `analytics.source.credentials.static.*` for explicit source stream credentials;
- omit `analytics.object_storage.credentials` to let DuckDB/DuckLake use its ambient credential
  chain where available;
- set `credentials.instance_keys: true` to request instance/workload identity credential lookup;
- set `credentials.static.access_key`, `credentials.static.secret_key`, and optional
  `credentials.static.session_token` for explicit static credentials.

When the binary runs continuously, `analytics-storage` starts the configured source pollers and
feeds contract records into the engine. Lambda deployments do not start this poller; AWS event
source mappings handle stream polling and invoke `analytics-lambda` with batches.

## Retention

Tables can opt into analytics-owned row expiry. Static retention belongs in the table manifest:

```json
{
  "source_table_name": "audit_stream",
  "analytics_table_name": "audit_events",
  "tenant_selector": { "kind": "attribute", "attribute_name": "tenant_id" },
  "retention": {
    "period_ms": 7776000000,
    "timestamp": { "kind": "attribute", "attribute_path": "created_at_ms" }
  }
}
```

Use `{ "kind": "ingested_at" }` for the timestamp when expiry should be based on when
aux-analytics ingested the row. Retention-enabled tables get internal columns `__ingested_at`,
`__expiry`, and `__missing_retention`; these names are reserved.

Dynamic tenant-specific retention belongs in runtime config under `analytics.retention`. Tenant
policy can be loaded over aux-storage HTTP or DynamoDB `GetItem`/`Query`. DynamoDB access uses the
AWS SDK, and `query_table.limit` must be `1`. Aux-storage lookups retry 3 times with exponential
backoff. Failed strict lookups retain the row with `__missing_retention = true` until repaired with
`aux-analytics repair-retention`.

Standalone polling checkpoints are stored durably in the analytics database in
`__analytics_source_checkpoints`. Aux-storage polling stores the last stream page position per
source table. Standard stream polling stores the last successfully ingested sequence number per
stream shard and resumes with `AFTER_SEQUENCE_NUMBER` on restart. Aux-fn does not own or schedule
these checkpoints; it only supplies manifests and deployment config for the standalone
aux-analytics process.

## Health And Metrics

The HTTP API exposes `/up`, `/ready`, `/health`, `/diagnostics`, and `/metrics`.
`/health` includes source poller status and current checkpoint health. `/diagnostics`
returns the same source detail with counters for poll errors, ingest errors, and
checkpoint saves. `/metrics` exports Prometheus metrics when `features.metrics.enabled`
is true.

Important polling metrics:

- `analytics.source.polls_total`
- `analytics.source.poll_errors_total`
- `analytics.source.records_per_poll`
- `analytics.source.records_ingested_total`
- `analytics.source.ingest_errors_total`
- `analytics.source.checkpoints_saved_total`
- `analytics.source.checkpoint_errors_total`
- `analytics.source.checkpoints`
- `analytics.retention.lookups_total`
- `analytics.retention.lookup_failures_total`
- `analytics.retention.sweeps_total`
- `analytics.retention.sweep_duration_ms`
- `analytics.retention.rows_deleted_total`
- `analytics.query.requests_total`
- `analytics.query.latency_ms`
- `analytics.http.ingest.requests_total`
- `analytics.http.ingest.latency_ms`

Prometheus renders dots as underscores, for example
`analytics.source.polls_total` is exposed as
`analytics_source_polls_total`.

See `configuration.md` for the full option table and schema commands.
See `observability.md` for the metrics endpoint and stdout/stderr logging behavior.
See `examples/` for local DuckDB and DuckLake configurations.

## Boundaries

`analytics-contract` is the integration boundary. Aux-fn should export manifests and, when pushing
records over HTTP, use the contract JSON shape. Static integrations that already link the storage
facade can use `analytics-storage` to convert storage records into contract records, but
`analytics-engine` and `analytics-api` do not depend on the storage facade or aux-fn crates.

DuckLake connections use a catalog and data path:

```sh
cargo run -p analytics-cli -- init \
  --manifest manifest.json \
  --ducklake-sqlite-catalog metadata.ducklake \
  --ducklake-data-path data_files
```

## AWS Lambda

`analytics-lambda` builds the `aux-analytics-lambda` binary. It loads the same manifest and catalog
configuration as the CLI, then handles direct JSON invocation events for ETL and queries.

Environment variables:

- `AUX_ANALYTICS_CONFIG`: optional standard config file path.
- `AUX_ANALYTICS_MANIFEST`: optional manifest path override.
- `AUX_ANALYTICS_DUCKDB`: optional DuckDB path override.
- `AUX_ANALYTICS_DUCKLAKE_SQLITE_CATALOG`: optional DuckLake SQLite catalog override.
- `AUX_ANALYTICS_DUCKLAKE_POSTGRES_CATALOG`: optional DuckLake Postgres catalog override.
- `AUX_ANALYTICS_DUCKLAKE_DATA_PATH`: required when a DuckLake env override is used.

Example unscoped SQL query event:

```json
{
  "operation": "unscoped_sql_query",
  "sql": "select count(*) as rows from users"
}
```

Example ETL event:

```json
{
  "operation": "ingest",
  "analytics_table_name": "users",
  "record_key": "user-1",
  "record": {
    "Keys": {},
    "SequenceNumber": "1",
    "NewImage": {
      "profile": {
        "M": {
          "email": {
            "S": "a@example.com"
          }
        }
      }
    }
  }
}
```
