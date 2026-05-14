# Aux Analytics Configuration

Aux Analytics reads the standard JSON config used by the platform storage stack.
Every command accepts a config file with `--config config.json`, and every config
value can be overridden with `--overrides path.to.field=value`.

Generate schemas from the binary:

```sh
cargo run -p analytics-cli -- config-schema > config.schema.json
cargo run -p analytics-cli -- schema > manifest.schema.json
cargo run -p analytics-cli -- openapi > openapi.json
```

## Precedence

| Source | Example | Precedence |
| --- | --- | --- |
| Command flag | `--duckdb local.duckdb` | Highest for backend selection |
| Override argument | `--overrides analytics.source.poll_interval_ms=50` | Overrides config file |
| Config file | `--config config/analytics.json` | Main deployment config |
| Defaults | omitted fields | Lowest |

## Analytics Config Fields

| Field | Default | Description |
| --- | --- | --- |
| `analytics.manifest_path` | none | Path to the table manifest JSON. Can be overridden by `--manifest` or `AUX_ANALYTICS_MANIFEST`. |
| `analytics.http.ingest_endpoint_enabled` | `true` | Enables the direct HTTP `/ingest/{analytics_table_name}` endpoint. Set to `false` for standalone polling deployments that should only ingest from configured source streams. |
| `analytics.source.stream_type` | none | Global stream format for source tables. Values: `storage_stream`, `aux_storage`. |
| `analytics.source.endpoint_url` | none | Source endpoint override. Required for `aux_storage` polling, optional for AWS-compatible standard stream endpoints. |
| `analytics.source.region` | AWS SDK default | Region for standard stream polling. |
| `analytics.source.credentials` | AWS SDK default | Credential chain for source polling. Uses the same shape as aux-storage remote credentials. |
| `analytics.source.poll_interval_ms` | `100` | Poll loop interval. The poller should keep issuing background requests inside the interval budget. |
| `analytics.source.poll_max_shards` | `16` | Maximum shards or stream partitions polled concurrently. |
| `analytics.source.poll_max_responses_per_interval` | `160` | Maximum 1 MB stream responses to fetch per interval across all shards. At the default 100 ms interval this budgets up to 1600 responses/s, enough headroom for 100 MB/s when responses are below the 1 MB service maximum. |
| `analytics.source.tables[].table_name` | required | Source storage table name. |
| `analytics.source.tables[].stream_type` | inherits global | Per-table stream format override. |
| `analytics.source.tables[].stream_identifier` | none | Standard stream ARN for `storage_stream`. For `aux_storage`, table polling uses `table_name`; this field is informational/reserved. |
| `analytics.retention.enabled` | `false` | Enables the periodic retention sweeper for dynamic retention tables. Static manifest retention can compute expiry without dynamic lookup, but the sweeper must be enabled to delete expired rows continuously. |
| `analytics.retention.sweep_interval_ms` | `60000` | Interval between retention sweeps. |
| `analytics.retention.delete_batch_size` | `500` | Maximum expired rows deleted per batch. |
| `analytics.retention.delete_batch_pause_ms` | `250` | Pause between delete batches to avoid overwhelming the DuckLake catalog backend. |
| `analytics.retention.tables[].analytics_table_name` | required | Analytics table with dynamic tenant retention. |
| `analytics.retention.tables[].default_period_ms` | required | Fallback retention duration in milliseconds. |
| `analytics.retention.tables[].strict` | `false` | When true, failed tenant lookup stores `__missing_retention = true` and leaves `__expiry` null instead of using fallback. |
| `analytics.retention.tables[].timestamp` | required | Expiry basis: `{ "kind": "ingested_at" }` or `{ "kind": "attribute", "attribute_path": "created_at_ms" }`. |
| `analytics.retention.tables[].tenant_policy.source` | required | `aux_storage` or `dynamodb`. |
| `analytics.retention.tables[].tenant_policy.request.get_item` | optional | DynamoDB-compatible get item request with `table_name` and `key`. |
| `analytics.retention.tables[].tenant_policy.request.query_table` | optional | DynamoDB-compatible query request. `limit` is required and must be `1`. |
| `analytics.retention.tables[].tenant_policy.duration_selector.attribute_path` | required | Dot path to a numeric duration value in milliseconds in the returned item. |
| `analytics.retention.tables[].tenant_policy.cache_ttl_ms` | `300000` | Tenant retention policy cache TTL. |
| `analytics.catalog.backend` | none | `duckdb`, `ducklake_sqlite`, or `ducklake_postgres`. |
| `analytics.catalog.connection_string` | none | DuckDB path or DuckLake catalog connection string. |
| `analytics.object_storage.provider` | `s3` | Object store provider: `s3`, `r2`, or `generic`. |
| `analytics.object_storage.scheme` | `s3` | URI scheme used in DuckLake `DATA_PATH`, for example `s3`. |
| `analytics.object_storage.bucket` | none | Bucket/container name. If omitted, `path` is used directly. |
| `analytics.object_storage.path` | none | Object prefix or local path. Required for DuckLake backends. |
| `analytics.object_storage.endpoint_url` | none | S3-compatible endpoint URL for R2, MinIO, or another generic bucket service. |
| `analytics.object_storage.region` | none | Object store region. Use `auto` for Cloudflare R2. |
| `analytics.object_storage.credentials` | none | Credential chain configuration. See below. |

## Credential Chain

`analytics.source.credentials` and `analytics.object_storage.credentials` mirror
aux-storage remote credential configuration:

```json
{
  "analytics": {
    "object_storage": {
      "credentials": {
        "instance_keys": true
      }
    }
  }
}
```

Use one of these modes:

| Mode | Config | Behavior |
| --- | --- | --- |
| Ambient/default | omit `credentials` | Let the AWS SDK or DuckDB/DuckLake resolve credentials from the environment. |
| Instance/workload identity | `{ "instance_keys": true }` | Use the runtime credential chain for instance, task, or workload identity. |
| Static | `{ "static": { "access_key": "...", "secret_key": "...", "session_token": "..." } }` | Use explicit credentials. `session_token` is optional. |

Do not configure static credentials and `instance_keys: true` together.

## Polling Behavior

Polling belongs in `analytics-storage` or another adapter crate. The
`analytics-engine` crate only accepts contract stream records. The standalone
`aux-analytics-api` binary starts polling when `analytics.source.tables` is
non-empty. Lambda deployments do not use this runtime because AWS event source
mappings manage stream polling and invoke the Lambda handler with batches.

The resident poller:

- poll up to `poll_max_shards` shards concurrently;
- keep fetching additional pages while work remains, rather than sleeping after
  one response;
- respect `poll_max_responses_per_interval` as a per-interval fairness budget;
- treat each stream response as at most 1 MB and often smaller;
- convert both aux-storage custom stream records and standard stream records
  into `analytics-contract` records before ingestion;
- save durable checkpoints only after the batch has been ingested.

Checkpoints are internal aux-analytics state, stored in the analytics database
table `__analytics_source_checkpoints`. For `aux_storage` sources, the
checkpoint is the last stream page position for the table. For `storage_stream`
sources, the checkpoint is the last successfully ingested sequence number per
stream shard. On restart the poller resumes standard streams with
`AFTER_SEQUENCE_NUMBER`.

Aux-fn does not run analytics polling, checkpointing, or analytics background
jobs when aux-analytics is deployed as the standalone product. Aux-fn only
exports the manifest and passes config/deployment values through to the
aux-analytics process.

Checkpoint health is visible in `/health` and `/diagnostics`. Prometheus
metrics are available at `/metrics` when `features.metrics.enabled` is true,
including poll counts, poll errors, records per poll, ingested records, ingest
errors, checkpoint saves, checkpoint errors, current checkpoint count, and query
request counts.

Metric names use the same dotted subsystem style as aux-fn instrumentation, and
Prometheus renders them with underscores:

| Metric | Type | Labels |
| --- | --- | --- |
| `analytics.source.polls_total` | counter | none |
| `analytics.source.poll_errors_total` | counter | none |
| `analytics.source.records_per_poll` | histogram | none |
| `analytics.source.records_ingested_total` | counter | none |
| `analytics.source.ingest_errors_total` | counter | none |
| `analytics.source.checkpoints_saved_total` | counter | none |
| `analytics.source.checkpoint_errors_total` | counter | none |
| `analytics.source.checkpoints` | gauge | none |
| `analytics.query.requests_total` | counter | `type`, `outcome` |
| `analytics.query.latency_ms` | histogram | `type`, `outcome` |
| `analytics.http.ingest.requests_total` | counter | `outcome` |
| `analytics.http.ingest.latency_ms` | histogram | `outcome` |
| `analytics.retention.lookups_total` | counter | `table`, `source`, `outcome` |
| `analytics.retention.lookup_latency_ms` | histogram | `table`, `source`, `outcome` |
| `analytics.retention.lookup_failures_total` | counter | `table`, `source`, `tenant_id` |
| `analytics.retention.missing_rows` | gauge | `table` |
| `analytics.retention.sweeps_total` | counter | `table`, `outcome` |
| `analytics.retention.sweep_duration_ms` | histogram | `table`, `outcome` |
| `analytics.retention.delete_batches_total` | counter | `table`, `outcome` |
| `analytics.retention.delete_batch_duration_ms` | histogram | `table`, `outcome` |
| `analytics.retention.rows_deleted_total` | counter | `table` |

## Retention Behavior

Static retention is declared in the manifest. Dynamic tenant-specific retention is declared in
runtime config and is resolved over HTTP from aux-storage or DynamoDB. Dynamic request values may use
`${tenant_id}` templates. Returned policy items must contain a numeric DynamoDB `N` duration in
milliseconds at `duration_selector.attribute_path`.

For `query_table`, `limit` is required and must be exactly `1`. DynamoDB lookups use the AWS SDK for
SigV4 signing. Aux-storage lookups use DynamoDB-compatible JSON requests and retry transport or 5xx
failures up to 3 times with exponential backoff.

Rows with failed strict lookup are retained with `__missing_retention = true` and `__expiry = null`.
Repair them with:

```sh
cargo run -p analytics-cli -- repair-retention \
  --manifest manifest.json \
  --config config.json \
  --duckdb analytics.duckdb \
  --table audit_events \
  --tenant-id tenant-a
```

For a 100 MB/s target with a 100 ms interval, the system needs capacity for at
least 10 MB per interval. Because each stream response is capped at 1 MB, the
poller must be able to process at least 10 responses per interval. The defaults
budget 160 responses per interval across 16 shards to leave room for small
responses and uneven shard load.

## Examples

Runnable examples live under `examples/`:

| Example | Purpose |
| --- | --- |
| `examples/local-aux-storage-duckdb` | Local aux-storage polling into DuckDB with `/ingest` disabled. |
| `examples/local-ducklake-file` | DuckLake catalog with local filesystem data files. |

The scripted local demo runs the same no-`/ingest` polling path:

```sh
bash scripts/demo-local-polling.sh
```

Local DuckDB:

```json
{
  "analytics": {
    "manifest_path": "manifest.json",
    "catalog": {
      "backend": "duckdb",
      "connection_string": "analytics.duckdb"
    }
  }
}
```

DuckLake on R2:

```json
{
  "analytics": {
    "manifest_path": "manifest.json",
    "source": {
      "stream_type": "storage_stream",
      "region": "us-east-1",
      "poll_interval_ms": 100,
      "poll_max_shards": 32,
      "poll_max_responses_per_interval": 320,
      "tables": [
        {
          "table_name": "tenant_entities",
          "stream_identifier": "arn:aws:dynamodb:us-east-1:123456789012:table/tenant_entities/stream/2026-05-13T00:00:00.000"
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
      "path": "prod",
      "endpoint_url": "https://account-id.r2.cloudflarestorage.com",
      "region": "auto",
      "credentials": {
        "instance_keys": true
      }
    }
  }
}
```

Aux-storage source polling:

```json
{
  "analytics": {
    "source": {
      "stream_type": "aux_storage",
      "endpoint_url": "http://127.0.0.1:39124/storage",
      "poll_interval_ms": 100,
      "poll_max_shards": 16,
      "poll_max_responses_per_interval": 160,
      "tables": [
        { "table_name": "tenant_entities" },
        { "table_name": "audit_events" }
      ]
    }
  }
}
```
