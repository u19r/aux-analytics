# Observability

Aux Analytics exposes runtime observability through the HTTP metrics endpoint and process output.
The standalone `aux-analytics-api` process is the primary source for both signals.

## Metrics Endpoint

The HTTP API mounts `GET /metrics` when `features.metrics.enabled` is `true`. Metrics are enabled
by default. If `features.metrics.prometheus.bearer_token` is configured, clients must send
`Authorization: Bearer <token>`; missing tokens return `401` and invalid tokens return `403`.

The endpoint returns Prometheus text format with content type `text/plain; version=0.0.4`.
Metric names are recorded in dotted form in Rust and rendered with underscores by the Prometheus
exporter. For example, `analytics.source.polls_total` appears as
`analytics_source_polls_total`.

Histograms are exported with the standard Prometheus `_bucket`, `_sum`, and `_count` series.

| Metric | Prometheus name | Type | Labels | Description |
| --- | --- | --- | --- | --- |
| `analytics.source.polls_total` | `analytics_source_polls_total` | counter | none | Source polling intervals started by the standalone poller. |
| `analytics.source.poll_errors_total` | `analytics_source_poll_errors_total` | counter | none | Poller calls that failed before returning a batch. |
| `analytics.source.records_per_poll` | `analytics_source_records_per_poll` | histogram | none | Number of source records returned in each successful poll batch. |
| `analytics.source.records_ingested_total` | `analytics_source_records_ingested_total` | counter | none | Polled source records successfully ingested into analytics tables. |
| `analytics.source.ingest_errors_total` | `analytics_source_ingest_errors_total` | counter | none | Polled source records that failed ingestion. |
| `analytics.source.checkpoints_saved_total` | `analytics_source_checkpoints_saved_total` | counter | none | Source checkpoints durably saved after successful ingestion. |
| `analytics.source.checkpoint_errors_total` | `analytics_source_checkpoint_errors_total` | counter | none | Failed checkpoint save attempts. |
| `analytics.source.checkpoints` | `analytics_source_checkpoints` | gauge | none | Current number of tracked source checkpoints in poller health state. |
| `analytics.query.requests_total` | `analytics_query_requests_total` | counter | `type`, `outcome` | Query API requests handled by `/query` and `/structured-query`. |
| `analytics.query.latency_ms` | `analytics_query_latency_ms` | histogram | `type`, `outcome` | Query API request latency in milliseconds. |
| `analytics.http.ingest.requests_total` | `analytics_http_ingest_requests_total` | counter | `outcome` | HTTP ingest requests handled by `/ingest/{analytics_table_name}`. |
| `analytics.http.ingest.latency_ms` | `analytics_http_ingest_latency_ms` | histogram | `outcome` | HTTP ingest request latency in milliseconds. |

Query metrics use these label values:

- `type`: `raw` for `/query`, `structured` for `/structured-query`.
- `outcome`: `success`, `error`, or `validation_error`.

HTTP ingest metrics use these label values:

- `outcome`: `success`, `error`, or `validation_error`.

Source polling metrics are emitted only by the standalone API server when source polling is
configured. Lambda deployments rely on AWS event source mappings and do not run the standalone
poller.

## Stdout Logs

The standalone API process writes application tracing events to stdout as newline-delimited JSON.
The tracing filter comes from `tracing.log_level` in config and defaults to `warn`.

Example config:

```json
{
  "tracing": {
    "log_level": "analytics_api=info,analytics_storage=info,warn"
  }
}
```

Important stdout events include:

- source polling disabled because no source tables are configured;
- source polling disabled because no configured source table is pollable;
- source configuration loaded, including the configured source table count;
- polled record ingestion failures, with `analytics_table_name` and error context;
- checkpoint save failures;
- source polling failures;
- shutdown signal receipt and shutdown handler warnings.

The process also writes two human-readable startup lines to stdout:

- `Using log filter (source: <config|default>): <filter>`
- `Listening on <bind_addr>`

Collectors should therefore tolerate both JSON tracing events and these startup text lines on
stdout.

## Stderr Logs

The standalone API process does not use stderr for normal application logs. Stderr is used before
tracing initialization when `tracing.log_level` is present but invalid:

```text
Invalid log level '<value>' in config (<error>), falling back to default filter
```

After that fallback, tracing initializes with the default `warn` filter and writes application
events to stdout.

Runtime failures outside the tracing pipeline, such as process panics or container supervisor
messages, may still appear on stderr, but they are not part of the Aux Analytics application log
contract.
