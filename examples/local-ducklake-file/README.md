# Local DuckLake File Catalog

This example shows a DuckLake deployment that writes data files to the local
filesystem rather than S3, R2, or another S3-compatible endpoint.

Initialize the catalog and tables:

```sh
cargo run -p analytics-cli --bin aux-analytics -- init \
  --config examples/local-ducklake-file/analytics-config.json
```

Serve the API:

```sh
cargo run -p analytics-api --bin aux-analytics-api -- \
  --config examples/local-ducklake-file/analytics-config.json
```

The source stream in this example is configured as a standard stream. In a real
deployment, set `analytics.source.region`, credentials, and
`tables[].stream_identifier` to the live source stream ARN.
