# Local Aux-Storage To DuckDB

This example runs aux-analytics as a standalone poller against a local
aux-storage HTTP service. The direct `/ingest` endpoint is disabled so data can
only arrive through configured source polling.

Start aux-storage from the sibling repository:

```sh
cd ../aux-storage
cargo run -p storage-api --bin storage-api -- \
  --storage sqlite \
  --db-path /tmp/aux-analytics-example-storage.db \
  --port 39124 \
  --enable-internal-helper-routes
```

Start aux-analytics:

```sh
cargo run -p analytics-api --bin aux-analytics-api -- \
  --config examples/local-aux-storage-duckdb/analytics-config.json
```

Use the API health endpoints to verify the poller has started and is saving
checkpoint state.
