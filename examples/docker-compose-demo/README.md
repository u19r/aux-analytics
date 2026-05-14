# Docker Compose Demo

This demo builds and runs a complete local stack:

- aux-storage as the storage-compatible source service;
- aux-analytics as a standalone poller and query API.

The analytics service has `/ingest` disabled. Data reaches DuckDB only through
the configured aux-storage stream poller.

From this directory:

```sh
docker compose build
docker compose up -d storage analytics
```

`storage-init` runs automatically before analytics starts. It creates the source
table so the analytics poller begins cleanly instead of polling a missing table.

Useful endpoints on the host:

| Service | URL |
| --- | --- |
| aux-storage ready | `http://127.0.0.1:39124/storage/ready` |
| aux-analytics ready | `http://127.0.0.1:39090/ready` |
| aux-analytics health | `http://127.0.0.1:39090/health` |
| aux-analytics metrics | `http://127.0.0.1:39090/metrics` |

Query the ingested row through the tenant-scoped structured query API:

```sh
curl -sS http://127.0.0.1:39090/tenant-query \
  -H 'content-type: application/json' \
  -d '{"target_tenant_id":"tenant_01","query":{"analytics_table_name":"users","select":[{"kind":"column","column_name":"email"}],"filters":[],"group_by":[],"order_by":[{"expression":{"kind":"column","column_name":"email"},"direction":"asc"}],"limit":100}}'
```

Stop and remove local data:

```sh
docker compose down -v
```
