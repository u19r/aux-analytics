// cli constants.
pub(crate) const ROOT_LONG_ABOUT: &str = r"Run and operate Aux Analytics.

Aux Analytics ingests source storage stream records into DuckDB-compatible analytical tables,
queries those tables through an embedded engine, and exposes durable Day 2 operation controls for
planning and monitoring backfills.

Common workflows:
  aux-analytics schema
      Print the analytics manifest JSON Schema.

  aux-analytics config-schema
      Print the runtime configuration JSON Schema.

  aux-analytics init --manifest manifest.json --duckdb analytics.duckdb
      Create or migrate the analytical tables declared by a manifest.

  aux-analytics tenant-query --target-tenant-id tenant-a --query query.json \
    --manifest manifest.json --duckdb analytics.duckdb
      Run a structured query with an engine-owned tenant predicate.

  aux-analytics backfill plan --config config.json --target-table users --output text
      Dry-run a safe existing-table adoption plan.

  aux-analytics backfill run --request backfill-execution.json --fixture fixture.json \
    --operation-store-duckdb operations.duckdb
      Execute a local fixture backfill through durable operation state.

    aux-analytics check run --operation-store-duckdb operations.duckdb \
    --operation-id check_123 --target-table users --source-rows source.json \
    --destination-rows destination.json
      Run a local read-only reconciliation check and persist its report.

  aux-analytics privacy-fix raw-backup --request privacy-fix.json \
    --operation-store-duckdb operations.duckdb
      Execute one complete privacy_fix MaintenanceRequest and persist incident evidence.

  aux-analytics fix table --request table-fix.json --raw-backup-root backups \
    --operation-store-duckdb operations.duckdb
      Dry-run a table repair request from local raw backup truth and persist the report.

  aux-analytics trim plan --request trim.json --destination-duckdb analytics.duckdb \
    --operation-store-duckdb operations.duckdb
      Run a destructive trim request and persist privacy-safe evidence.

  aux-analytics operations status --duckdb operations.duckdb --operation-id op_123 --output text
      Inspect a durable operation.

  aux-analytics completions zsh > ~/.zfunc/_aux-analytics
      Generate shell completion scripts for local installation.

Configuration model:
  Most commands accept --config and --overrides PATH=VALUE. CLI flags override configuration for
  the current invocation only. Backend flags choose the destination store directly when supplied.

Backend selection:
  --duckdb PATH uses a local DuckDB database.
  --ducklake-sqlite-catalog PATH uses DuckLake with a SQLite catalog.
  --ducklake-aux-catalog PATH uses DuckLake with an aux metadata catalog.
  --ducklake-data-path PATH configures the DuckLake data/object location.

Safety model:
  Public raw SQL is intentionally limited. Prefer structured query commands for tenant-scoped
  reads. Backfill planning is dry-run only; execution commands must route through durable operation
  state and audit trails.";
