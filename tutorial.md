# Aux Analytics Manifest Tutorial

Aux Analytics reads a JSON manifest that describes how DynamoDB or aux-storage stream records
become DuckDB or DuckLake tables. The manifest is intentionally generic: it can
ingest existing DynamoDB-compatible items as raw JSON documents, and teams can
add typed projections only where they want query-friendly columns.

## Quick Reference

| Table field                   | Required | Options                                                                                                             |
| ----------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------- |
| `source_table_name`           | yes      | Source table name in the stream event.                                                                              |
| `analytics_table_name`        | yes      | DuckDB/DuckLake table to create.                                                                                    |
| `source_table_name_prefix`    | no       | Optional source table prefix for matching families of tables.                                                       |
| `tenant_id`                   | no       | Fixed tenant id for every row from this registration.                                                               |
| `tenant_selector`             | no       | `none`, `table_name`, `attribute`, `attribute_regex`, `partition_key_prefix`, `table_name_or_partition_key_prefix`. |
| `row_identity`                | no       | `record_key`, `stream_keys`, `attribute`, `attribute_regex`.                                                        |
| `document_column`             | no       | Defaults to `"item"`. Set `null` to disable full-item JSON storage.                                                 |
| `skip_delete`                 | no       | `true` keeps rows when delete stream records arrive.                                                                |
| `retention`                   | no       | Static row retention with `period_ms` and a timestamp basis.                                                        |
| `condition_expression`        | no       | DynamoDB-compatible filter applied before source items are ingested.                                                |
| `expression_attribute_names`  | no       | Aliases for projection paths, such as `{ "#type": "type" }`.                                                        |
| `expression_attribute_values` | no       | Values referenced by condition expressions.                                                                         |
| `projection_attribute_names`  | no       | Top-level attributes to copy directly as columns.                                                                   |
| `projection_columns`          | no       | Named projections from nested paths with optional column types.                                                     |
| `columns`                     | no       | Explicit output columns for fully typed tables.                                                                     |
| `partition_keys`              | no       | DuckLake partition hints.                                                                                           |
| `clustering_keys`             | no       | DuckLake sort hints.                                                                                                |

Aux Analytics always creates the built-in output columns `tenant_id`,
`table_name`, and `__id`. Do not declare these names in `document_column`,
`columns`, `projection_attribute_names`, or `projection_columns`; use
`tenant_selector` and `row_identity` to populate them. Layout hints such as
`partition_keys` and `clustering_keys` may still reference built-in columns.
Retention-enabled tables also reserve `__ingested_at`, `__expiry`, and
`__missing_retention`.

Tenant selector options:

```json
{ "kind": "none" }
{ "kind": "table_name" }
{ "kind": "attribute", "attribute_name": "tenant_id" }
{
  "kind": "attribute_regex",
  "attribute_name": "pk",
  "regex": "^TENANT#(?<tenant_id>[^#]+)#",
  "capture": "tenant_id"
}
{ "kind": "partition_key_prefix", "attribute_name": "pk" }
{ "kind": "table_name_or_partition_key_prefix", "attribute_name": "pk" }
```

`partition_key_prefix` exists for compatibility with simple `TYPE#tenant#...`
keys. Prefer `attribute` when the item already stores a tenant field, or
`attribute_regex` when an existing key string must be parsed. Regex selectors
use named capture groups so the manifest documents exactly which value becomes
`tenant_id`.

Row identity options:

```json
{ "kind": "record_key" }
{ "kind": "stream_keys" }
{ "kind": "attribute", "attribute_name": "id" }
{
  "kind": "attribute_regex",
  "attribute_name": "sk",
  "regex": "^ORDER#(?<order_id>[^#]+)$",
  "capture": "order_id"
}
```

Use `stream_keys` for most existing tables because it is stable across updates.
Use `attribute` or `attribute_regex` when the item has a durable business key
that should become the analytics row id.

## Static Retention

Static retention is part of the manifest because it is a stable table contract.
The duration is always numeric milliseconds.

Use a source item unix epoch millisecond attribute:

```json
{
  "version": 1,
  "tables": [
    {
      "source_table_name": "audit_stream",
      "analytics_table_name": "audit_events",
      "tenant_selector": { "kind": "attribute", "attribute_name": "tenant_id" },
      "row_identity": { "kind": "attribute", "attribute_name": "event_id" },
      "projection_attribute_names": ["event_id", "tenant_id", "created_at_ms"],
      "retention": {
        "period_ms": 7776000000,
        "timestamp": { "kind": "attribute", "attribute_path": "created_at_ms" }
      }
    }
  ]
}
```

Use aux-analytics ingestion time:

```json
{
  "retention": {
    "period_ms": 2592000000,
    "timestamp": { "kind": "ingested_at" }
  }
}
```

Aux Analytics stores `__expiry` and the periodic retention sweeper deletes rows
where `__expiry` is in the past.

## Dynamic Tenant Retention

Dynamic tenant retention belongs in runtime config because it depends on an HTTP
policy source, credentials, cache TTL, and deployment-specific endpoints. The
returned policy value must be a DynamoDB numeric `N` value in milliseconds at
`duration_selector.attribute_path`.

Aux-storage `GetItem` example:

```json
{
  "analytics": {
    "retention": {
      "enabled": true,
      "sweep_interval_ms": 60000,
      "delete_batch_size": 500,
      "delete_batch_pause_ms": 250,
      "tables": [
        {
          "analytics_table_name": "audit_events",
          "default_period_ms": 7776000000,
          "strict": false,
          "timestamp": { "kind": "attribute", "attribute_path": "created_at_ms" },
          "tenant_policy": {
            "source": "aux_storage",
            "endpoint_url": "http://127.0.0.1:39124/storage",
            "request": {
              "get_item": {
                "table_name": "tenant_retention",
                "key": { "tenant_id": { "S": "${tenant_id}" } },
                "consistent_read": true
              }
            },
            "duration_selector": { "attribute_path": "retention.analytics_ms" },
            "cache_ttl_ms": 300000
          }
        }
      ]
    }
  }
}
```

DynamoDB GSI query example:

```json
{
  "analytics": {
    "retention": {
      "enabled": true,
      "tables": [
        {
          "analytics_table_name": "audit_events",
          "default_period_ms": 7776000000,
          "strict": true,
          "timestamp": { "kind": "ingested_at" },
          "tenant_policy": {
            "source": "dynamodb",
            "region": "us-east-1",
            "credentials": { "instance_keys": true },
            "request": {
              "query_table": {
                "table_name": "tenant_retention",
                "index_name": "tenant_id-index",
                "key_condition_expression": "tenant_id = :tenant_id",
                "expression_attribute_values": {
                  ":tenant_id": { "S": "${tenant_id}" }
                },
                "scan_index_forward": false,
                "limit": 1
              }
            },
            "duration_selector": { "attribute_path": "analytics.retention_ms" },
            "cache_ttl_ms": 300000
          }
        }
      ]
    }
  }
}
```

DynamoDB requests use the AWS SDK for SigV4 signing. Aux-storage requests retry
transport and 5xx failures 3 times with exponential backoff. If `strict` is
true and lookup fails, the row is retained with `__missing_retention = true`
and `__expiry = null` until repaired:

```sh
cargo run -p analytics-cli -- repair-retention \
  --manifest manifest.json \
  --config analytics-config.json \
  --duckdb analytics.duckdb \
  --table audit_events \
  --tenant-id tenant-a
```

## Minimal Generic Table

Use this when you already have data and do not want to change item shapes.

```json
{
  "version": 1,
  "tables": [
    {
      "source_table_name": "legacy_orders",
      "analytics_table_name": "orders",
      "tenant_selector": { "kind": "none" },
      "row_identity": { "kind": "stream_keys" },
      "document_column": "item"
    }
  ]
}
```

This creates an `orders` table with:

- `table_name`: the source table name from the manifest.
- `tenant_id`: an empty string because `tenant_selector.kind` is `none`.
- `__id`: a stable id derived from the stream record key map.
- `item`: the full DynamoDB item converted to JSON.

Example query:

```sql
select item->>'status' as status, count(*) as count
from orders
group by status;
```

The same table can be queried without raw SQL through the structured query
contract:

```json
{
  "query": {
    "analytics_table_name": "orders",
    "select": [
      {
        "kind": "document_path",
        "document_column": "item",
        "path": "status",
        "alias": "status"
      },
      { "kind": "count", "alias": "count" }
    ],
    "group_by": [
      {
        "kind": "document_path",
        "document_column": "item",
        "path": "status"
      }
    ],
    "order_by": [
      {
        "expression": {
          "kind": "document_path",
          "document_column": "item",
          "path": "status"
        },
        "direction": "asc"
      }
    ],
    "limit": 100
  }
}
```

## Generic Table With Tenant Metadata

If the table belongs to one tenant but the items do not contain tenant metadata,
set `tenant_id` directly.

```json
{
  "version": 1,
  "tables": [
    {
      "source_table_name": "customer_42_items",
      "analytics_table_name": "customer_items",
      "tenant_id": "customer_42",
      "row_identity": { "kind": "stream_keys" },
      "document_column": "item"
    }
  ]
}
```

## Shared Table With Tenant In The Key

If one source table contains many tenants and the partition key embeds tenant
identity, use a regex selector with a named capture group.

```json
{
  "version": 1,
  "tables": [
    {
      "source_table_name": "shared_items",
      "analytics_table_name": "shared_orders",
      "tenant_selector": {
        "kind": "attribute_regex",
        "attribute_name": "pk",
        "regex": "^TENANT#(?<tenant_id>[^#]+)#ORDER#(?<order_id>[^#]+)$",
        "capture": "tenant_id"
      },
      "row_identity": {
        "kind": "attribute_regex",
        "attribute_name": "pk",
        "regex": "^TENANT#(?<tenant_id>[^#]+)#ORDER#(?<order_id>[^#]+)$",
        "capture": "order_id"
      },
      "document_column": "item"
    }
  ]
}
```

For a key like `TENANT#tenant_a#ORDER#123`, Aux Analytics stores `tenant_a` in
`tenant_id` and uses `123` as the row id.

## Single-Table Design With Filters

Use `condition_expression` when one source table contains several item types but
each analytics table should store only one slice of that stream. The expression
uses DynamoDB condition-expression syntax and is evaluated against each stream
image before ingestion.

In this example, one DynamoDB-compatible source table named `app_items` stores
users and orders. Aux Analytics creates two DuckDB/DuckLake tables from that
same stream:

```json
{
  "version": 1,
  "tables": [
    {
      "source_table_name": "app_items",
      "analytics_table_name": "users",
      "tenant_selector": {
        "kind": "attribute_regex",
        "attribute_name": "pk",
        "regex": "^TENANT#(?<tenant_id>[^#]+)$",
        "capture": "tenant_id"
      },
      "row_identity": {
        "kind": "attribute_regex",
        "attribute_name": "sk",
        "regex": "^USER#(?<user_id>[^#]+)$",
        "capture": "user_id"
      },
      "document_column": "item",
      "condition_expression": "#type = :user_type",
      "expression_attribute_names": {
        "#type": "entity_type"
      },
      "expression_attribute_values": {
        ":user_type": { "S": "USER" }
      },
      "projection_columns": [
        {
          "column_name": "user_id",
          "attribute_path": "user_id",
          "column_type": { "kind": "primitive", "primitive": "var_char" }
        },
        {
          "column_name": "email",
          "attribute_path": "profile.email",
          "column_type": { "kind": "primitive", "primitive": "var_char" }
        }
      ]
    },
    {
      "source_table_name": "app_items",
      "analytics_table_name": "orders",
      "tenant_selector": {
        "kind": "attribute_regex",
        "attribute_name": "pk",
        "regex": "^TENANT#(?<tenant_id>[^#]+)$",
        "capture": "tenant_id"
      },
      "row_identity": {
        "kind": "attribute_regex",
        "attribute_name": "sk",
        "regex": "^ORDER#(?<order_id>[^#]+)$",
        "capture": "order_id"
      },
      "document_column": "item",
      "condition_expression": "#type = :order_type AND attribute_exists(total_cents)",
      "expression_attribute_names": {
        "#type": "entity_type"
      },
      "expression_attribute_values": {
        ":order_type": { "S": "ORDER" }
      },
      "projection_columns": [
        {
          "column_name": "order_id",
          "attribute_path": "order_id",
          "column_type": { "kind": "primitive", "primitive": "var_char" }
        },
        {
          "column_name": "total_cents",
          "attribute_path": "total_cents",
          "column_type": { "kind": "primitive", "primitive": "big_int" }
        }
      ]
    }
  ]
}
```

An item with `entity_type = "USER"` is ingested into `users` and skipped for
`orders`. An item with `entity_type = "ORDER"` and a `total_cents` attribute is
ingested into `orders` and skipped for `users`.

Filtering also applies to deletes and updates. If an old image matched a table
but the new image no longer matches, Aux Analytics removes that row from the
filtered analytics table. If neither image matches, the stream record is skipped
for that table.

Use `expression_attribute_names` when an attribute is a reserved word or should
be aliased, and use `expression_attribute_values` for placeholders such as
`:order_type`. Common filter forms include equality, comparison operators,
`attribute_exists(...)`, `attribute_not_exists(...)`, `begins_with(...)`,
`contains(...)`, `AND`, and `OR`.

## Add Query-Friendly Columns

Raw JSON is flexible, but repeated analytical queries are easier and faster with
typed columns. Add `projection_columns` to extract selected attributes while
still keeping the complete item.

```json
{
  "version": 1,
  "tables": [
    {
      "source_table_name": "legacy_orders",
      "analytics_table_name": "orders",
      "tenant_selector": { "kind": "none" },
      "row_identity": { "kind": "stream_keys" },
      "document_column": "item",
      "projection_columns": [
        {
          "column_name": "order_id",
          "attribute_path": "orderId",
          "column_type": { "kind": "primitive", "primitive": "var_char" }
        },
        {
          "column_name": "status",
          "attribute_path": "status",
          "column_type": { "kind": "primitive", "primitive": "var_char" }
        },
        {
          "column_name": "total_cents",
          "attribute_path": "totals.grandTotalCents",
          "column_type": { "kind": "primitive", "primitive": "big_int" }
        }
      ]
    }
  ]
}
```

Example query:

```sql
select status, sum(total_cents) as revenue_cents
from orders
group by status;
```

For API clients that should not send SQL, use `/tenant-query` with a target
tenant id and registered columns:

```json
{
  "target_tenant_id": "tenant_01",
  "query": {
    "analytics_table_name": "orders",
    "select": [
      { "kind": "column", "column_name": "status" },
      { "kind": "count", "alias": "count" }
    ],
    "filters": [
      {
        "kind": "eq",
        "expression": { "kind": "column", "column_name": "status" },
        "value": "paid"
      }
    ],
    "group_by": [{ "kind": "column", "column_name": "status" }],
    "limit": 100
  }
}
```

Structured queries are validated against the generated JSON schema and then
against the active manifest. Column references must be built-in columns,
projected columns, explicit columns, or the configured `document_column`.

## Attribute Names With Dots Or Reserved Words

Use `expression_attribute_names` when an attribute name needs an alias in an
attribute path.

```json
{
  "source_table_name": "legacy_items",
  "analytics_table_name": "legacy_items",
  "tenant_selector": { "kind": "none" },
  "row_identity": { "kind": "stream_keys" },
  "document_column": "item",
  "expression_attribute_names": {
    "#meta": "metadata.v1",
    "#type": "type"
  },
  "projection_columns": [
    {
      "column_name": "kind",
      "attribute_path": "#type",
      "column_type": { "kind": "primitive", "primitive": "var_char" }
    },
    {
      "column_name": "source",
      "attribute_path": "#meta.source",
      "column_type": { "kind": "primitive", "primitive": "var_char" }
    }
  ]
}
```

## Lists

Use `[]` in an attribute path to project values from list members.

```json
{
  "column_name": "tag_names",
  "attribute_path": "tags[].name",
  "column_type": { "kind": "list", "primitive": "var_char" }
}
```

## Row Identity

`row_identity` controls how inserts, updates, and deletes target the same
analytics row.

- `{ "kind": "stream_keys" }`: recommended for existing DynamoDB-compatible
  tables. The row id is derived from the stream record `Keys` map.
- `{ "kind": "record_key" }`: use when the caller supplies a stable `record_key`
  in the HTTP or Lambda ingestion request.
- `{ "kind": "attribute", "attribute_name": "id" }`: use a string attribute as
  the stable row id.
- `{ "kind": "attribute_regex", "attribute_name": "sk", "regex": "^USER#(?<id>[^#]+)$", "capture": "id" }`:
  extract the row id from a string attribute using a named capture group.

## Document Column

`document_column` stores the full item as JSON. It defaults to `"item"` when the
field is omitted. Set it to `null` only for a fully typed table where every
useful attribute is represented by explicit columns.

```json
{
  "source_table_name": "typed_orders",
  "analytics_table_name": "typed_orders",
  "tenant_selector": { "kind": "none" },
  "row_identity": { "kind": "stream_keys" },
  "document_column": null,
  "projection_columns": [
    {
      "column_name": "order_id",
      "attribute_path": "orderId",
      "column_type": { "kind": "primitive", "primitive": "var_char" }
    }
  ]
}
```

## DuckLake Layout Hints

`partition_keys` and `clustering_keys` are optional DuckLake layout hints. They
can refer to projected columns or built-in columns such as `tenant_id`.

```json
{
  "source_table_name": "orders",
  "analytics_table_name": "orders",
  "tenant_selector": { "kind": "none" },
  "row_identity": { "kind": "stream_keys" },
  "document_column": "item",
  "projection_columns": [
    {
      "column_name": "status",
      "attribute_path": "status",
      "column_type": { "kind": "primitive", "primitive": "var_char" }
    }
  ],
  "partition_keys": [{ "column_name": "tenant_id" }],
  "clustering_keys": [{ "column_name": "status", "order": "asc" }]
}
```

## Complex Aux-Fn Style Manifest

Aux-fn exports a manifest from product-owned table registrations. A typical
table keeps the full item, projects analytics-friendly columns, uses a fixed
tenant id supplied by registration, and adds DuckLake layout hints:

```json
{
  "version": 1,
  "tables": [
    {
      "source_table_name": "tenant_entities_t_01",
      "analytics_table_name": "tenant_01_users",
      "tenant_id": "t_01",
      "tenant_selector": { "kind": "table_name" },
      "row_identity": { "kind": "stream_keys" },
      "document_column": "item",
      "expression_attribute_names": {
        "#user": "user_name",
        "#org": "primary_org_id"
      },
      "projection_columns": [
        {
          "column_name": "user_id",
          "attribute_path": "id",
          "column_type": { "kind": "primitive", "primitive": "var_char" }
        },
        {
          "column_name": "username",
          "attribute_path": "#user",
          "column_type": { "kind": "primitive", "primitive": "var_char" }
        },
        {
          "column_name": "display_name",
          "attribute_path": "display_name",
          "column_type": { "kind": "primitive", "primitive": "var_char" }
        },
        {
          "column_name": "org_id",
          "attribute_path": "#org",
          "column_type": { "kind": "primitive", "primitive": "var_char" }
        },
        {
          "column_name": "roles",
          "attribute_path": "roles[].name",
          "column_type": { "kind": "list", "primitive": "var_char" }
        }
      ],
      "partition_keys": [{ "column_name": "tenant_id" }],
      "clustering_keys": [
        { "column_name": "org_id", "order": "asc" },
        { "column_name": "username", "order": "asc" }
      ]
    }
  ]
}
```

This shape is still domain-agnostic from aux-analytics' point of view: aux-fn
owns the registration and exports JSON; aux-analytics only validates the
manifest, creates the table, and ingests contract stream records.

## Recommended Adoption Path

1. Start with one manifest table per source table using `tenant_selector.none`,
   `row_identity.stream_keys`, and `document_column.item`.
2. Ingest a small stream sample and confirm that raw JSON queries work.
3. Add `condition_expression` when one source stream contains multiple item
   types and each analytics table should receive only some of them.
4. Use structured queries for product/API integrations that should be constrained
   to manifest-registered tables and fields.
5. Add `projection_columns` for fields used in filters, joins, grouping, and
   dashboards.
6. Add DuckLake layout hints only after query patterns are known.
7. Keep the manifest in source control and regenerate OpenAPI/schema docs as
   part of release validation.
