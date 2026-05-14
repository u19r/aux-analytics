# Retention Examples

`manifest-static.json` shows static manifest retention using a source timestamp.
`analytics-config-dynamic.json` shows dynamic tenant retention through an
aux-storage `GetItem` request. DynamoDB configs use the same `get_item` or
`query_table` request shape; `query_table.limit` must be `1`.
