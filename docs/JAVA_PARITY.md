# Payload compatibility with the Java watcher

The Go watcher publishes CDC events with a byte-compatible JSON payload so consumers written for the Java watcher continue to work unchanged.

## Event envelope

```json
{
  "type": "INSERT",
  "schema": "public",
  "table": "users",
  "data": "{\"id\":1, \"name\":\"Alice\"}",
  "rawMessage": "INSERT",
  "lsn": "0/1A2B3C4D",
  "timestamp": 1735689600000,
  "sourceTimestamp": 1735689599999
}
```

| Field | Type | Notes |
|---|---|---|
| `type` | string | One of `BEGIN`/`COMMIT`/`INSERT`/`UPDATE`/`DELETE`/`DDL`/`TRUNCATE`/`HEARTBEAT`. **Serialized as string, not int** — Go uses a custom `MarshalJSON`. |
| `schema` | string | Database schema / MySQL db name |
| `table` | string | Table name. Empty for BEGIN/COMMIT/DDL/HEARTBEAT |
| `data` | string | See "data field" below — **the data field is a JSON-encoded string, not a nested object**, matching Jackson's behavior |
| `rawMessage` | string | Operation label (informational) |
| `lsn` | string | PG: LSN `0/1A2B3C4D`. MySQL: `file:pos` |
| `timestamp` | int64 | epoch millis when the watcher created the event |
| `sourceTimestamp` | int64 | epoch millis from the DB (PG: BEGIN commit time; MySQL: binlog event header). 0 if unknown |

## Data field

The `data` field is a **string containing JSON**, not a nested JSON object. This matches Jackson's default behavior in the Java watcher, where `CDCEvent.data` is declared as `String`.

Consumers parse it with a second JSON decode:
```go
var envelope cdc.Event
json.Unmarshal(msg.Data(), &envelope)
var row map[string]any
json.Unmarshal([]byte(envelope.Data), &row)   // data is a string; decode it
```

### INSERT
```json
{"id":1, "name":"Alice", "email":"alice@test.com"}
```

### UPDATE
```json
{"old":{"id":1, "name":"Alice"}, "new":{"id":1, "name":"Bob"}}
```

(PG: requires `REPLICA IDENTITY FULL` to get the `old` block. MySQL via canal: always has both.)

### DELETE
```json
{"id":1, "name":"Alice"}
```

### TRUNCATE
```json
{"options":0}
```

### DDL

**PostgreSQL** (via event trigger + `_cdc_ddl_log` table):
```json
{"id":"1", "command_tag":"ALTER TABLE", "object_type":"table", "schema_name":"public", "object_identity":"public.users", "query":"ALTER TABLE users ADD COLUMN age INT"}
```

**MySQL** (via canal `OnDDL`):
```
ALTER TABLE users ADD COLUMN age INT
```
(Raw SQL string, not JSON. Matches Java behavior. `CDCService` skips JSON validation for DDL events.)

### BEGIN / COMMIT / HEARTBEAT
`data` is empty.

## Type mapping

### PostgreSQL (OID-driven)

| PG type | OID | JSON output |
|---|---|---|
| `smallint`, `integer`, `bigint`, `oid` | 21, 23, 20, 26 | unquoted number |
| `real`, `double precision` | 700, 701 | unquoted number |
| `numeric` | 1700 | unquoted number (preserves precision as string would lose it) |
| `boolean` | 16 | `true` / `false` |
| `json`, `jsonb` | 114, 3802 | **nested JSON** (not quoted string). Falls back to quoted string if the value fails `json.Valid` |
| `text`, `varchar`, everything else | — | quoted string, escaped per RFC 8259 (including control chars) |
| NULL | — | `null` |
| Unchanged TOAST | — | `"unchanged"` (literal string) |

### MySQL (canal-driven)

| MySQL type | JSON output |
|---|---|
| numeric types | unquoted number |
| `BOOLEAN` / `TINYINT(1)` | `true` / `false` |
| `DATETIME`, `TIMESTAMP` | ISO 8601 with **milliseconds**: `"2026-04-20T12:34:56.789Z"` (not plain RFC3339) |
| `DATE` | `"yyyy-MM-dd"` |
| `TIME` | `"HH:mm:ss"` |
| `DECIMAL` | unquoted number (preserves precision) |
| `JSON` | **nested JSON** (not quoted string) |
| binary types (`BINARY`, `VARBINARY`, `BLOB`) | UTF-8 string if valid, else base64 |
| `TEXT`, `VARCHAR`, `CHAR` | quoted string, escaped per RFC 8259 |

## NATS subject format

```
{prefix}.{schema}.{table}
```

- `{prefix}` = `nats.subject_prefix` config (default `cdc`, multi-tenant uses `cdc.tenant-a` etc.)
- `{schema}` = event schema, or `default` if empty
- `{table}` = event table, or `_ddl` if empty (DDL events)
- Non-wildcard characters only: `.` `*` `>` are replaced with `_` in `{schema}` and `{table}` to prevent subject injection

Examples:
- INSERT on `public.users`: `cdc.public.users`
- DDL on `public`: `cdc.public._ddl`

## Differences from Java (that shouldn't affect consumers)

These are byte-level differences that matter only if you're doing `==` comparison of raw JSON bytes. Every well-behaved consumer handles both equivalently.

- **Whitespace**: Go uses minimal whitespace (`{"a":1,"b":2}`), Java Jackson uses `{"a":1, "b":2}` (single space after `,`). Same JSON semantics.
- **Unicode escapes**: Go escapes U+0000–U+001F as `\uXXXX` per RFC 8259. Java's escapeJSON only handled `\n \r \t \\ \"` — control chars passed through as raw bytes, which is technically invalid JSON. If your consumer strictly validates JSON, the Go output is more correct.
- **Key ordering**: Go struct fields iterate in source declaration order; Jackson may order alphabetically depending on config. Consumers must not rely on key order.

## Verification

`internal/cdc/service_test.go::TestEventJSONFormat` asserts the envelope shape byte-for-byte (type as string, timestamp as number, data preserved as string, etc.) against a hardcoded expected output. Breaking this test means breaking Java-compat.

The E2E tests (`TestE2E_PG_JSONBColumn`, `TestE2E_MySQL_JSONColumn`, etc.) verify the `data` field structure under real DB conditions.
