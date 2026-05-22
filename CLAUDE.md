# excalibase-watcher-go — project context

This file exists so you (or Claude) don't have to re-derive the design decisions next time you open the repo cold.

---

## Why Go?

The Java/Spring Boot version is a perfectly good library for apps that already embed in Spring. But as a **standalone CDC agent** (its primary deployment mode), Spring Boot is ~400 MB of image, 250 MB+ memory baseline, 2–5 s startup.

Go gives us:
- **~27 MB binary**, ~30 MB memory baseline, instant startup
- Same feature set (verified against the Java test matrix)
- Same NATS payload format (backward-compatible — see `docs/JAVA_PARITY.md`)
- Simpler deployment: single binary, distroless image, no JRE

The Java repo stays for users who want an embeddable Spring library. The Go binary is what ships to Kubernetes.

---

## The invariants — do not break these

These are load-bearing design decisions. Violating them is how every bug in this repo has happened.

### 1. The watcher is a **pure NATS publisher**.
It never calls `CreateOrUpdateStream`. It only verifies `Stream()` exists and fails fast otherwise. Reason: in multi-tenant deployments (N watchers sharing one stream), `CreateOrUpdateStream` would have each tenant overwrite the others' subjects list, breaking everyone except the latest-started tenant.

Infra provisions the stream **once** with a wildcard: `subjects=['cdc.>']`. Each tenant watcher uses a sub-prefix (`cdc.tenant-a`, `cdc.tenant-b`) that falls under the wildcard. No topology change per tenant.

Code: `internal/nats/publisher.go::ensureStream`.

### 2. **Offset save follows NATS ack, NOT canal's XID position.**
Canal's `OnXID` used to save the binlog offset after each transaction. But `HandleEvent` is non-blocking on the channel — events can be buffered (100k) and not yet published when canal moves past them. A crash between "offset saved" and "publisher acked" = data loss.

Now: the NATS publisher updates `lastAckedLSN` atomically after each successful `js.Publish` ack. The MySQL listener runs a background `offsetSaver` goroutine that polls `publisher.LastAckedLSN()` every 1 s and persists that position. Offset is always ≤ delivered. Crash = re-read from binlog starting at the last acked position. At-least-once holds.

Code: `internal/mysql/listener.go::offsetSaver`, `internal/nats/publisher.go::lastAckedLSN`.

### 3. **No silent event drops.**
Originally, `HandleEvent` did `select { case sub.ch <- event; default: drop }`. Under any backpressure, events vanished with just a log line. Fixed by switching to a blocking send with rate-limited backpressure logging (once per 10 s per subscriber). If NATS is slow, canal eventually blocks — which is correct: MySQL retains the binlog, we catch up when NATS recovers.

Buffer size: 100k events (was 10k). For realistic CDC throughput (100–1000 evt/s), this is seconds of runway.

Code: `internal/cdc/service.go::deliver`.

### 4. **Canal starts at current master position, never position 0.**
`canal.Run()` with `Dump.ExecutionPath=""` (which we want — we don't want canal's built-in `mysqldump` to block startup) starts at binlog position **0 of the oldest file** — i.e. it replays everything. We call `c.GetMasterPos()` first and use `RunFrom(pos)` explicitly. The returned position is also saved to the offset store so restart stays consistent.

Code: `internal/mysql/listener.go::Start` (look for `GetMasterPos`).

### 5. **Never trust the Dockerfile base to keep CGO off.**
The go-mysql library compiles with `CGO_ENABLED=0`. Verify this in CI — if anyone adds a CGO dependency later, the distroless/static image breaks.

Code: `Makefile::build` sets `CGO_ENABLED=0`; `Dockerfile` inherits.

### 6. **Identifier validation is strict.**
Anywhere a config value is interpolated into SQL or a regex: `validateIdentifier(s)` must return ok first. This includes:
- Postgres publication name + slot name (spliced into replication protocol args)
- Postgres schema/table names in snapshot queries
- MySQL schema/table names in canal's `IncludeTableRegex` (must also use `regexp.QuoteMeta`)
- Schema/table names extracted from dump file parsing

Regex: `^[a-zA-Z_][a-zA-Z0-9_]*$`. No `$` char (was in there; removed).

Code: `internal/postgres/setup.go::validateIdentifier`.

### 7. **JSON/JSONB columns emit as raw nested JSON.**
Before: `{"payload":"{\"foo\":\"bar\"}"}` (consumers had to double-decode).
After: `{"payload":{"foo":"bar"}}`.

Triggered by OID check (PG: `json=114`, `jsonb=3802`) or canal type code (MySQL: `schema.TYPE_JSON=11`). Falls back to quoted string if `json.Valid` returns false.

### 8. **Payload format matches Java byte-for-byte (where it matters).**
Consumers written against the Java watcher must work against the Go watcher without changes. Critical:
- `type` is a string (`"INSERT"`), not an int. Custom `MarshalJSON` on `EventType`.
- MySQL DDL `data` is raw SQL (`ALTER TABLE ...`), not a JSON object. CDCService skips JSON validation for non-DML types.
- MySQL timestamps use millisecond precision (`2006-01-02T15:04:05.000Z07:00`), not plain RFC3339.

See `docs/JAVA_PARITY.md` for the full list.

---

## Gotchas I wasted hours on

### MySQL canal

- **`Dump.ExecutionPath=""` → starts at position 0.** Explicit `GetMasterPos` + `RunFrom` is required. See invariant #4.
- **All rows in one `WRITE_ROWS` batch share `Header.LogPos`.** When you batch-insert 50 rows in one statement, all 50 OnRow calls carry the same LSN. Don't use LSN as an event primary key.
- **Canal's `currentFile` is tracked via `OnRotate`**, not provided on `OnRow`. You must combine `OnRotate`'s `NextLogName` with `Header.LogPos` to reconstruct the full position. We seed `currentFile` from `startPos.File` so we have a value before the first rotate.
- **`MYSQL_PWD` in healthcheck, not `-pXXX`.** `-p` on the mysqladmin command line exposes the password in `ps`. Use env var.

### PostgreSQL / pglogrepl

- **`replication=database` must be in the connection string** or `START_REPLICATION` fails with a cryptic syntax error. `pgjdbc` does this implicitly; Go does not.
- **Acknowledge with `lastReceivedLSN + 1`**, not `lastReceivedLSN`. `pgjdbc` does the `+1` internally; Go requires it explicit.
- **Snapshot connection needs a different URL** — strip `replication=database` before using `pgx.Connect` for snapshot queries (otherwise you get a replication connection which can't run normal SELECTs).
- **`CREATE PUBLICATION FOR ALL TABLES` captures future tables too.** Good — but be aware that creating the publication happens at watcher startup, so any table created **before** the publication existed may not be included until it's rediscovered via DDL.
- **pgoutput column marker `u`** means "unchanged (TOAST)" — serialize as `"unchanged"` string, not skip the column.
- **JSONB OID is 3802**, JSON is 114, bool is 16, numerics are `{20,21,23,26,700,701,1700}`. These are hardcoded because they're stable.

### NATS JetStream

- **`CreateOrUpdateStream` CLOBBERS existing subjects list.** See invariant #1. This is the single most painful bug I've seen — works fine with one watcher, silently breaks when you add a second.
- **Consumer `DeliverPolicy.New`** is the default you want for live CDC tests; `DeliverAllPolicy` for "catch up from the start of the stream" tests (e.g. snapshot tests, resume-from-offset tests).
- **Stream config retention=`limits`** with `discard=old` means: when disk/msg/age limits are hit, oldest events are discarded. Subscribers must either be fast enough or keep up via durable consumer state.

### Testing

- **Each E2E test uses unique stream names and PG replication slot names.** Otherwise `TestMain`'s cross-test slot cleanup races with still-running tests.
- **`ensureNATSStream` in tests must be idempotent.** If a test provisions a wildcard stream (`cdc.>`) and a helper tries to "ensure" a tenant-prefix stream on the same name, it would clobber the wildcard. Fixed: `ensureNATSStream` skips if stream already exists.
- **MySQL testcontainers init takes ~60–90 s.** Don't set tight timeouts. Prefer `wait.ForLog("ready for connections").WithOccurrence(2)` (MySQL logs this twice during init) with a 180s timeout.
- **MySQL crash test needs `fetchEvents(count*3, ...)`.** After crash+restart, the stream may contain phase1 events + phase1 replays + phase2 events. Fetching `count*1` misses late phase2 rows.

### Helm

- **`nats.streamInit.enabled=false` by default.** Pre-install Job is opt-in; prod should provision streams out-of-band via terraform.
- **MySQL PVC mount `/data/binlog.offset`** is default. If `mysql.persistence.enabled=false`, restart replays from whatever master position was queried at last start (not "latest master position" — the persisted offset).

---

## Where to look first when things break

| Symptom | Check |
|---|---|
| "stream does not exist" | `nats stream ls` — provision it (see `docs/MULTI_TENANT.md`) |
| "no persisted offset, starting from current master position" on every restart | PVC not mounted, or `mysql.offsetFile` path is not writable |
| Events arriving out of order | Check goroutine count — more than one canal goroutine per DB = bug |
| "subscriber channel full, applying backpressure" repeatedly | NATS is slow or down; check `js.Publish` latency |
| Watcher exits with `invalid identifier` | Config has a non-identifier char in a schema/table/slot/publication name |
| Payload `type` is an int instead of a string | You're reading from a stream populated by an old (pre-fix) Go watcher; `cdc.Event.MarshalJSON` now emits string |
| `sawPhase2 = false` in a restart test | Offset save throttled (>100 evt or >1 s). Flush in `Stop()` should catch it; if not, bump test fixture size |

---

## File map

```
main.go                          # Cobra CLI, lifecycle, graceful shutdown
internal/
  cdc/
    event.go                     # Event struct + EventType (custom MarshalJSON)
    service.go                   # Channel-based event bus, blocking send + backpressure
  postgres/
    listener.go                  # pglogrepl WAL consumer, reconnect, heartbeat
    parser.go                    # pgoutput binary parser (B/C/R/I/U/D/T)
    setup.go                     # Publication/slot/DDL trigger creation, validateIdentifier
    snapshot_chunked.go          # PK-based SELECT chunking
    snapshot_dump.go             # pg_dump COPY format parser
  mysql/
    listener.go                  # canal-based binlog consumer, offsetSaver goroutine
    offset.go                    # FileOffsetStore
    snapshot_dump.go             # mysqldump INSERT parser
  nats/
    publisher.go                 # JetStream publisher, LastAckedLSN tracking
  jsonutil/escape.go             # Shared RFC 8259 JSON string escape
  config/config.go               # Viper YAML + env var
  health/health.go               # HTTP /healthz /readyz
  metrics/metrics.go             # Prometheus counters
  schema/history.go              # FileSchemaHistoryStore

e2e/                             # Real-binary + docker-compose E2E tests
helm/excalibase-watcher-go/      # Helm chart
helm/examples/                   # postgres-only, mysql-only, both-dbs, multi-tenant
```

---

## What's not tested (real gaps)

If you hit a bug in any of these, it's uncharted territory — add a test before fixing:

- Long-running (24h+) stability — no memory/FD leak tests
- Network partition between watcher and MySQL mid-transaction
- Upgrade path between offset-file formats (not versioned yet)
- TLS for DB connections or NATS (infra exists in libs, not wired in config)
- UUID / PG array / hstore / citext / jsonpath column types
- Very large rows (>10 MB TEXT/BLOB)
- Crash during initial snapshot (restart should restart snapshot; not verified)
- MySQL 8.4 GTID-based replication (we use file+pos)
- Multi-region NATS failover

---

## Related context

- Java watcher at `/home/duc/Documents/duk/excalibase-watcher` — keep payload compatible with it
- `excalibase-graphql` needs a separate PR to make its `NatsCDCService` tenant-aware (JWT-filtered per-subscription consumers) — this is not in this repo
- Upstream libraries worth bookmarking:
  - https://pkg.go.dev/github.com/jackc/pglogrepl
  - https://pkg.go.dev/github.com/go-mysql-org/go-mysql/canal
  - https://pkg.go.dev/github.com/nats-io/nats.go/jetstream
