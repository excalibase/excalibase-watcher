# excalibase-watcher-go

Standalone CDC (Change Data Capture) agent. Streams PostgreSQL WAL and MySQL binlog changes to NATS JetStream.

Go rewrite of the Java/Spring Boot [excalibase-watcher](https://github.com/excalibase/excalibase-watcher). Single static binary (~27 MB), no JVM, ~30 MB memory at rest.

## What it does

```
 ┌───────────┐                  ┌────────────────┐                  ┌──────────────┐
 │ PostgreSQL│──── WAL ────────▶│ excalibase-    │──── publish ────▶│ NATS         │
 │  (slot)   │                  │  watcher-go    │                  │ JetStream    │
 └───────────┘                  │                │                  │ cdc.{schema} │
 ┌───────────┐                  │  + health      │                  │   .{table}   │
 │   MySQL   │──── binlog ─────▶│  + metrics     │                  └──────────────┘
 │  (canal)  │                  │  + offset save │
 └───────────┘                  └────────────────┘
```

- Postgres: logical replication via [`jackc/pglogrepl`](https://github.com/jackc/pglogrepl), pgoutput plugin
- MySQL: binlog replication via [`go-mysql-org/go-mysql/canal`](https://github.com/go-mysql-org/go-mysql)
- NATS: JetStream publisher via [`nats-io/nats.go/jetstream`](https://github.com/nats-io/nats.go)

## Quick start

```bash
# Build
make build                              # produces bin/watcher

# Run infra (Postgres + MySQL + NATS)
docker compose up -d

# Provision NATS stream ONCE (watcher does not create streams — see docs/MULTI_TENANT.md)
docker run --rm --network=host natsio/nats-box:latest \
  nats stream add CDC --subjects='cdc.>' --storage=file --retention=limits \
  --max-age=15m --discard=old --replicas=1 --defaults

# Run
cp config.example.yaml config.yaml
./bin/watcher --config config.yaml
```

Endpoints on `:8080`:
- `GET /healthz` — liveness (UP when a listener is running)
- `GET /readyz` — readiness
- `GET /metrics` — Prometheus metrics (see below)

### Metrics

| Metric | Type | Meaning |
|---|---|---|
| `cdc_events_total{type}` | counter | Events accepted onto the internal bus, by type |
| `cdc_nats_published_total{type}` | counter | Events acknowledged by JetStream, by type |
| `cdc_nats_errors_total` | counter | Failed publish attempts (each one is retried) |
| `cdc_lag_seconds` | gauge | `now − commit timestamp` of the last COMMIT record processed. Keeps growing while WAL is still arriving or an event awaits its NATS ack; reset to `0` when a primary keepalive arrives with nothing pending (the watcher is caught up), until the next commit. Refreshed on every commit, keepalive and slot sample |
| `cdc_slot_confirmed_flush_lsn` | gauge | `pg_replication_slots.confirmed_flush_lsn` as a 64-bit integer |
| `cdc_slot_restart_lsn` | gauge | `pg_replication_slots.restart_lsn` as a 64-bit integer |
| `cdc_slot_retained_wal_bytes` | gauge | WAL pinned by the slot: `pg_current_wal_lsn() − restart_lsn` (on a standby `pg_last_wal_receive_lsn()` is used) |
| `cdc_last_event_timestamp_seconds` | gauge | Unix time the watcher last processed a WAL data record |
| `cdc_slots_dropped_total` | counter | Orphaned replication slots dropped by the cleanup routine |

Slot gauges are sampled from `pg_replication_slots` every `postgres.slot_stats_interval_seconds` (default 15, env `WATCHER_POSTGRES_SLOT_STATS_INTERVAL_SECONDS`) over a small maintenance pool that reconnects on its own after a database restart or hibernation. Lag is derived from the Postgres commit timestamp, so it also measures clock skew between the database and the watcher.

### Orphaned replication slot cleanup

An inactive logical slot retains WAL forever. The watcher therefore keeps an owner registry table in the source database, `excalibase_cdc_slot_owners` (`slot_name`, `owner_id`, `heartbeat_at`, `released_at`; created at startup, changes on it are never emitted as CDC events), and runs a cleanup pass at startup and every `interval_minutes`:

1. Only logical slots of the current database whose name matches `slot_pattern` are considered (default `^cdc_`, which covers the chart default `cdc_slot` and the platform's `cdc_watcher`). Anything else is never touched.
2. The watcher's own slot is never dropped, and an `active` slot is never dropped.
3. If `retained_wal_threshold_bytes` > 0, slots retaining less WAL than that are kept.
4. A slot whose registry row is marked `released_at` (its owner shut down gracefully) is dropped on the next pass.
5. Otherwise the slot is dropped once its owner's `heartbeat_at` is older than `stale_after_minutes`. A slot with no registry row (created by an older watcher or by hand) uses the time this watcher first observed it inactive instead, so a mixed-version rollout is safe.

Each watcher heartbeats its row every `heartbeat_seconds` and marks `released_at` on SIGTERM. Ownership is keyed by `owner_id` (default: hostname, i.e. the pod name).

```yaml
postgres:
  owner_id: ""                       # WATCHER_POSTGRES_OWNER_ID, default hostname
  slot_cleanup:
    enabled: true                    # WATCHER_POSTGRES_SLOT_CLEANUP_ENABLED
    interval_minutes: 10             # WATCHER_POSTGRES_SLOT_CLEANUP_INTERVAL_MINUTES
    stale_after_minutes: 30          # WATCHER_POSTGRES_SLOT_CLEANUP_STALE_AFTER_MINUTES
    retained_wal_threshold_bytes: 0  # WATCHER_POSTGRES_SLOT_CLEANUP_RETAINED_WAL_THRESHOLD_BYTES (0 = any)
    dry_run: false                   # WATCHER_POSTGRES_SLOT_CLEANUP_DRY_RUN — log "would drop" only
    slot_pattern: "^cdc_"            # WATCHER_POSTGRES_SLOT_CLEANUP_SLOT_PATTERN
    heartbeat_seconds: 30            # WATCHER_POSTGRES_SLOT_CLEANUP_HEARTBEAT_SECONDS
```

If the database role cannot create the registry table, the watcher logs a warning, skips both heartbeat and cleanup, and streams CDC normally.

**Interplay with project pause / hibernation.** Cleanup runs inside the watcher connected to that database, so a paused project (watcher scaled to zero, CNPG cluster hibernated) drops nothing while paused; its WAL is not growing either. Stop the watcher *before* hibernating the cluster so the SIGTERM handler can mark `released_at`. On resume the restarted watcher re-claims its own slot by name (never dropping it) and, if the slot name changed in between, the released old slot is dropped on the first cleanup pass instead of being retained for `stale_after_minutes`. If the watcher was killed without SIGTERM (node loss), the slot is dropped once the heartbeat is `stale_after_minutes` old — only by a watcher that is connected to that database.

**Ack ordering.** The slot's `confirmed_flush_lsn` only advances past an event once JetStream has acknowledged it: the listener remembers the LSN of every publishable event it hands to the bus and holds the standby status at the last acked LSN while any is outstanding. A failed publish is retried with backoff (never skipped), so a crash or restart re-streams exactly the unpublished tail. Without a NATS publisher (`nats.enabled=false`) every received LSN is confirmed.

## Configuration

YAML file (default `config.yaml`) with environment variable override via `WATCHER_*` prefix. Nested keys use `_` as separator: `postgres.password` → `WATCHER_POSTGRES_PASSWORD`.

See `config.example.yaml` for the full schema.

**Secrets must be provided via env vars.** The config file allows empty string for `username`/`password`; Viper `BindEnv` overrides them from env.

## Deployment

### Kubernetes (Helm)

```bash
helm install watcher helm/excalibase-watcher-go/ -f helm/examples/postgres-only.yaml
```

Examples:
- `helm/examples/postgres-only.yaml`
- `helm/examples/mysql-only.yaml`
- `helm/examples/both-dbs.yaml`
- `helm/examples/multi-tenant.yaml` — N watchers sharing one NATS stream

**MySQL offset persistence** requires a PVC. Enabled by default in the chart (`mysql.persistence.enabled=true`).

**NATS stream provisioning** is separate from the chart by default. Opt into the chart's pre-install Job with `nats.streamInit.enabled=true` for dev clusters, or use terraform / external tooling for production.

### Docker

```bash
docker build -t excalibase/watcher-go .
docker run --rm -v $(pwd)/config.yaml:/etc/watcher/config.yaml \
  excalibase/watcher-go --config /etc/watcher/config.yaml
```

Image: distroless, nonroot, ~30 MB.

## Testing

```bash
make test              # unit tests (race detector)
make integration-test  # unit + integration (testcontainers)
make e2e-test          # starts docker-compose, runs E2E, tears down
```

E2E tests run the **real compiled binary** against real Postgres/MySQL/NATS containers. They cover:
- CRUD (insert/update/delete/truncate/DDL/type mapping/table filtering/chunked snapshot)
- Health + metrics endpoints
- **Regression**: no-replay-on-first-start, restart-resumes-from-offset, backpressure-no-loss, crash-mid-tx, both-DBs-simultaneously, PG-slot-resume
- **Edge cases**: JSON/JSONB columns, large transactions, binary data with control chars, schema evolution mid-stream
- **Multi-tenant**: shared-stream no-clobber, fail-fast when stream missing

30 E2E tests. See `e2e/` for details.

## Documentation

- [`CLAUDE.md`](./CLAUDE.md) — project context, design decisions, gotchas (read this first if you haven't touched the repo in weeks)
- [`docs/MULTI_TENANT.md`](./docs/MULTI_TENANT.md) — how to run N watchers against one shared NATS stream
- [`docs/JAVA_PARITY.md`](./docs/JAVA_PARITY.md) — payload-format compatibility with the Java watcher

## License

Apache 2.0 (same as upstream excalibase-watcher).
