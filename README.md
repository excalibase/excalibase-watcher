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
- `GET /metrics` — Prometheus metrics (`cdc_events_total`, `cdc_nats_published_total`, `cdc_nats_errors_total`)

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
