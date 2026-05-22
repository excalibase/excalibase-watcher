# Multi-tenant deployment

One NATS JetStream cluster, one shared stream, N per-tenant watchers.

## Model

```
                         ┌─────────────────────────────┐
                         │  NATS JetStream (shared)    │
                         │   stream: CDC                │
                         │   subjects: ["cdc.>"]        │   ← set ONCE by infra
                         │   retention: limits/15m file │
                         └──────────┬──────────────────┘
                                    │
         ┌──────────────┬───────────┼───────────┬──────────────┐
         │              │           │           │              │
  watcher-tenant-a   watcher-B   watcher-C   ...          graphql
  subject_prefix:    prefix:     prefix:                  consumer:
  cdc.tenant-a       cdc.B       cdc.C                    cdc.{jwt.tenantId}.>
```

Each watcher publishes under its own prefix. The stream's `cdc.>` wildcard captures every event automatically — no topology changes when you add a new tenant.

## Why not stream-per-tenant?

Simpler per-tenant isolation, but:
- Every new tenant = new stream = new JetStream resource to provision + monitor
- Fan-out at the subscriber side (graphql) is harder: must subscribe to N streams
- Duplicated retention/replication settings across streams

Shared stream + subject-prefix tenancy is the same pattern Kafka uses (topic prefixes + ACLs). Infra provisions once; tenants slot in without modifying anything.

## The rule the watcher enforces

**The watcher never creates or modifies the stream.** It will exit non-zero with a clear error if the stream doesn't exist:

```
NATS stream "CDC" does not exist — provision it before starting the watcher
(e.g. `nats stream add CDC --subjects='cdc.>'`). The watcher is a pure
publisher and will not create or modify streams.
```

This is the safeguard against the "last writer wins" bug: if the watcher called `CreateOrUpdateStream` on every boot with its own subject prefix, tenant B starting would overwrite tenant A's subjects and break tenant A silently.

## Provisioning the stream (once per cluster)

Pick one. All three produce the same result.

### Option 1 — `nats` CLI (simplest)

```bash
nats stream add CDC \
  --subjects='cdc.>' \
  --storage=file \
  --retention=limits \
  --max-age=15m \
  --discard=old \
  --max-msgs=-1 --max-bytes=-1 --max-msg-size=-1 \
  --replicas=3 \
  --dupe-window=2m \
  --defaults
```

### Option 2 — Helm pre-install Job (this chart)

```yaml
# values.yaml
nats:
  streamInit:
    enabled: true
    subjects: "cdc.>"
    replicas: 3
```

The chart runs a `nats-box` Job with `"helm.sh/hook": pre-install,pre-upgrade` that executes the CLI command idempotently (`|| echo "already exists"`). Good for dev clusters. **Do not use in prod** — stream lifetime shouldn't be coupled to a release.

### Option 3 — Terraform (`nats-io/jetstream` provider)

```hcl
resource "jetstream_stream" "cdc" {
  name      = "CDC"
  subjects  = ["cdc.>"]
  storage   = "file"
  retention = "limits"
  max_age   = 15 * 60
  replicas  = 3
}
```

### Option 4 — `nats-box` init container in a shared infra chart

Same CLI command, run as a Kubernetes Job in whatever chart owns your NATS deployment.

## Deploying N tenants

```bash
# One per tenant. The only difference is subjectPrefix and the DB credentials.
helm install watcher-tenant-a helm/excalibase-watcher-go/ \
  -f helm/examples/multi-tenant.yaml \
  --set nameOverride=watcher-tenant-a \
  --set nats.subjectPrefix=cdc.tenant-a \
  --set postgres.url="postgres://tenant-a-db:5432/app?replication=database" \
  --set postgres.existingSecret=tenant-a-db-creds

helm install watcher-tenant-b helm/excalibase-watcher-go/ \
  -f helm/examples/multi-tenant.yaml \
  --set nameOverride=watcher-tenant-b \
  --set nats.subjectPrefix=cdc.tenant-b \
  --set postgres.url="postgres://tenant-b-db:5432/app?replication=database" \
  --set postgres.existingSecret=tenant-b-db-creds
```

Each tenant:
- Has its own PG replication slot (separate slot name per release via `postgres.slotName`, or use `nameOverride` suffix)
- Has its own MySQL offset file (on its own PVC)
- Publishes under its own subject prefix
- Shares the NATS stream with all other tenants

## Consumer side (e.g., graphql)

Consumers filter by subject:

```go
// NATS JetStream Go
consumer, _ := js.CreateConsumer(ctx, "CDC", jetstream.ConsumerConfig{
  FilterSubject: "cdc." + tenantIdFromJWT + ".>",
  DeliverPolicy: jetstream.DeliverNewPolicy,
})
```

For graphql specifically: use an **ephemeral consumer** per WebSocket subscription (lives as long as the socket). NOT a durable global consumer — that would give every subscriber every tenant's events.

## Optional hardening: NATS accounts/permissions

Subject-prefix isolation relies on each watcher behaving correctly. If you want NATS itself to enforce tenant boundaries, use accounts:

```
account TENANT_A {
  users = [
    { user: watcher-a, permissions: { publish: ["cdc.tenant-a.>"] } }
  ]
}
account TENANT_B {
  users = [
    { user: watcher-b, permissions: { publish: ["cdc.tenant-b.>"] } }
  ]
}
account GRAPHQL {
  users = [
    { user: gql, permissions: { subscribe: ["cdc.>"], publish: [] } }
  ]
}
```

NATS rejects cross-tenant publishes at the bus layer even if a watcher is misconfigured. Worth doing before onboarding the first external tenant.

## Verifying the setup works

```bash
# 1. Stream exists with the expected subjects
nats stream info CDC
# Config.Subjects should be ["cdc.>"], not a tenant-specific prefix.

# 2. Both tenants publishing
nats sub "cdc.tenant-a.>" &
nats sub "cdc.tenant-b.>" &

# 3. Trigger an INSERT on each tenant's DB and verify only the right subscriber sees it.
```

The `TestE2E_SharedStream_NoTopologyClobber` test exercises exactly this scenario against real containers.

## Troubleshooting

**Both tenants configured, only one receiving events.**
Check `nats stream info CDC | grep Subjects`. If it shows a tenant-specific prefix instead of `cdc.>`, an older (pre-fix) watcher clobbered it on boot. Delete and re-provision the stream.

**Watcher exits with "NATS stream X does not exist".**
It's telling you the truth. Provision the stream — see "Provisioning" above.

**Events from tenant A appearing under tenant B's prefix.**
Check the tenant's `subject_prefix` config. Then check NATS account/user permissions if you've enabled them.
