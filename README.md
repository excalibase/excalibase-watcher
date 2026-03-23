# excalibase-watcher

A lightweight Java CDC (Change Data Capture) server that streams database changes to NATS JetStream in real-time. Supports PostgreSQL (WAL) and MySQL (binlog). Any service in any language can subscribe to INSERT, UPDATE, DELETE, DDL, and TRUNCATE events without touching the database.

```
[Postgres WAL / MySQL binlog]
          |
  excalibase-watcher-api   (Spring Boot server)
          |
    NATS JetStream
      |         |         |
  cdc.public.orders   cdc.public.users   cdc.public._ddl
      |         |         |
  inventory-svc   search-indexer   schema-tracker
```

---

## Features

| Feature | Postgres | MySQL |
|---------|----------|-------|
| INSERT / UPDATE / DELETE | pgoutput WAL stream | binlog replication |
| DDL capture (CREATE/ALTER/DROP) | Event trigger + `_cdc_ddl_log` table | QUERY event filtering |
| TRUNCATE events | pgoutput 'T' message | N/A |
| Transaction boundaries (BEGIN/COMMIT) | pgoutput B/C messages | XID + QUERY events |
| Real column names | From RELATION message | From INFORMATION_SCHEMA |
| Type-aware JSON | INT/FLOAT/NUMERIC as numbers, BOOL as boolean | Date/Timestamp as ISO, BigDecimal as number, binary as base64 |
| Table filtering | `app.cdc.postgres.tables=orders,users` | `app.cdc.mysql.tables=orders,users` |
| Chunked snapshot | JDBC SELECT before CDC starts | JDBC SELECT before CDC starts |
| Dump file snapshot | Parse `pg_dump` COPY format | Parse `mysqldump` INSERT format |
| Reconnect with backoff | Exponential 1s to 30s | Exponential 1s to 30s |
| Heartbeat (WAL bloat prevention) | Every 30s of inactivity | Built-in via binlog connector |
| Offset persistence | Replication slot (server-side) | `FileBinlogOffsetStore` (file) |
| Source timestamps | From pgoutput BEGIN message | From binlog event header |
| Schema history | `FileSchemaHistoryStore` (JSON per table) | Interface ready |
| Health check | Spring Actuator `/actuator/health` | Same |
| Metrics | Micrometer `cdc.events.total`, `cdc.nats.published` | Same |

---

## Modules

| Module | Description |
|--------|-------------|
| `excalibase-watcher-core` | `CDCService` + `CDCEvent` + health indicator + schema history |
| `excalibase-watcher-postgres` | PostgreSQL WAL listener (`PostgresCDCListener` + `PgOutputParser`) |
| `excalibase-watcher-mysql` | MySQL binlog listener (`MysqlBinlogListener`) |
| `excalibase-watcher-nats` | NATS JetStream publisher |
| `excalibase-watcher-api` | Spring Boot CDC server + E2E tests |

---

## Quick start

### 1. Start infrastructure

```bash
docker compose up -d
```

Starts:
- **PostgreSQL 16** on `localhost:5432` — `wal_level=logical`
- **MySQL 8.0** on `localhost:3308` — `binlog_format=ROW`
- **NATS 2.10** on `localhost:4222` (JetStream enabled)

### 2. Run the server

```bash
mvn spring-boot:run -pl excalibase-watcher-api
```

On startup it:
- Connects to Postgres WAL and MySQL binlog
- Auto-creates replication slot, publication, and DDL event triggers
- Creates a NATS JetStream stream (`CDC`)
- Publishes all CDC events to `cdc.{schema}.{table}`
- Exposes health at `http://localhost:8080/actuator/health`

### 3. Make some changes

```sql
-- Postgres (localhost:5432, user: postgres, pass: postgres, db: excalibase)
CREATE TABLE orders (
    id      SERIAL PRIMARY KEY,
    product VARCHAR(100),
    qty     INT,
    price   NUMERIC(10,2)
);
ALTER TABLE orders REPLICA IDENTITY FULL;

INSERT INTO orders (product, qty, price) VALUES ('keyboard', 2, 49.99);
UPDATE orders SET qty = 1 WHERE product = 'keyboard';
DELETE FROM orders WHERE product = 'keyboard';
TRUNCATE TABLE orders;
```

### 4. Watch the events

Server console output:
```
[CDC] INSERT public.orders data={"id":1, "product":"keyboard", "qty":2, "price":49.99}
[CDC] UPDATE public.orders data={"old":{"id":1, "product":"keyboard", "qty":2, "price":49.99}, "new":{"id":1, "product":"keyboard", "qty":1, "price":49.99}}
[CDC] DELETE public.orders data={"id":1, "product":"keyboard", "qty":1, "price":49.99}
[CDC] TRUNCATE public.orders
[CDC] DDL public.null data={"command_tag":"CREATE TABLE", "object_identity":"public.orders", ...}
```

Note: numeric values (`qty`, `price`) are JSON numbers, not strings. Booleans are JSON booleans.

---

## NATS subject format

| Event type | Subject | Example |
|-----------|---------|---------|
| DML (INSERT/UPDATE/DELETE) | `{prefix}.{schema}.{table}` | `cdc.public.orders` |
| TRUNCATE | `{prefix}.{schema}.{table}` | `cdc.public.orders` |
| DDL | `{prefix}.{schema}._ddl` | `cdc.public._ddl` |

Wildcard subscription: `cdc.>` receives all events.

### NATS message payload

```json
{
  "type": "INSERT",
  "schema": "public",
  "table": "orders",
  "data": "{\"id\":1, \"product\":\"keyboard\", \"qty\":2, \"price\":49.99}",
  "rawMessage": "INSERT",
  "lsn": "0/1989540",
  "timestamp": 1742056200000,
  "sourceTimestamp": 1742056199500
}
```

- `timestamp` — when the watcher processed the event (epoch millis)
- `sourceTimestamp` — when the database committed the change (epoch millis, 0 if unavailable)

---

## Architecture

```
excalibase-watcher-api  ← the CDC server (you deploy this)
  connects to: Postgres / MySQL
  publishes to: NATS JetStream

your-spring-app  ← your service (no excalibase dependency needed)
  subscribes to: NATS JetStream
  receives: CDC events as JSON
```

The watcher-api is a standalone server. Your services are pure NATS consumers — **no CDC library dependency, any language works**.

---

## Consuming events

### Spring Boot (Java)

No excalibase dependency needed — just `jnats`:

```xml
<dependency>
    <groupId>io.nats</groupId>
    <artifactId>jnats</artifactId>
    <version>2.17.6</version>
</dependency>
```

```java
@Service
public class OrderChangeListener {

    @Value("${app.nats.url:nats://localhost:4222}")
    private String natsUrl;

    @PostConstruct
    void subscribe() throws Exception {
        Connection nc = Nats.connect(natsUrl);
        JetStream js = nc.jetStream();

        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .durable("my-order-service")       // survives restarts
                .deliverPolicy(DeliverPolicy.New)  // only new events
                .build();
        PushSubscribeOptions opts = PushSubscribeOptions.builder()
                .stream("CDC")
                .configuration(cc)
                .build();

        // Listen to a specific table
        js.subscribe("cdc.public.orders", opts, msg -> {
            String json = new String(msg.getData());
            System.out.println("Order changed: " + json);
            msg.ack();
        });

        // Or listen to all changes: js.subscribe("cdc.>", opts, handler);
        // Or DDL only: js.subscribe("cdc.public._ddl", opts, handler);
    }
}
```

### Node.js

```js
const { connect, consumerOpts } = require('nats')

const nc = await connect({ servers: 'nats://localhost:4222' })
const js = nc.jetstream()

const opts = consumerOpts()
opts.durable('my-service')
opts.deliverAll()
opts.ackExplicit()

const sub = await js.subscribe('cdc.>', opts)
for await (const msg of sub) {
  const event = JSON.parse(msg.data)
  console.log(`[${event.type}] ${event.schema}.${event.table}`, event.data)
  msg.ack()
}
```

### Load balancing across pods

```js
opts.durable('inventory-service')
opts.queue('inventory-service')  // messages distributed across pods
```

Without `queue()` each pod gets every message (fan-out). With `queue()` messages are load-balanced.

---

## Configuration reference

### Postgres CDC

```properties
# Connection (falls back to spring.datasource.* if not set)
app.cdc.postgres.url=jdbc:postgresql://localhost:5432/mydb
app.cdc.postgres.username=postgres
app.cdc.postgres.password=postgres
app.cdc.postgres.enabled=true

# Replication
app.cdc.slot-name=cdc_slot
app.cdc.publication-name=cdc_publication
app.cdc.create-slot-if-not-exists=true
app.cdc.create-publication-if-not-exists=true

# Table filtering (empty = all tables)
app.cdc.postgres.tables=orders,users

# DDL capture via event triggers
app.cdc.postgres.capture-ddl=false

# Snapshot
app.cdc.postgres.snapshot-mode=NONE          # NONE | CHUNKED | BACKUP_FILE
app.cdc.postgres.snapshot-chunk-size=10000
```

### MySQL CDC

```properties
# Connection (falls back to spring.datasource.* if not set)
app.cdc.mysql.url=jdbc:mysql://localhost:3306/mydb
app.cdc.mysql.username=root
app.cdc.mysql.password=secret
app.cdc.mysql.enabled=true

# Table filtering (empty = all tables)
app.cdc.mysql.tables=orders,users

# Snapshot
app.cdc.mysql.snapshot-mode=NONE             # NONE | CHUNKED | BACKUP_FILE
app.cdc.mysql.snapshot-chunk-size=10000

# Offset persistence (resume from last position after restart)
app.cdc.mysql.offset-store.file=/var/lib/watcher/binlog.offset
```

### NATS JetStream

```properties
app.nats.url=nats://localhost:4222
app.nats.stream-name=CDC
app.nats.subject-prefix=cdc
app.nats.storage=memory                      # memory | file
app.nats.max-age-minutes=60
app.nats.enabled=true
```

For production: use `storage=file` and increase `max-age-minutes`.

### Actuator

```properties
server.port=8080
management.endpoints.web.exposure.include=health,metrics
management.endpoint.health.show-details=always
```

Endpoints:
- `GET /actuator/health` — UP when CDC listener is running
- `GET /actuator/metrics/cdc.events.total` — event count by type
- `GET /actuator/metrics/cdc.nats.published` — NATS publish count

---

## Database setup

### PostgreSQL

```sql
-- postgresql.conf
wal_level = logical
max_replication_slots = 5
max_wal_senders = 5
```

For UPDATE/DELETE with full row data:
```sql
ALTER TABLE orders REPLICA IDENTITY FULL;
```

| REPLICA IDENTITY | INSERT | UPDATE | DELETE |
|-----------------|--------|--------|--------|
| `DEFAULT` | full row | new only | PK only |
| `FULL` | full row | old + new | full row |

### MySQL

```ini
# my.cnf
log_bin          = ON
binlog_format    = ROW
binlog_row_image = FULL
server_id        = 1
```

User privileges:
```sql
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user'@'%';
```

---

## CDCEvent types

| Type | Description | Published to NATS? |
|------|-------------|-------------------|
| `INSERT` | Row inserted | Yes |
| `UPDATE` | Row updated (old + new) | Yes |
| `DELETE` | Row deleted | Yes |
| `DDL` | Schema change (CREATE/ALTER/DROP) | Yes (to `_ddl` subject) |
| `TRUNCATE` | Table truncated (Postgres only) | Yes |
| `BEGIN` | Transaction start | No |
| `COMMIT` | Transaction end (MySQL includes xid) | No |
| `HEARTBEAT` | Idle keepalive (Postgres, every 30s) | No |

---

## Build & test

```bash
mvn install    # builds all modules + runs all tests (requires Docker)
```

**69 tests** across all modules, all using Testcontainers:
- 10 core unit tests
- 18 Postgres integration tests (WAL, DDL, TRUNCATE, reconnect, type mapping, schema history)
- 18 MySQL integration tests (binlog, DDL, BEGIN/COMMIT, type mapping)
- 5 NATS integration tests
- 18 E2E tests (full server + DB + NATS pipeline)

---

## License

[Apache License 2.0](LICENSE)
