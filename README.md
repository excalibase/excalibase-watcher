# excalibase-watcher

A lightweight Java CDC (Change Data Capture) library that streams database changes to NATS JetStream. Any service — in any language — can subscribe to real-time INSERT, UPDATE, and DELETE events without touching the database.

```
[Postgres WAL / MySQL binlog]
          ↓
  excalibase-watcher-sample  (or your own Spring Boot app)
          ↓
    NATS JetStream  (subject: cdc.{schema}.{table})
          ↓
  inventory-svc  |  search-indexer  |  audit-logger  |  ...
```

---

## Modules

| Module | Description |
|---|---|
| `excalibase-watcher-core` | `CDCService` + `CDCEvent` — pure Java, no Spring required for usage |
| `excalibase-watcher-postgres` | PostgreSQL WAL listener (logical replication, pgoutput protocol) |
| `excalibase-watcher-mysql` | MySQL binlog listener (real-time replication stream, same mechanism as Debezium) |
| `excalibase-watcher-nats` | NATS JetStream publisher — bridges CDCService to NATS |
| `excalibase-watcher-sample` | Runnable Spring Boot demo wired to docker-compose |

---

## Quick start

### 1. Start infrastructure

```bash
docker compose up -d
```

Starts:
- **PostgreSQL 16** on `localhost:5432` — `wal_level=logical` pre-configured
- **MySQL 8.0** on `localhost:3308`
- **NATS 2.10** on `localhost:4222` (JetStream enabled), monitoring on `localhost:8222`

### 2. Run the sample app

```bash
mvn spring-boot:run -pl excalibase-watcher-sample
```

On startup it:
- Connects to Postgres and auto-creates a replication slot + publication
- Connects to NATS and creates a `CDC` JetStream stream
- Logs every CDC event to console

### 3. Make some changes

```sql
-- connect to postgres: localhost:5432 / user: postgres / pass: postgres / db: excalibase

CREATE TABLE orders (
    id      SERIAL PRIMARY KEY,
    product VARCHAR(100),
    qty     INT,
    status  VARCHAR(20) DEFAULT 'pending'
);

INSERT INTO orders (product, qty) VALUES ('keyboard', 2);
INSERT INTO orders (product, qty) VALUES ('mouse', 5);
UPDATE orders SET status = 'shipped', qty = 1 WHERE product = 'keyboard';
DELETE FROM orders WHERE product = 'mouse';
```

### 4. Watch the events

Sample app console:
```
[CDC] type=INSERT schema=public table=orders lsn=0/1989540 data={"col_0":"1","col_1":"keyboard","col_2":"2","col_3":"pending"}
[CDC] type=INSERT schema=public table=orders lsn=0/1989668 data={"col_0":"2","col_1":"mouse","col_2":"5","col_3":"pending"}
[CDC] type=UPDATE schema=public table=orders lsn=0/1989760 data={"new":{"col_0":"1","col_1":"keyboard","col_2":"1","col_3":"shipped"}}
[CDC] type=DELETE schema=public table=orders lsn=0/19897F0 data={"col_0":"2","col_1":null,"col_2":null,"col_3":null}
```

> Add `ALTER TABLE orders REPLICA IDENTITY FULL;` to get full old row data in UPDATE/DELETE (see [PostgreSQL setup](#postgresql-setup-requirements)).

---

## NATS message format

Every event is published as JSON to subject `cdc.{schema}.{table}`:

```json
{
  "type": "INSERT",
  "schema": "public",
  "table": "orders",
  "data": "{\"col_0\":\"1\", \"col_1\":\"keyboard\", \"col_2\":\"2\", \"col_3\":\"pending\"}",
  "rawMessage": "INSERT",
  "lsn": "0/1989540",
  "timestamp": 1742056200000
}
```

> **Postgres vs MySQL data format difference:**
> - **MySQL** uses real column names: `{"id": 1, "name": "laptop", "price": 999.99}`
> - **Postgres** uses positional names: `{"col_0": "1", "col_1": "laptop", "col_2": "999.99"}` — the pgoutput WAL format does not include column names inline. Column name resolution is a planned improvement.

---

## Consuming events

Any NATS client in any language can subscribe. Multiple independent consumers each get every message.

### Node.js

```js
const { connect, consumerOpts } = require('nats')

const nc = await connect({ servers: 'nats://localhost:4222' })
const js = nc.jetstream()

const opts = consumerOpts()
opts.durable('my-service')   // survives restarts
opts.deliverAll()
opts.ackExplicit()

const sub = await js.subscribe('cdc.>', opts)
for await (const msg of sub) {
  const event = JSON.parse(msg.data)
  console.log(`[${event.type}] ${event.schema}.${event.table}`, event.data)
  msg.ack()
}
```

### Scaling (multiple pods)

```js
// Same durable name + queue group = load-balanced across pods, each message processed once
opts.durable('inventory-service')
opts.queue('inventory-service')   // <-- add this when scaling
```

Without `queue()` each pod gets every message (fan-out). With `queue()` messages are distributed across pods (load balance).

---

## PostgreSQL setup requirements

PostgreSQL must have `wal_level = logical`. This is pre-configured in the provided `docker-compose.yml`. For an existing Postgres:

```sql
-- Check current level
SHOW wal_level;

-- In postgresql.conf:
wal_level = logical
max_replication_slots = 5
max_wal_senders = 5
-- Then restart Postgres
```

The library auto-creates the publication and replication slot on first run (`CREATE PUBLICATION ... FOR ALL TABLES`). Configurable:

```properties
app.cdc.slot-name=my_slot
app.cdc.publication-name=my_publication
app.cdc.create-slot-if-not-exists=true
app.cdc.create-publication-if-not-exists=true
```

### REPLICA IDENTITY — controls what data appears in UPDATE/DELETE events

This is per-table and must be set manually:

```sql
-- DEFAULT (Postgres default): UPDATE/DELETE only include primary key columns
-- Good enough if you only need to know *which row* changed
ALTER TABLE orders REPLICA IDENTITY DEFAULT;

-- FULL: UPDATE/DELETE include all column values (old row for DELETE, old+new for UPDATE)
-- Required if consumers need the full deleted row or the before-image of an UPDATE
ALTER TABLE orders REPLICA IDENTITY FULL;
```

| Setting | INSERT | UPDATE | DELETE |
|---|---|---|---|
| `DEFAULT` | full row | new values only (no old values) | primary key only |
| `FULL` | full row | old + new values | full deleted row |

Example — `REPLICA IDENTITY DEFAULT` (what you get without any setup):
```json
// DELETE only has primary key, other columns are null
{"type":"DELETE","table":"orders","data":{"col_0":"2","col_1":null,"col_2":null,"col_3":null}}
```

Example — `REPLICA IDENTITY FULL`:
```json
// DELETE has the full row
{"type":"DELETE","table":"orders","data":{"col_0":"2","col_1":"mouse","col_2":"5","col_3":"pending"}}
// UPDATE has both old and new
{"type":"UPDATE","table":"orders","data":{"old":{"col_0":"1","col_1":"keyboard","col_2":"2","col_3":"pending"},"new":{"col_0":"1","col_1":"keyboard","col_2":"1","col_3":"shipped"}}}
```

**Recommendation:** use `REPLICA IDENTITY FULL` for any table where consumers need the full row on DELETE, or need to diff old vs new on UPDATE.

**What gets captured:** INSERT, UPDATE, DELETE on all tables in the publication. BEGIN/COMMIT are filtered out before publishing to NATS.

---

## Postgres vs MySQL capability comparison

| Capability | Postgres (WAL) | MySQL (binlog) |
|---|---|---|
| INSERT detection | ✅ full row | ✅ full row |
| UPDATE detection | ✅ new only by default; old+new with `REPLICA IDENTITY FULL` | ✅ full old + new row |
| DELETE detection | ✅ PK only by default; full row with `REPLICA IDENTITY FULL` | ✅ full deleted row |
| Column names in data | ⚠ positional (`col_0`, `col_1`...) | ✅ real column names |
| Latency | Real-time (WAL stream) | Real-time (binlog stream) |
| DB setup required | `wal_level=logical` in server config | `log_bin=ROW`, `binlog_row_image=FULL` in server config |
| Special table schema | None | None |

---

## MySQL setup requirements

MySQL CDC uses **binlog replication** — the same mechanism as Debezium. No special table schema is required.

### Server configuration

```ini
# my.cnf
log_bin          = ON
binlog_format    = ROW
binlog_row_image = FULL
server_id        = 1
```

This is pre-configured in the provided `docker-compose.yml`. For an existing MySQL instance, add these to `my.cnf` and restart.

### User privileges

The connecting user needs replication privileges:

```sql
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user'@'%';
FLUSH PRIVILEGES;
```

### Example

```sql
CREATE TABLE products (
    id    INT AUTO_INCREMENT PRIMARY KEY,
    name  VARCHAR(100),
    price DECIMAL(10,2),
    stock INT DEFAULT 0
);

INSERT INTO products (name, price, stock) VALUES ('laptop', 999.99, 10);
INSERT INTO products (name, price, stock) VALUES ('phone', 499.50, 25);
UPDATE products SET stock = 8, price = 949.99 WHERE name = 'laptop';
DELETE FROM products WHERE name = 'phone';
```

Events emitted (MySQL uses **real column names**, unlike Postgres):
```
[CDC] type=INSERT schema=mydb table=products data={"id": 1, "name": "laptop", "price": 999.99, "stock": 10}
[CDC] type=INSERT schema=mydb table=products data={"id": 2, "name": "phone", "price": 499.50, "stock": 25}
[CDC] type=UPDATE schema=mydb table=products data={"old": {"id": 1, "name": "laptop", "price": 999.99, "stock": 10}, "new": {"id": 1, "name": "laptop", "price": 949.99, "stock": 8}}
[CDC] type=DELETE schema=mydb table=products data={"id": 2, "name": "phone", "price": 499.50, "stock": 25}
```

Configure which tables to watch:

```properties
app.cdc.mysql.tables=orders,users    # empty = watch all tables
```

---

## Configuration reference

### Postgres

```properties
spring.datasource.url=jdbc:postgresql://localhost:5432/mydb
spring.datasource.username=postgres
spring.datasource.password=postgres

app.cdc.enabled=true
app.cdc.slot-name=cdc_slot
app.cdc.publication-name=cdc_publication
app.cdc.create-slot-if-not-exists=true
app.cdc.create-publication-if-not-exists=true
```

### MySQL

```properties
spring.datasource.url=jdbc:mysql://localhost:3306/mydb
spring.datasource.username=user
spring.datasource.password=secret

app.cdc.enabled=true
app.cdc.mysql.tables=          # empty = watch all tables; comma-separated to filter
```

### NATS

```properties
app.nats.url=nats://localhost:4222
app.nats.stream-name=CDC
app.nats.subject-prefix=cdc
app.nats.storage=memory        # memory | file
app.nats.max-age-minutes=60
app.nats.enabled=true
```

> For production use `storage=file` and increase `max-age-minutes` so consumers that are temporarily offline don't miss events.

---

## Build & test

```bash
mvn install        # builds all modules + runs all tests (requires Docker)
```

Tests: 10 core + 8 postgres + 11 mysql + 5 nats = **34 tests**, all using Testcontainers (no manual setup needed).
