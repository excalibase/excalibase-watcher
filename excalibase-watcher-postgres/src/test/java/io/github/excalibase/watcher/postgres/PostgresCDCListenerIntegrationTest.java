/*
 * Copyright 2025 Excalibase Team and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.excalibase.watcher.postgres;

import io.github.excalibase.watcher.CDCEvent;
import io.github.excalibase.watcher.CDCService;
import io.github.excalibase.watcher.postgres.snapshot.PostgresDumpSnapshot;
import io.github.excalibase.watcher.schema.FileSchemaHistoryStore;
import io.github.excalibase.watcher.schema.SchemaHistoryEntry;
import io.github.excalibase.watcher.snapshot.SnapshotMode;
import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.Disposable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link PostgresCDCListener} using a real PostgreSQL container.
 * <p>
 * These tests verify the full CDC pipeline: PostgreSQL WAL → pgoutput protocol
 * → {@link CDCEvent} routing via {@link CDCService}.
 * Each test gets a fresh replication slot to avoid stale WAL events leaking between tests.
 * </p>
 *
 * <p>Requires Docker to be running.</p>
 */
@Testcontainers
class PostgresCDCListenerIntegrationTest {

    private static final String SLOT_NAME = "test_cdc_slot";
    private static final String PUBLICATION_NAME = "test_cdc_publication";
    private static final String TEST_TABLE = "cdc_test_users";

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16")
            .withCommand("postgres",
                    "-c", "wal_level=logical",
                    "-c", "max_replication_slots=5",
                    "-c", "max_wal_senders=5")
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass");

    private CDCService cdcService;
    private PostgresCDCListener cdcListener;
    private Connection regularConnection;

    @BeforeEach
    void setUp() throws Exception {
        regularConnection = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());

        try (Statement stmt = regularConnection.createStatement()) {
            // Drop slot first so each test starts with a clean WAL position
            dropSlotIfExists(stmt);

            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS %s (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(100) NOT NULL,
                        email VARCHAR(200),
                        active BOOLEAN DEFAULT true
                    )
                    """.formatted(TEST_TABLE));
        }

        cdcService = new CDCService();

        cdcListener = new PostgresCDCListener.Builder()
                .jdbcUrl(postgres.getJdbcUrl())
                .credentials(postgres.getUsername(), postgres.getPassword())
                .slotName(SLOT_NAME)
                .publicationName(PUBLICATION_NAME)
                .createSlotIfNotExists(true)
                .createPublicationIfNotExists(true)
                .eventHandler(cdcService::handleCDCEvent)
                .build();

        cdcListener.start();
        cdcService.markRunning();

        // Allow the replication stream to initialize
        Thread.sleep(500);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (cdcListener != null) {
            cdcListener.stop();
        }
        cdcService.shutdown();

        if (regularConnection != null && !regularConnection.isClosed()) {
            try (Statement stmt = regularConnection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + TEST_TABLE);
                dropSlotIfExists(stmt);
            }
            regularConnection.close();
        }
    }

    @Test
    void shouldCaptureInsertEvent() throws Exception {
        List<CDCEvent> events = new ArrayList<>();
        Disposable sub = cdcService.getTableEventStream(TEST_TABLE).subscribe(events::add);

        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name, email) VALUES ('Alice', 'alice@example.com')");
        }

        waitForEvents(events, 1);

        assertThat(events).hasSize(1);
        CDCEvent event = events.get(0);
        assertThat(event.getType()).isEqualTo(CDCEvent.Type.INSERT);
        assertThat(event.getTable()).isEqualTo(TEST_TABLE);
        assertThat(event.getSchema()).isEqualTo("public");
        assertThat(event.getData()).startsWith("{");
        assertThat(event.getLsn()).isNotNull();
        assertThat(event.getTimestamp()).isPositive();

        sub.dispose();
    }

    @Test
    void shouldCaptureUpdateEvent() throws Exception {
        List<CDCEvent> events = new ArrayList<>();
        Disposable sub = cdcService.getTableEventStream(TEST_TABLE).subscribe(events::add);

        // Insert the row we'll update
        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name, email) VALUES ('Bob', 'bob@example.com')");
        }
        waitForEvents(events, 1);
        events.clear(); // discard the INSERT, only care about UPDATE

        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("UPDATE " + TEST_TABLE + " SET name = 'Bobby' WHERE name = 'Bob'");
        }

        waitForEvents(events, 1);

        assertThat(events).hasSize(1);
        CDCEvent event = events.get(0);
        assertThat(event.getType()).isEqualTo(CDCEvent.Type.UPDATE);
        assertThat(event.getTable()).isEqualTo(TEST_TABLE);
        // UPDATE data wraps old/new: {"old":{...}, "new":{...}}
        assertThat(event.getData()).contains("\"new\":");

        sub.dispose();
    }

    @Test
    void shouldCaptureDeleteEvent() throws Exception {
        List<CDCEvent> events = new ArrayList<>();
        Disposable sub = cdcService.getTableEventStream(TEST_TABLE).subscribe(events::add);

        // Insert the row we'll delete
        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name, email) VALUES ('Charlie', 'charlie@example.com')");
        }
        waitForEvents(events, 1);
        events.clear(); // discard the INSERT, only care about DELETE

        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("DELETE FROM " + TEST_TABLE + " WHERE name = 'Charlie'");
        }

        waitForEvents(events, 1);

        assertThat(events).hasSize(1);
        CDCEvent event = events.get(0);
        assertThat(event.getType()).isEqualTo(CDCEvent.Type.DELETE);
        assertThat(event.getTable()).isEqualTo(TEST_TABLE);
        assertThat(event.getData()).isNotNull();

        sub.dispose();
    }

    @Test
    void shouldCaptureMultipleEventsInOrder() throws Exception {
        List<CDCEvent> events = new ArrayList<>();
        Disposable sub = cdcService.getTableEventStream(TEST_TABLE).subscribe(events::add);

        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name) VALUES ('User1')");
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name) VALUES ('User2')");
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name) VALUES ('User3')");
        }

        waitForEvents(events, 3);

        assertThat(events).hasSize(3);
        assertThat(events).allMatch(e -> e.getType() == CDCEvent.Type.INSERT);
        assertThat(events).allMatch(e -> TEST_TABLE.equals(e.getTable()));

        sub.dispose();
    }

    @Test
    void shouldSupportMultipleSubscribersForSameTable() throws Exception {
        List<CDCEvent> subscriber1Events = new ArrayList<>();
        List<CDCEvent> subscriber2Events = new ArrayList<>();

        Disposable sub1 = cdcService.getTableEventStream(TEST_TABLE).subscribe(subscriber1Events::add);
        Disposable sub2 = cdcService.getTableEventStream(TEST_TABLE).subscribe(subscriber2Events::add);

        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name) VALUES ('SharedUser')");
        }

        waitForEvents(subscriber1Events, 1);
        waitForEvents(subscriber2Events, 1);

        assertThat(subscriber1Events).hasSize(1);
        assertThat(subscriber2Events).hasSize(1);
        assertThat(subscriber1Events.get(0).getType()).isEqualTo(CDCEvent.Type.INSERT);
        assertThat(subscriber2Events.get(0).getType()).isEqualTo(CDCEvent.Type.INSERT);

        sub1.dispose();
        sub2.dispose();
    }

    @Test
    void shouldNotRouteEventsToWrongTableStream() throws Exception {
        List<CDCEvent> targetEvents = new ArrayList<>();
        List<CDCEvent> otherEvents = new ArrayList<>();

        Disposable sub1 = cdcService.getTableEventStream(TEST_TABLE).subscribe(targetEvents::add);
        Disposable sub2 = cdcService.getTableEventStream("some_other_table").subscribe(otherEvents::add);

        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name) VALUES ('RoutingTest')");
        }

        waitForEvents(targetEvents, 1);

        assertThat(targetEvents).hasSize(1);
        assertThat(otherEvents).isEmpty();

        sub1.dispose();
        sub2.dispose();
    }

    @Test
    void shouldCaptureRowsFromPgDumpFile() throws Exception {
        // pg_dump COPY format — user provides LSN separately
        Path dumpFile = Files.createTempFile("pg-dump", ".sql");
        try (BufferedWriter w = Files.newBufferedWriter(dumpFile)) {
            w.write("--\n-- PostgreSQL dump\n--\n\n");
            w.write("COPY public." + TEST_TABLE + " (id, name, email, active) FROM stdin;\n");
            w.write("1\tDumpPgUser1\tu1@pg.com\tt\n");
            w.write("2\tDumpPgUser2\tu2@pg.com\tf\n");
            w.write("\\.\n\n");
        }

        List<CDCEvent> events = new ArrayList<>();
        PostgresDumpSnapshot.run(dumpFile, "0/1000000", events::add);

        assertThat(events).hasSize(2);
        assertThat(events).allMatch(e -> e.getType() == CDCEvent.Type.INSERT);
        assertThat(events.get(0).getData()).contains("DumpPgUser1");
        assertThat(events.get(1).getData()).contains("DumpPgUser2");
        assertThat(events.get(0).getTable()).isEqualTo(TEST_TABLE);
        assertThat(events.get(0).getSchema()).isEqualTo("public");
        assertThat(events.get(0).getLsn()).isEqualTo("0/1000000");

        Files.deleteIfExists(dumpFile);
    }

    @Test
    void shouldCapturePreExistingRowsWithChunkedSnapshot() throws Exception {
        // Insert rows BEFORE the snapshot listener starts
        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name, email) VALUES ('PgPre1', 'p1@x.com')");
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name, email) VALUES ('PgPre2', 'p2@x.com')");
        }

        // Use a unique slot per test to avoid conflicts
        String snapshotSlot = "snapshot_test_slot";
        try (Statement stmt = regularConnection.createStatement()) {
            try { stmt.execute("SELECT pg_drop_replication_slot('" + snapshotSlot + "')"); } catch (Exception ignored) {}
        }

        CDCService snapshotCdc = new CDCService();
        List<CDCEvent> events = new ArrayList<>();
        snapshotCdc.getTableEventStream(TEST_TABLE).subscribe(events::add);

        PostgresCDCListener snapshotListener = new PostgresCDCListener.Builder()
                .jdbcUrl(postgres.getJdbcUrl())
                .credentials(postgres.getUsername(), postgres.getPassword())
                .slotName(snapshotSlot)
                .publicationName(PUBLICATION_NAME)
                .createSlotIfNotExists(true)
                .createPublicationIfNotExists(false) // publication already exists from setUp
                .snapshotMode(SnapshotMode.CHUNKED)
                .eventHandler(snapshotCdc::handleCDCEvent)
                .build();

        snapshotListener.start();
        snapshotCdc.markRunning();
        Thread.sleep(1000);

        assertThat(events.stream().anyMatch(e -> e.getData().contains("PgPre1"))).isTrue();
        assertThat(events.stream().anyMatch(e -> e.getData().contains("PgPre2"))).isTrue();
        assertThat(events).allMatch(e -> e.getType() == CDCEvent.Type.INSERT);

        // New inserts after snapshot still captured
        events.clear();
        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name) VALUES ('PgPostSnapshot')");
        }
        waitForEvents(events, 1);
        assertThat(events).anyMatch(e -> e.getData().contains("PgPostSnapshot"));

        snapshotListener.stop();
        snapshotCdc.shutdown();
        try (Statement stmt = regularConnection.createStatement()) {
            try { stmt.execute("SELECT pg_drop_replication_slot('" + snapshotSlot + "')"); } catch (Exception ignored) {}
        }
    }

    @Test
    void shouldIncludeRealColumnNamesInData() throws Exception {
        List<CDCEvent> events = new ArrayList<>();
        Disposable sub = cdcService.getTableEventStream(TEST_TABLE).subscribe(events::add);

        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name, email) VALUES ('ColTest', 'col@test.com')");
        }

        waitForEvents(events, 1);

        CDCEvent event = events.get(0);
        assertThat(event.getData()).contains("\"name\"");
        assertThat(event.getData()).contains("\"email\"");
        assertThat(event.getData()).contains("\"id\"");
        assertThat(event.getData()).contains("ColTest");
        assertThat(event.getData()).doesNotContain("col_0");
        assertThat(event.getData()).doesNotContain("col_1");

        sub.dispose();
    }

    @Test
    void shouldReflectActiveSubscriptionCount() {
        assertThat(cdcService.getActiveSubscriptionCount()).isZero();

        Disposable sub1 = cdcService.getTableEventStream(TEST_TABLE).subscribe();
        Disposable sub2 = cdcService.getTableEventStream(TEST_TABLE).subscribe();

        assertThat(cdcService.getActiveSubscriptionCount()).isEqualTo(2);
        assertThat(cdcService.getActiveSubscriptionCount(TEST_TABLE)).isEqualTo(2);

        sub1.dispose();
        sub2.dispose();
    }

    @Test
    void shouldFilterToSpecifiedTablesOnly() throws Exception {
        // Create a second table
        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS other_table (id SERIAL PRIMARY KEY, val TEXT)");
        }

        String filteredSlot = "filtered_slot";
        try (Statement stmt = regularConnection.createStatement()) {
            try { stmt.execute("SELECT pg_drop_replication_slot('" + filteredSlot + "')"); } catch (Exception ignored) {}
        }

        CDCService filteredCdc = new CDCService();
        List<CDCEvent> targetEvents = new ArrayList<>();
        List<CDCEvent> otherEvents = new ArrayList<>();
        filteredCdc.getTableEventStream(TEST_TABLE).subscribe(targetEvents::add);
        filteredCdc.getTableEventStream("other_table").subscribe(otherEvents::add);

        PostgresCDCListener filteredListener = new PostgresCDCListener.Builder()
                .jdbcUrl(postgres.getJdbcUrl())
                .credentials(postgres.getUsername(), postgres.getPassword())
                .slotName(filteredSlot)
                .publicationName(PUBLICATION_NAME)
                .createSlotIfNotExists(true)
                .createPublicationIfNotExists(false)
                .tables(List.of(TEST_TABLE))      // only watch TEST_TABLE
                .eventHandler(filteredCdc::handleCDCEvent)
                .build();

        filteredListener.start();
        filteredCdc.markRunning();
        Thread.sleep(500);

        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name) VALUES ('FilteredTarget')");
            stmt.execute("INSERT INTO other_table (val) VALUES ('ShouldBeIgnored')");
        }

        waitForEvents(targetEvents, 1);
        Thread.sleep(300); // give time for other_table event to arrive (if it would)

        assertThat(targetEvents).hasSize(1);
        assertThat(targetEvents.get(0).getData()).contains("FilteredTarget");
        assertThat(otherEvents).isEmpty(); // other_table was filtered out

        filteredListener.stop();
        filteredCdc.shutdown();
        try (Statement stmt = regularConnection.createStatement()) {
            try { stmt.execute("SELECT pg_drop_replication_slot('" + filteredSlot + "')"); } catch (Exception ignored) {}
            stmt.execute("DROP TABLE IF EXISTS other_table");
        }
    }

    @Test
    void shouldRecordSchemaHistoryOnRelationMessages() throws Exception {
        Path histDir = Files.createTempDirectory("schema-history");
        FileSchemaHistoryStore store = new FileSchemaHistoryStore(histDir);

        String histSlot = "hist_test_slot";
        try (Statement stmt = regularConnection.createStatement()) {
            try { stmt.execute("SELECT pg_drop_replication_slot('" + histSlot + "')"); } catch (Exception ignored) {}
        }

        CDCService histCdc = new CDCService();

        PostgresCDCListener histListener = new PostgresCDCListener.Builder()
                .jdbcUrl(postgres.getJdbcUrl())
                .credentials(postgres.getUsername(), postgres.getPassword())
                .slotName(histSlot)
                .publicationName(PUBLICATION_NAME)
                .createSlotIfNotExists(true)
                .createPublicationIfNotExists(false)
                .schemaHistoryStore(store)
                .eventHandler(histCdc::handleCDCEvent)
                .build();

        histListener.start();
        histCdc.markRunning();
        Thread.sleep(500);

        // Trigger a DML so RELATION message is sent for the test table
        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name) VALUES ('SchemaHistTest')");
        }
        Thread.sleep(1000);

        // Verify schema was recorded
        var latest = store.getLatestSchema("public", TEST_TABLE);
        assertThat(latest).isPresent();
        SchemaHistoryEntry entry = latest.get();
        assertThat(entry.schema()).isEqualTo("public");
        assertThat(entry.table()).isEqualTo(TEST_TABLE);
        assertThat(entry.columns()).extracting("name").contains("id", "name", "email", "active");

        histListener.stop();
        histCdc.shutdown();
        try (Statement stmt = regularConnection.createStatement()) {
            try { stmt.execute("SELECT pg_drop_replication_slot('" + histSlot + "')"); } catch (Exception ignored) {}
        }
    }

    @Test
    void shouldMapNumericAndBooleanTypesCorrectly() throws Exception {
        // Create a table with typed columns
        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS type_test_pg (
                        id SERIAL PRIMARY KEY,
                        price NUMERIC(10,2),
                        qty INT,
                        active BOOLEAN DEFAULT true,
                        name TEXT
                    )""");
        }

        String typeSlot = "type_test_slot";
        try (Statement stmt = regularConnection.createStatement()) {
            try { stmt.execute("SELECT pg_drop_replication_slot('" + typeSlot + "')"); } catch (Exception ignored) {}
        }

        CDCService typeCdc = new CDCService();
        List<CDCEvent> events = new ArrayList<>();
        typeCdc.getTableEventStream("type_test_pg").subscribe(events::add);

        PostgresCDCListener typeListener = new PostgresCDCListener.Builder()
                .jdbcUrl(postgres.getJdbcUrl())
                .credentials(postgres.getUsername(), postgres.getPassword())
                .slotName(typeSlot)
                .publicationName(PUBLICATION_NAME)
                .createSlotIfNotExists(true)
                .createPublicationIfNotExists(false)
                .eventHandler(typeCdc::handleCDCEvent)
                .build();
        typeListener.start();
        typeCdc.markRunning();
        Thread.sleep(500);

        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("INSERT INTO type_test_pg (price, qty, active, name) VALUES (19.99, 5, true, 'Widget')");
        }

        waitForEvents(events, 1);
        assertThat(events).hasSize(1);
        String data = events.get(0).getData();

        // Numeric values should be JSON numbers (no quotes)
        assertThat(data).contains("\"price\":19.99");
        assertThat(data).contains("\"qty\":5");

        // Boolean should be JSON boolean
        assertThat(data).contains("\"active\":true");

        // Text should remain a JSON string
        assertThat(data).contains("\"name\":\"Widget\"");

        typeListener.stop();
        typeCdc.shutdown();
        try (Statement stmt = regularConnection.createStatement()) {
            try { stmt.execute("SELECT pg_drop_replication_slot('" + typeSlot + "')"); } catch (Exception ignored) {}
            stmt.execute("DROP TABLE IF EXISTS type_test_pg");
        }
    }

    @Test
    void shouldCaptureTruncateEvent() throws Exception {
        // Insert a row first so there's data to truncate
        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name) VALUES ('WillBeTruncated')");
        }
        Thread.sleep(300);

        List<CDCEvent> events = new CopyOnWriteArrayList<>();
        Disposable sub = cdcService.getAllEventsFlux()
                .filter(e -> e.getType() == CDCEvent.Type.TRUNCATE)
                .subscribe(events::add);

        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("TRUNCATE TABLE " + TEST_TABLE);
        }

        waitForEvents(events, 1);

        assertThat(events).hasSize(1);
        CDCEvent event = events.get(0);
        assertThat(event.getType()).isEqualTo(CDCEvent.Type.TRUNCATE);
        assertThat(event.getTable()).isEqualTo(TEST_TABLE);
        assertThat(event.getSchema()).isEqualTo("public");

        sub.dispose();
    }

    @Test
    void shouldCaptureDdlEventsViaEventTrigger() throws Exception {
        // Create a DDL-capturing listener with a separate slot
        String ddlSlot = "ddl_test_slot";
        try (Statement stmt = regularConnection.createStatement()) {
            try { stmt.execute("SELECT pg_drop_replication_slot('" + ddlSlot + "')"); } catch (Exception ignored) {}
        }

        CDCService ddlCdc = new CDCService();
        List<CDCEvent> ddlEvents = new CopyOnWriteArrayList<>();
        ddlCdc.getAllEventsFlux()
                .filter(e -> e.getType() == CDCEvent.Type.DDL)
                .subscribe(ddlEvents::add);

        PostgresCDCListener ddlListener = new PostgresCDCListener.Builder()
                .jdbcUrl(postgres.getJdbcUrl())
                .credentials(postgres.getUsername(), postgres.getPassword())
                .slotName(ddlSlot)
                .publicationName(PUBLICATION_NAME)
                .createSlotIfNotExists(true)
                .createPublicationIfNotExists(false)
                .captureDdl(true)
                .eventHandler(ddlCdc::handleCDCEvent)
                .build();

        ddlListener.start();
        ddlCdc.markRunning();
        Thread.sleep(500);

        // Execute DDL statements
        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("CREATE TABLE ddl_capture_test (id INT PRIMARY KEY, val TEXT)");
            stmt.execute("ALTER TABLE ddl_capture_test ADD COLUMN extra INT");
            stmt.execute("DROP TABLE ddl_capture_test");
        }

        // Wait for DDL events
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
        while (ddlEvents.size() < 3 && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }

        assertThat(ddlEvents).hasSizeGreaterThanOrEqualTo(3);
        assertThat(ddlEvents).allMatch(e -> e.getType() == CDCEvent.Type.DDL);
        assertThat(ddlEvents).anyMatch(e -> e.getData().contains("CREATE TABLE"));
        assertThat(ddlEvents).anyMatch(e -> e.getData().contains("ALTER TABLE"));
        assertThat(ddlEvents).anyMatch(e -> e.getData().contains("DROP TABLE"));
        assertThat(ddlEvents).allMatch(e -> "public".equals(e.getSchema()));

        ddlListener.stop();
        ddlCdc.shutdown();
        try (Statement stmt = regularConnection.createStatement()) {
            try { stmt.execute("SELECT pg_drop_replication_slot('" + ddlSlot + "')"); } catch (Exception ignored) {}
        }
    }

    @Test
    void shouldReportRunningStatus() {
        assertThat(cdcService.isRunning()).isTrue();
    }

    @Test
    void parseSchemaShouldExtractCurrentSchemaFromJdbcUrl() {
        // Default — no currentSchema param → "public"
        assertThat(PostgresCDCListener.parseSchema("jdbc:postgresql://host:5432/mydb"))
                .isEqualTo("public");

        // Explicit currentSchema query parameter
        assertThat(PostgresCDCListener.parseSchema("jdbc:postgresql://host:5432/mydb?currentSchema=custom"))
                .isEqualTo("custom");

        // currentSchema with other params
        assertThat(PostgresCDCListener.parseSchema("jdbc:postgresql://host:5432/mydb?loggerLevel=OFF&currentSchema=sales&ssl=true"))
                .isEqualTo("sales");

        // No query string at all
        assertThat(PostgresCDCListener.parseSchema("jdbc:postgresql://localhost:5432/testdb"))
                .isEqualTo("public");
    }

    @Test
    void shouldReconnectAfterConnectionKilledAndContinueReceivingEvents() throws Exception {
        // Use thread-safe list since events arrive from listener thread
        List<CDCEvent> events = new CopyOnWriteArrayList<>();
        Disposable sub = cdcService.getAllEventsFlux()
                .filter(e -> e.getType() == CDCEvent.Type.INSERT)
                .subscribe(events::add);

        // 1. Verify CDC is working before kill
        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name) VALUES ('BeforeKill')");
        }
        waitForEvents(events, 1);
        assertThat(events).anyMatch(e -> e.getData().contains("BeforeKill"));

        // 2. Kill the replication connection via pg_terminate_backend
        var result = postgres.execInContainer("psql", "-U", postgres.getUsername(), "-d", postgres.getDatabaseName(),
                "-c", "SELECT pg_terminate_backend(pid) FROM pg_stat_activity " +
                      "WHERE backend_type = 'walsender' AND pid != pg_backend_pid()");
        assertThat(result.getStdout()).contains("t"); // 't' = true, connection killed

        // 3. Wait for reconnect
        Thread.sleep(5000);

        // 4. Insert after reconnect and verify it arrives
        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name) VALUES ('AfterReconnect')");
        }

        // Wait until we see the AfterReconnect event specifically
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(15);
        while (events.stream().noneMatch(e -> e.getData().contains("AfterReconnect"))
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
        }
        assertThat(events).anyMatch(e -> e.getData().contains("AfterReconnect"));

        // 5. Verify only one listener thread is alive (no duplicate threads from reconnect)
        long listenerThreadCount = Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.getName().equals("postgres-cdc-listener"))
                .filter(Thread::isAlive)
                .count();
        assertThat(listenerThreadCount)
                .as("Expected exactly 1 postgres-cdc-listener thread, but found %d (reconnect creates duplicates)", listenerThreadCount)
                .isEqualTo(1);

        sub.dispose();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void dropSlotIfExists(Statement stmt) {
        try {
            stmt.execute("SELECT pg_drop_replication_slot('" + SLOT_NAME + "')");
        } catch (Exception ignored) {
            // slot doesn't exist — that's fine
        }
    }

    /**
     * Poll up to 5 seconds for the expected number of events to arrive.
     */
    private void waitForEvents(List<CDCEvent> events, int expectedCount) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
        while (events.size() < expectedCount && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
    }

    /**
     * Poll up to 15 seconds — used for reconnect scenarios where backoff adds delay.
     */
    private void waitForEventsLong(List<CDCEvent> events, int expectedCount) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(15);
        while (events.size() < expectedCount && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
    }
}
