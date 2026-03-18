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
    void shouldReportRunningStatus() {
        assertThat(cdcService.isRunning()).isTrue();
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
}
