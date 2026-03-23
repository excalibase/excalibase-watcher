package io.github.excalibase.watcher.sample;

import io.github.excalibase.watcher.mysql.snapshot.BinlogPosition;
import io.github.excalibase.watcher.mysql.snapshot.FileBinlogOffsetStore;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * E2E integration test for the MySQL CDC pipeline.
 *
 * <p><b>Server</b> ({@code @SpringBootTest} = the full Spring application):</p>
 * <ul>
 *   <li>{@code MysqlBinlogStartup} — watches MySQL binlog replication</li>
 *   <li>{@code NatsEventPublisher} — forwards every CDCEvent to NATS JetStream</li>
 * </ul>
 *
 * <p><b>External clients</b> (test methods):</p>
 * <ul>
 *   <li>JDBC connection → writes to MySQL (INSERT / UPDATE / DELETE)</li>
 *   <li>NATS client → subscribes and asserts events arrive with correct data</li>
 * </ul>
 *
 * <p>Postgres listener is disabled ({@code app.cdc.postgres.enabled=false}).</p>
 */
@SpringBootTest
@Testcontainers
@DirtiesContext
class MysqlE2EIT {

    static final String STREAM    = "CDC_MYSQL_E2E";
    static final String PREFIX    = "cdc";
    static final String ORDERS    = "orders";
    static final String PRODUCTS  = "products";
    static final String DB_NAME   = "shop";

    @TempDir
    static Path sharedTempDir;

    // ── Infrastructure ────────────────────────────────────────────────────────

    @Container
    static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.0")
            .withDatabaseName(DB_NAME)
            .withUsername("root")
            .withPassword("shoppass")
            .withCommand(
                    "--log-bin=mysql-bin",
                    "--binlog-format=ROW",
                    "--binlog-row-image=FULL",
                    "--server-id=1");

    @SuppressWarnings("resource")
    @Container
    static GenericContainer<?> nats = new GenericContainer<>("nats:2.10")
            .withCommand("-js")
            .withExposedPorts(4222);

    // ── Spring server configuration ───────────────────────────────────────────

    @DynamicPropertySource
    static void serverConfig(DynamicPropertyRegistry reg) {
        // MySQL CDC
        reg.add("app.cdc.mysql.url",      mysql::getJdbcUrl);
        reg.add("app.cdc.mysql.username", mysql::getUsername);
        reg.add("app.cdc.mysql.password", mysql::getPassword);
        reg.add("app.cdc.mysql.enabled",  () -> "true");

        // Disable Postgres — not part of this E2E scenario
        reg.add("app.cdc.postgres.enabled", () -> "false");

        // Fallback spring.datasource (required by Spring Boot)
        reg.add("spring.datasource.url",      mysql::getJdbcUrl);
        reg.add("spring.datasource.username", mysql::getUsername);
        reg.add("spring.datasource.password", mysql::getPassword);

        // NATS
        reg.add("app.nats.url",             () -> "nats://localhost:" + nats.getMappedPort(4222));
        reg.add("app.nats.stream-name",     () -> STREAM);
        reg.add("app.nats.subject-prefix",  () -> PREFIX);
        reg.add("app.nats.storage",         () -> "memory");
        reg.add("app.nats.max-age-minutes", () -> "5");
        reg.add("app.nats.enabled",         () -> "true");
    }

    // ── Shared JDBC connection ────────────────────────────────────────────────

    Connection db;

    @BeforeEach
    void openDb() throws Exception {
        db = DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
    }

    @AfterEach
    void closeDb() throws Exception {
        db.close();
    }

    // =========================================================================
    // Scenario 1 — Basic INSERT / UPDATE / DELETE land on NATS
    // =========================================================================

    @Nested
    class BasicCdcEvents {

        @BeforeEach
        void createTable() throws Exception {
            try (Statement s = db.createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + ORDERS);
                s.execute("""
                        CREATE TABLE %s (
                            id    INT AUTO_INCREMENT PRIMARY KEY,
                            item  VARCHAR(200) NOT NULL,
                            qty   INT DEFAULT 1
                        )""".formatted(ORDERS));
            }
            Thread.sleep(500);
        }

        @AfterEach
        void dropTable() throws Exception {
            try (Statement s = db.createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + ORDERS);
            }
        }

        @Test
        void insertShouldPublishToNats() throws Exception {
            try (io.nats.client.Connection nc = natsConnect()) {
                JetStreamSubscription sub = subscribe(nc.jetStream(), ORDERS);

                try (Statement s = db.createStatement()) {
                    s.execute("INSERT INTO " + ORDERS + " (item, qty) VALUES ('Widget', 3)");
                }

                List<Message> msgs = poll(sub, 1);
                assertThat(msgs).isNotEmpty();
                String payload = new String(msgs.get(0).getData());
                assertThat(payload).contains("Widget");
                assertThat(payload).contains("3");
                assertThat(payload).contains("INSERT");

                sub.unsubscribe();
            }
        }

        @Test
        void updateShouldPublishOldAndNewToNats() throws Exception {
            try (Statement s = db.createStatement()) {
                s.execute("INSERT INTO " + ORDERS + " (item, qty) VALUES ('Gadget', 1)");
            }
            Thread.sleep(300);

            try (io.nats.client.Connection nc = natsConnect()) {
                JetStreamSubscription sub = subscribe(nc.jetStream(), ORDERS);

                try (Statement s = db.createStatement()) {
                    s.execute("UPDATE " + ORDERS + " SET qty = 10 WHERE item = 'Gadget'");
                }

                List<Message> msgs = poll(sub, 1);
                assertThat(msgs).isNotEmpty();
                String payload = new String(msgs.get(0).getData());
                assertThat(payload).contains("Gadget");
                assertThat(payload).contains("10");
                assertThat(payload).contains("UPDATE");
                // MySQL UPDATE wraps old and new row (keys appear as \\\"old\\\" in outer JSON)
                assertThat(payload).contains("old");
                assertThat(payload).contains("new");

                sub.unsubscribe();
            }
        }

        @Test
        void deleteShouldPublishFullRowToNats() throws Exception {
            try (Statement s = db.createStatement()) {
                s.execute("INSERT INTO " + ORDERS + " (item, qty) VALUES ('OldItem', 5)");
            }
            Thread.sleep(300);

            try (io.nats.client.Connection nc = natsConnect()) {
                JetStreamSubscription sub = subscribe(nc.jetStream(), ORDERS);

                try (Statement s = db.createStatement()) {
                    s.execute("DELETE FROM " + ORDERS + " WHERE item = 'OldItem'");
                }

                List<Message> msgs = poll(sub, 1);
                assertThat(msgs).isNotEmpty();
                String payload = new String(msgs.get(0).getData());
                // MySQL binlog_row_image=FULL — all columns in DELETE
                assertThat(payload).contains("OldItem");
                assertThat(payload).contains("DELETE");

                sub.unsubscribe();
            }
        }
    }

    // =========================================================================
    // Scenario 2 — Real column names (not col_0, col_1)
    // =========================================================================

    @Nested
    class RealColumnNames {

        @BeforeEach
        void createTable() throws Exception {
            try (Statement s = db.createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + PRODUCTS);
                s.execute("""
                        CREATE TABLE %s (
                            sku         VARCHAR(50) PRIMARY KEY,
                            description TEXT,
                            price       DECIMAL(10,2)
                        )""".formatted(PRODUCTS));
            }
            Thread.sleep(500);
        }

        @AfterEach
        void dropTable() throws Exception {
            try (Statement s = db.createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + PRODUCTS);
            }
        }

        @Test
        void eventDataContainsActualColumnNames() throws Exception {
            try (io.nats.client.Connection nc = natsConnect()) {
                JetStreamSubscription sub = subscribe(nc.jetStream(), PRODUCTS);

                try (Statement s = db.createStatement()) {
                    s.execute("INSERT INTO " + PRODUCTS +
                            " (sku, description, price) VALUES ('P-001', 'Blue pen', 1.99)");
                }

                List<Message> msgs = poll(sub, 1);
                assertThat(msgs).isNotEmpty();
                String payload = new String(msgs.get(0).getData());

                assertThat(payload).contains("sku");
                assertThat(payload).contains("description");
                assertThat(payload).contains("price");
                assertThat(payload).contains("P-001");
                assertThat(payload).contains("Blue pen");
                assertThat(payload).doesNotContain("col_0");
                assertThat(payload).doesNotContain("col_1");

                sub.unsubscribe();
            }
        }

        @Test
        void columnNamesUpdatedAfterAlterTable() throws Exception {
            // Add column to existing table — MySQL TABLE_MAP is re-sent before every DML
            try (Statement s = db.createStatement()) {
                s.execute("ALTER TABLE " + PRODUCTS + " ADD COLUMN stock INT DEFAULT 0");
            }
            Thread.sleep(300);

            try (io.nats.client.Connection nc = natsConnect()) {
                JetStreamSubscription sub = subscribe(nc.jetStream(), PRODUCTS);

                try (Statement s = db.createStatement()) {
                    s.execute("INSERT INTO " + PRODUCTS +
                            " (sku, description, price, stock) VALUES ('P-002', 'Red pen', 2.49, 100)");
                }

                List<Message> msgs = poll(sub, 1);
                assertThat(msgs).isNotEmpty();
                String payload = new String(msgs.get(0).getData());

                // New column name should appear (MySQL TABLE_MAP re-sent before every DML)
                assertThat(payload).contains("stock");
                assertThat(payload).contains("100");

                sub.unsubscribe();
            }
        }
    }

    // =========================================================================
    // Scenario 3 — NATS subjects are correctly namespaced by schema and table
    // =========================================================================

    @Nested
    class NatsSubjectRouting {

        @BeforeEach
        void createTables() throws Exception {
            try (Statement s = db.createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + ORDERS);
                s.execute("DROP TABLE IF EXISTS " + PRODUCTS);
                s.execute("CREATE TABLE " + ORDERS +
                        " (id INT AUTO_INCREMENT PRIMARY KEY, item VARCHAR(200))");
                s.execute("CREATE TABLE " + PRODUCTS +
                        " (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(200))");
            }
            Thread.sleep(500);
        }

        @AfterEach
        void dropTables() throws Exception {
            try (Statement s = db.createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + ORDERS);
                s.execute("DROP TABLE IF EXISTS " + PRODUCTS);
            }
        }

        @Test
        void ordersEventDoesNotAppearOnProductsSubject() throws Exception {
            try (io.nats.client.Connection nc = natsConnect()) {
                JetStreamSubscription productsSub = subscribe(nc.jetStream(), PRODUCTS);

                // Write to ORDERS — must NOT appear on products subject
                try (Statement s = db.createStatement()) {
                    s.execute("INSERT INTO " + ORDERS + " (item) VALUES ('ShouldNotRoute')");
                }

                // Write to PRODUCTS — must appear
                try (Statement s = db.createStatement()) {
                    s.execute("INSERT INTO " + PRODUCTS + " (name) VALUES ('CorrectProduct')");
                }

                List<Message> msgs = poll(productsSub, 1);
                assertThat(msgs).isNotEmpty();
                assertThat(new String(msgs.get(0).getData())).contains("CorrectProduct");

                List<Message> drain = pollDrain(productsSub);
                assertThat(drain.stream()
                        .map(m -> new String(m.getData()))
                        .noneMatch(p -> p.contains("ShouldNotRoute"))).isTrue();

                productsSub.unsubscribe();
            }
        }
    }

    // =========================================================================
    // Scenario 4 — Binlog offset persistence (FileBinlogOffsetStore)
    // =========================================================================

    @Nested
    class BinlogOffsetPersistence {

        @Test
        void offsetStoreRoundTrip(@TempDir Path tmp) throws Exception {
            FileBinlogOffsetStore store = new FileBinlogOffsetStore(tmp.resolve("test.offset"));

            // No file yet → empty
            assertThat(store.load()).isEmpty();

            // Capture live binlog position from MySQL
            String file;
            long pos;
            try (Statement s = db.createStatement();
                 ResultSet rs = s.executeQuery("SHOW MASTER STATUS")) {
                rs.next();
                file = rs.getString("File");
                pos  = rs.getLong("Position");
            }

            store.save(new BinlogPosition(file, pos));

            Optional<BinlogPosition> loaded = store.load();
            assertThat(loaded).isPresent();
            assertThat(loaded.get().file()).isEqualTo(file);
            assertThat(loaded.get().position()).isEqualTo(pos);
            assertThat(loaded.get().asString()).isEqualTo(file + ":" + pos);
        }

        @Test
        void overwritingOffsetReturnsLatest(@TempDir Path tmp) throws Exception {
            FileBinlogOffsetStore store = new FileBinlogOffsetStore(tmp.resolve("latest.offset"));

            store.save(new BinlogPosition("mysql-bin.000001", 100));
            store.save(new BinlogPosition("mysql-bin.000001", 500)); // overwrite

            Optional<BinlogPosition> loaded = store.load();
            assertThat(loaded).isPresent();
            assertThat(loaded.get().position()).isEqualTo(500);
        }
    }

    // =========================================================================
    // Scenario 5 — DDL events (CREATE/ALTER/DROP) arrive on NATS
    // =========================================================================

    @Nested
    class DdlEvents {

        @Test
        void ddlStatementsPublishedToNats() throws Exception {
            try (io.nats.client.Connection nc = natsConnect()) {
                // Subscribe to DDL subject: cdc.{schema}._ddl
                JetStreamSubscription sub = subscribeDdl(nc.jetStream());

                try (Statement s = db.createStatement()) {
                    s.execute("CREATE TABLE ddl_e2e_test (id INT PRIMARY KEY, val TEXT)");
                    s.execute("ALTER TABLE ddl_e2e_test ADD COLUMN extra INT DEFAULT 0");
                    s.execute("DROP TABLE ddl_e2e_test");
                }

                List<Message> msgs = poll(sub, 3);
                assertThat(msgs).hasSizeGreaterThanOrEqualTo(3);

                List<String> payloads = msgs.stream()
                        .map(m -> new String(m.getData()))
                        .toList();

                // All should be DDL type
                assertThat(payloads).allMatch(p -> p.contains("\"type\":\"DDL\""));

                // SQL text captured in data
                assertThat(payloads).anyMatch(p -> p.toUpperCase().contains("CREATE TABLE"));
                assertThat(payloads).anyMatch(p -> p.toUpperCase().contains("ALTER TABLE"));
                assertThat(payloads).anyMatch(p -> p.toUpperCase().contains("DROP TABLE"));

                sub.unsubscribe();
            }
        }
    }

    // =========================================================================
    // Scenario 6 — Multiple rapid inserts arrive in order on NATS
    // =========================================================================

    @Nested
    class OrderedEvents {

        @BeforeEach
        void createTable() throws Exception {
            try (Statement s = db.createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + ORDERS);
                s.execute("CREATE TABLE " + ORDERS + " (id INT AUTO_INCREMENT PRIMARY KEY, seq INT)");
            }
            Thread.sleep(500);
        }

        @AfterEach
        void dropTable() throws Exception {
            try (Statement s = db.createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + ORDERS);
            }
        }

        @Test
        void multipleInsertsArriveInOrderOnNats() throws Exception {
            try (io.nats.client.Connection nc = natsConnect()) {
                JetStreamSubscription sub = subscribe(nc.jetStream(), ORDERS);

                try (Statement s = db.createStatement()) {
                    s.execute("INSERT INTO " + ORDERS + " (seq) VALUES (1)");
                    s.execute("INSERT INTO " + ORDERS + " (seq) VALUES (2)");
                    s.execute("INSERT INTO " + ORDERS + " (seq) VALUES (3)");
                }

                List<Message> msgs = poll(sub, 3);
                assertThat(msgs).hasSize(3);
                List<String> payloads = msgs.stream().map(m -> new String(m.getData())).toList();
                assertThat(payloads).allMatch(p -> p.contains("\"type\":\"INSERT\""));
                assertThat(payloads).allMatch(p -> p.contains("orders"));
                assertThat(payloads.get(0)).isNotEqualTo(payloads.get(1));
                assertThat(payloads.get(1)).isNotEqualTo(payloads.get(2));

                sub.unsubscribe();
            }
        }
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    io.nats.client.Connection natsConnect() throws Exception {
        return Nats.connect("nats://localhost:" + nats.getMappedPort(4222));
    }

    JetStreamSubscription subscribe(JetStream js, String table) throws Exception {
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .deliverPolicy(DeliverPolicy.New)
                .build();
        PushSubscribeOptions opts = PushSubscribeOptions.builder()
                .stream(STREAM)
                .configuration(cc)
                .build();
        return js.subscribe(PREFIX + "." + DB_NAME + "." + table, opts);
    }

    JetStreamSubscription subscribeDdl(JetStream js) throws Exception {
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .deliverPolicy(DeliverPolicy.New)
                .build();
        PushSubscribeOptions opts = PushSubscribeOptions.builder()
                .stream(STREAM)
                .configuration(cc)
                .build();
        return js.subscribe(PREFIX + "." + DB_NAME + "._ddl", opts);
    }

    List<Message> poll(JetStreamSubscription sub, int atLeast) throws Exception {
        List<Message> out = new ArrayList<>();
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
        while (out.size() < atLeast && System.currentTimeMillis() < deadline) {
            Message m = sub.nextMessage(Duration.ofMillis(300));
            if (m != null) { m.ack(); out.add(m); }
        }
        return out;
    }

    List<Message> pollDrain(JetStreamSubscription sub) throws Exception {
        List<Message> out = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 500;
        while (System.currentTimeMillis() < deadline) {
            Message m = sub.nextMessage(Duration.ofMillis(100));
            if (m != null) { m.ack(); out.add(m); }
        }
        return out;
    }
}
