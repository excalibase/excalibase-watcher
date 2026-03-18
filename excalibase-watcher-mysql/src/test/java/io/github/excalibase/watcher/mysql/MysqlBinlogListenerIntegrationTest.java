package io.github.excalibase.watcher.mysql;

import io.github.excalibase.watcher.CDCEvent;
import io.github.excalibase.watcher.CDCService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
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
 * Integration tests for {@link MysqlBinlogListener} using a real MySQL container
 * with binlog replication enabled.
 *
 * <p>Requires Docker. MySQL is started with:</p>
 * <ul>
 *   <li>{@code binlog_format=ROW}</li>
 *   <li>{@code binlog_row_image=FULL}</li>
 *   <li>{@code log_bin=ON}</li>
 * </ul>
 */
@Testcontainers
class MysqlBinlogListenerIntegrationTest {

    private static final String TEST_TABLE = "cdc_test_users";

    @Container
    static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.0")
            .withDatabaseName("testdb")
            .withUsername("root")
            .withPassword("testpass")
            .withCommand(
                    "--log-bin=mysql-bin",
                    "--binlog-format=ROW",
                    "--binlog-row-image=FULL",
                    "--server-id=1");

    private String jdbcUrl;
    private CDCService cdcService;
    private MysqlBinlogListener binlogListener;
    private Connection regularConnection;

    @BeforeEach
    void setUp() throws Exception {
        jdbcUrl = mysql.getJdbcUrl();

        regularConnection = DriverManager.getConnection(jdbcUrl, mysql.getUsername(), mysql.getPassword());
        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + TEST_TABLE);
            stmt.execute("""
                    CREATE TABLE %s (
                        id    INT AUTO_INCREMENT PRIMARY KEY,
                        name  VARCHAR(100) NOT NULL,
                        email VARCHAR(200),
                        score INT DEFAULT 0
                    )
                    """.formatted(TEST_TABLE));
        }

        cdcService = new CDCService();

        binlogListener = new MysqlBinlogListener.Builder()
                .jdbcUrl(jdbcUrl)
                .credentials(mysql.getUsername(), mysql.getPassword())
                .tables(List.of(TEST_TABLE))
                .eventHandler(cdcService::handleCDCEvent)
                .build();

        binlogListener.start();
        cdcService.markRunning();

        // Allow binlog connection to establish
        Thread.sleep(500);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (binlogListener != null) binlogListener.stop();
        cdcService.shutdown();
        if (regularConnection != null && !regularConnection.isClosed()) {
            try (Statement stmt = regularConnection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + TEST_TABLE);
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
        assertThat(event.getSchema()).isEqualTo("testdb");
        assertThat(event.getData()).isNotNull();
        assertThat(event.getTimestamp()).isPositive();

        sub.dispose();
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
    void shouldCaptureMultipleInsertsInOrder() throws Exception {
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
        assertThat(events.get(0).getData()).contains("User1");
        assertThat(events.get(1).getData()).contains("User2");
        assertThat(events.get(2).getData()).contains("User3");

        sub.dispose();
    }

    @Test
    void shouldNotEmitRowsExistingBeforeListenerStart() throws Exception {
        // Insert BEFORE the new listener starts
        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name) VALUES ('PreExisting')");
        }

        CDCService freshCdcService = new CDCService();
        MysqlBinlogListener freshListener = new MysqlBinlogListener.Builder()
                .jdbcUrl(jdbcUrl)
                .credentials(mysql.getUsername(), mysql.getPassword())
                .tables(List.of(TEST_TABLE))
                .eventHandler(freshCdcService::handleCDCEvent)
                .build();

        List<CDCEvent> events = new ArrayList<>();
        Disposable sub = freshCdcService.getTableEventStream(TEST_TABLE).subscribe(events::add);

        freshListener.start();
        freshCdcService.markRunning();
        Thread.sleep(800);

        assertThat(events).isEmpty();

        sub.dispose();
        freshListener.stop();
        freshCdcService.shutdown();
    }

    @Test
    void shouldCaptureUpdateEvent() throws Exception {
        List<CDCEvent> events = new ArrayList<>();
        Disposable sub = cdcService.getTableEventStream(TEST_TABLE).subscribe(events::add);

        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name) VALUES ('Bob')");
        }
        waitForEvents(events, 1);
        events.clear();

        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("UPDATE " + TEST_TABLE + " SET name = 'Bobby' WHERE name = 'Bob'");
        }

        waitForEvents(events, 1);

        assertThat(events).hasSize(1);
        CDCEvent event = events.get(0);
        assertThat(event.getType()).isEqualTo(CDCEvent.Type.UPDATE);
        assertThat(event.getData()).contains("\"old\"");
        assertThat(event.getData()).contains("\"new\"");
        assertThat(event.getData()).contains("Bobby");

        sub.dispose();
    }

    @Test
    void shouldCaptureDeleteEvent() throws Exception {
        // INSERT first
        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name) VALUES ('ToDelete')");
        }

        List<CDCEvent> events = new ArrayList<>();
        Disposable sub = cdcService.getTableEventStream(TEST_TABLE).subscribe(events::add);
        waitForEvents(events, 1);
        events.clear();

        // DELETE
        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("DELETE FROM " + TEST_TABLE + " WHERE name = 'ToDelete'");
        }

        waitForEvents(events, 1);

        assertThat(events).hasSize(1);
        CDCEvent event = events.get(0);
        assertThat(event.getType()).isEqualTo(CDCEvent.Type.DELETE);
        assertThat(event.getData()).contains("ToDelete");  // full row, not just PK

        sub.dispose();
    }

    @Test
    void shouldNotRouteEventsToWrongTable() throws Exception {
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
    void shouldSupportMultipleSubscribersForSameTable() throws Exception {
        List<CDCEvent> sub1Events = new ArrayList<>();
        List<CDCEvent> sub2Events = new ArrayList<>();

        Disposable sub1 = cdcService.getTableEventStream(TEST_TABLE).subscribe(sub1Events::add);
        Disposable sub2 = cdcService.getTableEventStream(TEST_TABLE).subscribe(sub2Events::add);

        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name) VALUES ('SharedUser')");
        }

        waitForEvents(sub1Events, 1);
        waitForEvents(sub2Events, 1);

        assertThat(sub1Events).hasSize(1);
        assertThat(sub2Events).hasSize(1);

        sub1.dispose();
        sub2.dispose();
    }

    @Test
    void shouldReportRunningAfterStart() {
        assertThat(binlogListener.isRunning()).isTrue();
        assertThat(cdcService.isRunning()).isTrue();
    }

    @Test
    void shouldStopAfterStop() throws Exception {
        List<CDCEvent> events = new ArrayList<>();
        Disposable sub = cdcService.getTableEventStream(TEST_TABLE).subscribe(events::add);

        binlogListener.stop();
        assertThat(binlogListener.isRunning()).isFalse();

        Thread.sleep(300);

        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name) VALUES ('AfterStop')");
        }

        Thread.sleep(800);
        assertThat(events).isEmpty();

        sub.dispose();
    }

    @Test
    void shouldHandleNullableColumnsInData() throws Exception {
        List<CDCEvent> events = new ArrayList<>();
        Disposable sub = cdcService.getTableEventStream(TEST_TABLE).subscribe(events::add);

        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name) VALUES ('NoEmail')");
        }

        waitForEvents(events, 1);

        assertThat(events.get(0).getData()).contains("null");

        sub.dispose();
    }

    // -------------------------------------------------------------------------

    private void waitForEvents(List<CDCEvent> events, int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(8);
        while (events.size() < expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
    }
}
