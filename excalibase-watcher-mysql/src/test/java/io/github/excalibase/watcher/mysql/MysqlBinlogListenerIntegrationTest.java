package io.github.excalibase.watcher.mysql;

import io.github.excalibase.watcher.CDCEvent;
import io.github.excalibase.watcher.CDCService;
import io.github.excalibase.watcher.mysql.snapshot.MysqlDumpSnapshot;
import io.github.excalibase.watcher.snapshot.SnapshotMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.Disposable;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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
    void shouldMapColumnTypesCorrectly() throws Exception {
        // Create a table with various types
        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS type_test");
            stmt.execute("""
                    CREATE TABLE type_test (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        price DECIMAL(10,2),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        is_active BOOLEAN DEFAULT true,
                        birth_date DATE
                    )""");
        }

        CDCService typeCdc = new CDCService();
        List<CDCEvent> events = new ArrayList<>();
        typeCdc.getTableEventStream("type_test").subscribe(events::add);

        MysqlBinlogListener typeListener = new MysqlBinlogListener.Builder()
                .jdbcUrl(jdbcUrl)
                .credentials(mysql.getUsername(), mysql.getPassword())
                .tables(List.of("type_test"))
                .eventHandler(typeCdc::handleCDCEvent)
                .build();
        typeListener.start();
        typeCdc.markRunning();
        Thread.sleep(500);

        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("INSERT INTO type_test (price, is_active, birth_date) VALUES (19.99, true, '2000-01-15')");
        }

        waitForEvents(events, 1);
        assertThat(events).hasSize(1);
        String data = events.get(0).getData();

        // BigDecimal should be a JSON number (no quotes)
        assertThat(data).contains("19.99");
        assertThat(data).doesNotContain("\"19.99\"");

        // Boolean should be a JSON boolean
        assertThat(data).containsPattern("\"is_active\":\\s*(true|1)");

        // Date should be ISO format string
        assertThat(data).contains("2000-01-15");

        typeListener.stop();
        typeCdc.shutdown();
        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS type_test");
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
    void shouldReflectColumnChangesAfterAlterTable() throws Exception {
        List<CDCEvent> events = new ArrayList<>();
        Disposable sub = cdcService.getTableEventStream(TEST_TABLE).subscribe(events::add);

        // Insert before ALTER
        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name) VALUES ('BeforeAlter')");
        }
        waitForEvents(events, 1);
        assertThat(events.get(0).getData()).contains("\"name\"");
        events.clear();

        // ALTER TABLE — add a new column
        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("ALTER TABLE " + TEST_TABLE + " ADD COLUMN nickname VARCHAR(50)");
        }
        Thread.sleep(300); // let the DDL event be processed

        // Insert after ALTER — should include new column
        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name, nickname) VALUES ('AfterAlter', 'Nick')");
        }
        waitForEvents(events, 1);

        assertThat(events.get(0).getData()).contains("\"nickname\"");
        assertThat(events.get(0).getData()).contains("Nick");

        sub.dispose();
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

    @Test
    void shouldCapturePreExistingRowsWithChunkedSnapshot() throws Exception {
        // Insert rows BEFORE the snapshot listener starts
        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name, email) VALUES ('PreExist1', 'a@a.com')");
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name, email) VALUES ('PreExist2', 'b@b.com')");
        }

        CDCService snapshotCdcService = new CDCService();
        List<CDCEvent> events = new ArrayList<>();
        snapshotCdcService.getTableEventStream(TEST_TABLE).subscribe(events::add);

        MysqlBinlogListener snapshotListener = new MysqlBinlogListener.Builder()
                .jdbcUrl(jdbcUrl)
                .credentials(mysql.getUsername(), mysql.getPassword())
                .tables(List.of(TEST_TABLE))
                .snapshotMode(SnapshotMode.CHUNKED)
                .eventHandler(snapshotCdcService::handleCDCEvent)
                .build();

        snapshotListener.start();
        snapshotCdcService.markRunning();
        Thread.sleep(1000);

        // Pre-existing rows captured
        assertThat(events.stream().anyMatch(e -> e.getData().contains("PreExist1"))).isTrue();
        assertThat(events.stream().anyMatch(e -> e.getData().contains("PreExist2"))).isTrue();
        assertThat(events).allMatch(e -> e.getType() == CDCEvent.Type.INSERT);

        // New events after snapshot are still captured via CDC
        events.clear();
        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name) VALUES ('PostSnapshot')");
        }
        waitForEvents(events, 1);
        assertThat(events).anyMatch(e -> e.getData().contains("PostSnapshot"));

        snapshotListener.stop();
        snapshotCdcService.shutdown();
    }

    @Test
    void shouldCaptureRowsFromDumpFile() throws Exception {
        // Simulate a mysqldump --master-data=2 --complete-insert file
        String binlogFile;
        long binlogPos;
        try (Statement stmt = regularConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW MASTER STATUS")) {
            rs.next();
            binlogFile = rs.getString("File");
            binlogPos = rs.getLong("Position");
        }

        Path dumpFile = Files.createTempFile("test-dump", ".sql");
        try (BufferedWriter w = Files.newBufferedWriter(dumpFile)) {
            w.write("-- MySQL dump\n");
            w.write("-- CHANGE MASTER TO MASTER_LOG_FILE='" + binlogFile + "', MASTER_LOG_POS=" + binlogPos + ";\n");
            w.write("INSERT INTO `" + TEST_TABLE + "` (`id`, `name`, `email`, `score`) VALUES (1,'DumpUser1','u1@x.com',10);\n");
            w.write("INSERT INTO `" + TEST_TABLE + "` (`id`, `name`, `email`, `score`) VALUES (2,'DumpUser2','u2@x.com',20);\n");
        }

        List<CDCEvent> events = new ArrayList<>();
        MysqlDumpSnapshot.run(dumpFile, "testdb", events::add);

        assertThat(events).hasSize(2);
        assertThat(events).allMatch(e -> e.getType() == CDCEvent.Type.INSERT);
        assertThat(events.get(0).getData()).contains("DumpUser1");
        assertThat(events.get(1).getData()).contains("DumpUser2");
        assertThat(events.get(0).getTable()).isEqualTo(TEST_TABLE);
        assertThat(events.get(0).getLsn()).isEqualTo(binlogFile + ":" + binlogPos);

        Files.deleteIfExists(dumpFile);
    }

    @Test
    void shouldCaptureDdlEventsFromBinlog() throws Exception {
        // Need a listener without table filter to capture DDL for any table
        binlogListener.stop();
        cdcService.shutdown();

        CDCService ddlCdc = new CDCService();
        List<CDCEvent> ddlEvents = new CopyOnWriteArrayList<>();
        ddlCdc.getAllEventsFlux()
                .filter(e -> e.getType() == CDCEvent.Type.DDL)
                .subscribe(ddlEvents::add);

        MysqlBinlogListener ddlListener = new MysqlBinlogListener.Builder()
                .jdbcUrl(jdbcUrl)
                .credentials(mysql.getUsername(), mysql.getPassword())
                .eventHandler(ddlCdc::handleCDCEvent)
                .build();
        ddlListener.start();
        ddlCdc.markRunning();
        Thread.sleep(500);

        // Execute DDL statements
        try (Statement stmt = regularConnection.createStatement()) {
            stmt.execute("CREATE TABLE ddl_test_tbl (id INT PRIMARY KEY, val TEXT)");
            stmt.execute("ALTER TABLE ddl_test_tbl ADD COLUMN extra INT DEFAULT 0");
            stmt.execute("DROP TABLE ddl_test_tbl");
        }

        // Wait for DDL events
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(8);
        while (ddlEvents.size() < 3 && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }

        assertThat(ddlEvents).hasSizeGreaterThanOrEqualTo(3);
        assertThat(ddlEvents).allMatch(e -> e.getType() == CDCEvent.Type.DDL);

        // Verify SQL text is captured in the data field
        assertThat(ddlEvents).anyMatch(e -> e.getData().toUpperCase().contains("CREATE TABLE"));
        assertThat(ddlEvents).anyMatch(e -> e.getData().toUpperCase().contains("ALTER TABLE"));
        assertThat(ddlEvents).anyMatch(e -> e.getData().toUpperCase().contains("DROP TABLE"));

        // Schema should be the database name
        assertThat(ddlEvents).allMatch(e -> "testdb".equals(e.getSchema()));

        ddlListener.stop();
        ddlCdc.shutdown();
    }

    @Test
    void shouldEmitBeginAndCommitTransactionBoundaries() throws Exception {
        List<CDCEvent> allEvents = new CopyOnWriteArrayList<>();
        Disposable sub = cdcService.getAllEventsFlux().subscribe(allEvents::add);

        // Execute a transaction with an explicit commit
        try (Statement stmt = regularConnection.createStatement()) {
            regularConnection.setAutoCommit(false);
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name) VALUES ('TxTest1')");
            stmt.execute("INSERT INTO " + TEST_TABLE + " (name) VALUES ('TxTest2')");
            regularConnection.commit();
            regularConnection.setAutoCommit(true);
        }

        // Wait for enough events (BEGIN + 2x INSERT + COMMIT)
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(8);
        while (allEvents.size() < 4 && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }

        // Should have BEGIN, INSERT(s), COMMIT in order
        assertThat(allEvents).anyMatch(e -> e.getType() == CDCEvent.Type.BEGIN);
        assertThat(allEvents).anyMatch(e -> e.getType() == CDCEvent.Type.COMMIT);
        assertThat(allEvents.stream()
                .filter(e -> e.getType() == CDCEvent.Type.INSERT)
                .count()).isGreaterThanOrEqualTo(2);

        // COMMIT should contain xid
        CDCEvent commitEvent = allEvents.stream()
                .filter(e -> e.getType() == CDCEvent.Type.COMMIT)
                .findFirst().orElseThrow();
        assertThat(commitEvent.getData()).contains("xid");

        sub.dispose();
    }

    @Test
    void shouldReportNotRunningAfterStop() throws Exception {
        assertThat(binlogListener.isRunning()).isTrue();
        binlogListener.stop();
        assertThat(binlogListener.isRunning()).isFalse();
    }

    // -------------------------------------------------------------------------

    private void waitForEvents(List<CDCEvent> events, int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(8);
        while (events.size() < expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
    }
}
