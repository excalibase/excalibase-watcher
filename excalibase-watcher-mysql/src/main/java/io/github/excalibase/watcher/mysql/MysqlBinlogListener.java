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
package io.github.excalibase.watcher.mysql;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.XidEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import io.github.excalibase.watcher.CDCEvent;
import io.github.excalibase.watcher.mysql.snapshot.BinlogOffsetStore;
import io.github.excalibase.watcher.mysql.snapshot.BinlogPosition;
import io.github.excalibase.watcher.mysql.snapshot.MysqlChunkedSnapshot;
import io.github.excalibase.watcher.mysql.snapshot.MysqlDumpSnapshot;
import io.github.excalibase.watcher.snapshot.SnapshotMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Locale;
import java.util.regex.Pattern;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * MySQL CDC listener using binlog replication (same mechanism as Debezium).
 *
 * <p>Connects to MySQL as a replica and streams the binary log in real-time.
 * Captures INSERT, UPDATE, and DELETE events with full row data and real column names.</p>
 *
 * <p>MySQL server requirements:</p>
 * <pre>{@code
 * # my.cnf
 * log_bin          = ON
 * binlog_format    = ROW
 * binlog_row_image = FULL
 * server_id        = 1
 * }</pre>
 *
 * <p>The connecting user needs REPLICATION SLAVE and REPLICATION CLIENT privileges:</p>
 * <pre>{@code
 * GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user'@'%';
 * }</pre>
 *
 * <p>Construct via the builder:</p>
 * <pre>{@code
 * MysqlBinlogListener listener = new MysqlBinlogListener.Builder()
 *     .jdbcUrl("jdbc:mysql://localhost:3306/mydb")
 *     .credentials("user", "password")
 *     .tables(List.of("orders", "users"))  // empty = all tables
 *     .eventHandler(cdcService::handleCDCEvent)
 *     .build();
 * listener.start();
 * }</pre>
 */
public class MysqlBinlogListener {

    private static final Logger log = LoggerFactory.getLogger(MysqlBinlogListener.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String schema;
    private final Set<String> tableFilter; // empty = all tables
    private final Consumer<CDCEvent> eventHandler;
    private final SnapshotMode snapshotMode;
    private final int snapshotChunkSize;
    private final java.nio.file.Path snapshotBackupFile;
    private final BinlogOffsetStore offsetStore; // nullable

    private static final long OFFSET_SAVE_INTERVAL_MS = 10_000;
    private static final long OFFSET_SAVE_EVENT_INTERVAL = 1000;

    /** Matches DDL statements — only these QUERY events are emitted as CDCEvent.Type.DDL. */
    private static final Pattern DDL_PATTERN = Pattern.compile(
            "^\\s*(CREATE|ALTER|DROP|TRUNCATE|RENAME)\\s",
            Pattern.CASE_INSENSITIVE);

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong eventCount = new AtomicLong(0);
    private volatile long lastOffsetSave = 0;

    // tableId → TableMeta (schema + table name + ordered column names)
    private final Map<Long, TableMeta> tableRegistry = new ConcurrentHashMap<>();

    private BinaryLogClient binlogClient;
    private Connection metadataConnection;
    private Thread binlogThread;

    private MysqlBinlogListener(Builder builder) {
        this.jdbcUrl = builder.jdbcUrl;
        this.username = builder.username;
        this.password = builder.password;
        this.schema = builder.schema;
        this.tableFilter = Set.copyOf(builder.tables);
        this.eventHandler = builder.eventHandler;
        this.snapshotMode = builder.snapshotMode;
        this.snapshotChunkSize = builder.snapshotChunkSize;
        this.snapshotBackupFile = builder.snapshotBackupFile;
        this.offsetStore = builder.offsetStore;
    }

    /**
     * Start the binlog listener. Connects to MySQL, positions at the current binlog
     * offset (so pre-existing rows are not replayed), then streams events in a
     * background thread named {@code mysql-binlog-listener}.
     */
    public void start() throws SQLException, IOException {
        if (running.get()) {
            throw new IllegalStateException("MySQL binlog listener is already running");
        }

        log.info("Starting MySQL binlog listener — schema: {}, tables: {}",
                schema, tableFilter.isEmpty() ? "ALL" : tableFilter);

        metadataConnection = DriverManager.getConnection(jdbcUrl, username, password);

        String[] hostPort = parseHostPort(jdbcUrl);
        String host = hostPort[0];
        int port = Integer.parseInt(hostPort[1]);

        binlogClient = new BinaryLogClient(host, port, schema, username, password);
        binlogClient.setServerId(generateServerId());

        // Snapshot phase (if configured) — runs before CDC starts
        if (snapshotMode == SnapshotMode.CHUNKED) {
            log.info("Running chunked snapshot before starting CDC...");
            BinlogPosition snapshotPos = MysqlChunkedSnapshot.run(
                    jdbcUrl, username, password, schema, tableFilter, snapshotChunkSize, eventHandler);
            binlogClient.setBinlogFilename(snapshotPos.file());
            binlogClient.setBinlogPosition(snapshotPos.position());
            log.info("Snapshot complete, CDC starting from {}", snapshotPos.asString());
        } else if (snapshotMode == SnapshotMode.BACKUP_FILE) {
            if (snapshotBackupFile == null) {
                throw new IllegalStateException("BACKUP_FILE snapshot mode requires snapshotBackupFile to be set");
            }
            log.info("Loading snapshot from backup file: {}", snapshotBackupFile);
            try {
                BinlogPosition snapshotPos = MysqlDumpSnapshot.run(snapshotBackupFile, schema, eventHandler);
                binlogClient.setBinlogFilename(snapshotPos.file());
                binlogClient.setBinlogPosition(snapshotPos.position());
                log.info("Dump snapshot complete, CDC starting from {}", snapshotPos.asString());
            } catch (java.io.IOException e) {
                throw new IllegalStateException("Failed to load backup file: " + snapshotBackupFile, e);
            }
        } else {
            // Try to resume from persisted offset before falling back to current position
            if (offsetStore != null) {
                Optional<BinlogPosition> savedPos = offsetStore.load();
                if (savedPos.isPresent()) {
                    BinlogPosition pos = savedPos.get();
                    binlogClient.setBinlogFilename(pos.file());
                    binlogClient.setBinlogPosition(pos.position());
                    log.info("Resuming from persisted binlog offset: {}", pos.asString());
                } else {
                    positionAtCurrentOffset();
                }
            } else {
                positionAtCurrentOffset();
            }
        }

        binlogClient.registerEventListener(this::handleEvent);
        binlogClient.registerLifecycleListener(new BinaryLogClient.AbstractLifecycleListener() {
            @Override
            public void onConnect(BinaryLogClient client) {
                log.info("MySQL binlog connected — {}:{}", host, port);
            }

            @Override
            public void onDisconnect(BinaryLogClient client) {
                if (running.get()) {
                    log.warn("MySQL binlog disconnected unexpectedly");
                }
            }
        });

        running.set(true);

        binlogThread = new Thread(() -> {
            int attempt = 0;
            while (running.get()) {
                try {
                    binlogClient.connect(); // blocks until disconnected
                    if (!running.get()) break; // clean stop via stop()
                } catch (IOException e) {
                    if (!running.get()) break; // stop() was called
                    attempt++;
                    long delayMs = Math.min(1000L * attempt, 30_000L);
                    log.warn("MySQL binlog disconnected unexpectedly (attempt {}), retrying in {}ms", attempt, delayMs);
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
            running.set(false);
            log.info("MySQL binlog listener thread exited");
        }, "mysql-binlog-listener");
        binlogThread.setDaemon(false);
        binlogThread.start();

        log.info("MySQL binlog listener started successfully");
    }

    /**
     * Stop the binlog listener and close all connections.
     */
    public void stop() {
        if (!running.get()) return;

        log.info("Stopping MySQL binlog listener...");
        running.set(false);

        try {
            if (binlogClient != null) {
                binlogClient.disconnect();
            }
        } catch (IOException e) {
            log.error("Error disconnecting binlog client", e);
        }

        try {
            if (metadataConnection != null && !metadataConnection.isClosed()) {
                metadataConnection.close();
            }
        } catch (SQLException e) {
            log.error("Error closing metadata connection", e);
        }

        log.info("MySQL binlog listener stopped");
    }

    public boolean isRunning() {
        return running.get();
    }

    // -------------------------------------------------------------------------
    // Event dispatch
    // -------------------------------------------------------------------------

    private void handleEvent(Event event) {
        EventType type = event.getHeader().getEventType();
        // Binlog event timestamp (millis since epoch)
        long srcTs = event.getHeader().getTimestamp();

        switch (type) {
            case TABLE_MAP -> handleTableMap(event.getData());
            case EXT_WRITE_ROWS, WRITE_ROWS -> handleInsert(event.getData(), srcTs);
            case EXT_UPDATE_ROWS, UPDATE_ROWS -> handleUpdate(event.getData(), srcTs);
            case EXT_DELETE_ROWS, DELETE_ROWS -> handleDelete(event.getData(), srcTs);
            case QUERY -> handleQuery(event.getData(), srcTs);
            case XID -> handleXid(event.getData(), srcTs);
            default -> { /* ignored */ }
        }
    }

    private void handleTableMap(TableMapEventData data) {
        String eventSchema = data.getDatabase();
        String table = data.getTable();

        if (!eventSchema.equals(schema)) return;
        if (!tableFilter.isEmpty() && !tableFilter.contains(table)) return;

        try {
            List<String> columns = fetchColumnNames(eventSchema, table);
            tableRegistry.put(data.getTableId(), new TableMeta(eventSchema, table, columns));
            log.debug("Registered table map: {}:{} → {} columns", eventSchema, table, columns.size());
        } catch (SQLException e) {
            log.error("Failed to fetch column metadata for {}.{}", eventSchema, table, e);
        }
    }

    private void handleInsert(WriteRowsEventData data, long srcTs) {
        TableMeta meta = tableRegistry.get(data.getTableId());
        if (meta == null) return;

        for (Serializable[] row : data.getRows()) {
            String json = rowToJson(row, meta.columns, data.getIncludedColumns());
            eventHandler.accept(new CDCEvent(
                    CDCEvent.Type.INSERT, meta.schema, meta.table, json,
                    "INSERT", binlogPosition(), srcTs));
        }
        maybeSaveOffset();
    }

    private void handleUpdate(UpdateRowsEventData data, long srcTs) {
        TableMeta meta = tableRegistry.get(data.getTableId());
        if (meta == null) return;

        for (Map.Entry<Serializable[], Serializable[]> entry : data.getRows()) {
            String oldJson = rowToJson(entry.getKey(), meta.columns, data.getIncludedColumnsBeforeUpdate());
            String newJson = rowToJson(entry.getValue(), meta.columns, data.getIncludedColumns());
            String json = "{\"old\":" + oldJson + ", \"new\":" + newJson + "}";
            eventHandler.accept(new CDCEvent(
                    CDCEvent.Type.UPDATE, meta.schema, meta.table, json,
                    "UPDATE", binlogPosition(), srcTs));
        }
        maybeSaveOffset();
    }

    private void handleDelete(DeleteRowsEventData data, long srcTs) {
        TableMeta meta = tableRegistry.get(data.getTableId());
        if (meta == null) return;

        for (Serializable[] row : data.getRows()) {
            String json = rowToJson(row, meta.columns, data.getIncludedColumns());
            eventHandler.accept(new CDCEvent(
                    CDCEvent.Type.DELETE, meta.schema, meta.table, json,
                    "DELETE", binlogPosition(), srcTs));
        }
        maybeSaveOffset();
    }

    private void handleQuery(QueryEventData data, long srcTs) {
        String sql = data.getSql();
        if (sql == null) return;

        // Transaction BEGIN
        if ("BEGIN".equals(sql)) {
            eventHandler.accept(new CDCEvent(
                    CDCEvent.Type.BEGIN, schema, null, null, "BEGIN", binlogPosition(), srcTs));
            return;
        }

        // DDL statements
        if (!DDL_PATTERN.matcher(sql).find()) return;

        String eventSchema = data.getDatabase();
        if (eventSchema != null && !eventSchema.equals(schema)) return;

        eventHandler.accept(new CDCEvent(
                CDCEvent.Type.DDL, eventSchema, null, sql,
                "DDL", binlogPosition(), srcTs));
    }

    private void handleXid(XidEventData data, long srcTs) {
        eventHandler.accept(new CDCEvent(
                CDCEvent.Type.COMMIT, schema, null,
                "{\"xid\":" + data.getXid() + "}",
                "COMMIT", binlogPosition(), srcTs));
    }

    private void maybeSaveOffset() {
        if (offsetStore == null || binlogClient == null) return;
        long count = eventCount.incrementAndGet();
        long now = System.currentTimeMillis();
        if (count % OFFSET_SAVE_EVENT_INTERVAL == 0 || now - lastOffsetSave >= OFFSET_SAVE_INTERVAL_MS) {
            try {
                offsetStore.save(new BinlogPosition(
                        binlogClient.getBinlogFilename(), binlogClient.getBinlogPosition()));
                lastOffsetSave = now;
            } catch (IOException e) {
                log.warn("Failed to persist binlog offset", e);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void positionAtCurrentOffset() throws SQLException {
        // MySQL 8.4+ renamed SHOW MASTER STATUS to SHOW BINARY LOG STATUS
        try (Statement stmt = metadataConnection.createStatement()) {
            ResultSet rs = queryBinlogStatus(stmt);
            try (rs) {
                if (!rs.next()) {
                    throw new IllegalStateException(
                            "SHOW BINARY LOG STATUS / SHOW MASTER STATUS returned no rows. " +
                            "Ensure log_bin=ON and binlog_format=ROW in MySQL config.");
                }
                String file = rs.getString("File");
                long position = rs.getLong("Position");
                binlogClient.setBinlogFilename(file);
                binlogClient.setBinlogPosition(position);
                log.info("Binlog start position: {}:{}", file, position);
            }
        }
    }

    private ResultSet queryBinlogStatus(Statement stmt) throws SQLException {
        try {
            return stmt.executeQuery("SHOW BINARY LOG STATUS");
        } catch (SQLException e) {
            return stmt.executeQuery("SHOW MASTER STATUS");
        }
    }

    private List<String> fetchColumnNames(String schemaName, String tableName) throws SQLException {
        List<String> names = new ArrayList<>();
        try (PreparedStatement ps = metadataConnection.prepareStatement(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION")) {
            ps.setString(1, schemaName);
            ps.setString(2, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    names.add(rs.getString(1));
                }
            }
        }
        return names;
    }

    private String rowToJson(Serializable[] values, List<String> columns, BitSet includedColumns) {
        StringBuilder sb = new StringBuilder("{");
        int colIdx = 0;

        for (int i = 0; i < values.length; i++) {
            // Skip columns not included in this event (e.g. partial row images)
            while (colIdx < columns.size() && !includedColumns.get(colIdx)) {
                colIdx++;
            }
            if (colIdx >= columns.size()) break;

            if (i > 0) sb.append(", ");
            sb.append("\"").append(columns.get(colIdx)).append("\": ");
            appendValue(sb, values[i]);
            colIdx++;
        }

        sb.append("}");
        return sb.toString();
    }

    private static final DateTimeFormatter ISO_DT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    private void appendValue(StringBuilder sb, Object value) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof byte[] bytes) {
            // Strings are stored as byte[] in binlog events; try UTF-8, fallback to base64 for binary
            if (isValidUtf8Text(bytes)) {
                String str = new String(bytes, StandardCharsets.UTF_8);
                appendEscapedString(sb, str);
            } else {
                appendEscapedString(sb, Base64.getEncoder().encodeToString(bytes));
            }
        } else if (value instanceof Boolean) {
            sb.append(value);
        } else if (value instanceof BigDecimal bd) {
            sb.append(bd.toPlainString()); // avoid scientific notation
        } else if (value instanceof Number) {
            sb.append(value);
        } else if (value instanceof java.sql.Timestamp ts) {
            appendEscapedString(sb, ts.toInstant().atOffset(ZoneOffset.UTC).format(ISO_DT));
        } else if (value instanceof java.sql.Date d) {
            appendEscapedString(sb, d.toString()); // already ISO: yyyy-MM-dd
        } else if (value instanceof java.sql.Time t) {
            appendEscapedString(sb, t.toString()); // HH:mm:ss
        } else if (value instanceof java.util.Date d) {
            // DATETIME columns → java.util.Date
            appendEscapedString(sb, d.toInstant().atOffset(ZoneOffset.UTC).format(ISO_DT));
        } else if (value instanceof BitSet bs) {
            sb.append(bs.toLongArray().length > 0 ? bs.toLongArray()[0] : 0);
        } else {
            appendEscapedString(sb, value.toString());
        }
    }

    private void appendEscapedString(StringBuilder sb, String str) {
        sb.append("\"");
        sb.append(str.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t"));
        sb.append("\"");
    }

    /**
     * Heuristic check: returns false if bytes contain null (0x00), which indicates binary data
     * like BLOBs. This is NOT a full UTF-8 validator — binary data without null bytes
     * (e.g. compressed or encrypted payloads) would be incorrectly treated as text.
     * Acceptable for typical CDC workloads where binary columns are rare.
     */
    private static boolean isValidUtf8Text(byte[] bytes) {
        for (byte b : bytes) {
            if (b == 0) return false;
        }
        return true;
    }

    private String binlogPosition() {
        if (binlogClient == null) return null;
        return binlogClient.getBinlogFilename() + ":" + binlogClient.getBinlogPosition();
    }

    /** Derive a deterministic server ID from JDBC URL to avoid collisions across instances. */
    private long generateServerId() {
        return 65536 + (Math.abs(jdbcUrl.hashCode()) % 65536);
    }

    private static String[] parseHostPort(String jdbcUrl) {
        // jdbc:mysql://host:port/schema?params
        String stripped = jdbcUrl.replaceFirst("jdbc:mysql://", "");
        String hostPort = stripped.split("[/?]")[0];
        if (hostPort.contains(":")) {
            return hostPort.split(":", 2);
        }
        return new String[]{hostPort, "3306"};
    }

    // -------------------------------------------------------------------------
    // Inner types
    // -------------------------------------------------------------------------

    private record TableMeta(String schema, String table, List<String> columns) {}

    // -------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------

    public static class Builder {
        private String jdbcUrl;
        private String username;
        private String password;
        private String schema;
        private List<String> tables = new ArrayList<>();
        private Consumer<CDCEvent> eventHandler;
        private SnapshotMode snapshotMode = SnapshotMode.NONE;
        private int snapshotChunkSize = 10_000;
        private java.nio.file.Path snapshotBackupFile;
        private BinlogOffsetStore offsetStore;

        public Builder jdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        public Builder credentials(String username, String password) {
            this.username = username;
            this.password = password;
            return this;
        }

        public Builder schema(String schema) {
            this.schema = schema;
            return this;
        }

        public Builder tables(List<String> tables) {
            this.tables = new ArrayList<>(tables);
            return this;
        }

        public Builder eventHandler(Consumer<CDCEvent> eventHandler) {
            this.eventHandler = eventHandler;
            return this;
        }

        public Builder snapshotMode(SnapshotMode snapshotMode) {
            this.snapshotMode = snapshotMode;
            return this;
        }

        public Builder snapshotChunkSize(int chunkSize) {
            this.snapshotChunkSize = chunkSize;
            return this;
        }

        public Builder snapshotBackupFile(java.nio.file.Path path) {
            this.snapshotBackupFile = path;
            return this;
        }

        public Builder offsetStore(BinlogOffsetStore offsetStore) {
            this.offsetStore = offsetStore;
            return this;
        }

        public MysqlBinlogListener build() {
            if (jdbcUrl == null || username == null || password == null || eventHandler == null) {
                throw new IllegalArgumentException(
                        "Required: jdbcUrl, username, password, eventHandler");
            }
            if (schema == null) {
                schema = parseSchema(jdbcUrl);
            }
            return new MysqlBinlogListener(this);
        }

        private static String parseSchema(String jdbcUrl) {
            // jdbc:mysql://host:port/schema?params
            String stripped = jdbcUrl.replaceFirst("jdbc:mysql://", "");
            String[] parts = stripped.split("/", 2);
            if (parts.length < 2) {
                throw new IllegalArgumentException(
                        "Cannot parse schema from jdbcUrl: " + jdbcUrl);
            }
            return parts[1].split("[?]")[0];
        }
    }
}
