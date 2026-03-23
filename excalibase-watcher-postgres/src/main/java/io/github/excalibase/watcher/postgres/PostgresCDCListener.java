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
import io.github.excalibase.watcher.postgres.snapshot.PostgresChunkedSnapshot;
import io.github.excalibase.watcher.postgres.snapshot.PostgresDumpSnapshot;
import io.github.excalibase.watcher.schema.SchemaHistoryStore;
import io.github.excalibase.watcher.snapshot.SnapshotMode;
import org.postgresql.PGConnection;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Low-level PostgreSQL CDC listener using logical replication (pgoutput protocol).
 * <p>
 * Requires PostgreSQL 9.4+ with {@code wal_level = logical} in postgresql.conf.
 * Consumes the Write-Ahead Log (WAL) stream and emits {@link CDCEvent} objects
 * for each INSERT, UPDATE, DELETE, TRUNCATE, BEGIN, COMMIT, and DDL message.
 * </p>
 *
 * <p>Message parsing is delegated to {@link PgOutputParser}.</p>
 *
 * <p>Construct via the builder:</p>
 * <pre>{@code
 * PostgresCDCListener listener = new PostgresCDCListener.Builder()
 *     .jdbcUrl("jdbc:postgresql://localhost:5432/mydb")
 *     .credentials("user", "password")
 *     .slotName("my_slot")
 *     .publicationName("my_publication")
 *     .eventHandler(cdcService::handleCDCEvent)
 *     .build();
 * listener.start();
 * }</pre>
 */
public class PostgresCDCListener {

    private static final Logger logger = LoggerFactory.getLogger(PostgresCDCListener.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String slotName;
    private final String publicationName;
    private final Consumer<CDCEvent> eventHandler;
    private final boolean createSlotIfNotExists;
    private final boolean createPublicationIfNotExists;
    private final Set<String> tableFilter;
    private final SnapshotMode snapshotMode;
    private final int snapshotChunkSize;
    private final java.nio.file.Path snapshotBackupFile;
    private final String snapshotStartLsn;
    private final boolean captureDdl;
    private static final String DDL_LOG_TABLE = "_cdc_ddl_log";
    private static final long STATUS_INTERVAL_MS = 10_000;
    private static final long HEARTBEAT_INTERVAL_MS = 30_000;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile long lastStatusUpdate = 0;
    private volatile long lastMessageTime = 0;

    private Connection connection;
    private PGReplicationStream stream;
    private final PgOutputParser parser;

    public PostgresCDCListener(String jdbcUrl, String username, String password,
                               String slotName, String publicationName,
                               Consumer<CDCEvent> eventHandler,
                               boolean createSlotIfNotExists, boolean createPublicationIfNotExists,
                               Set<String> tableFilter,
                               SnapshotMode snapshotMode, int snapshotChunkSize,
                               java.nio.file.Path snapshotBackupFile, String snapshotStartLsn,
                               boolean captureDdl,
                               SchemaHistoryStore schemaHistoryStore) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.slotName = slotName;
        this.publicationName = publicationName;
        this.eventHandler = eventHandler;
        this.createSlotIfNotExists = createSlotIfNotExists;
        this.createPublicationIfNotExists = createPublicationIfNotExists;
        this.tableFilter = Set.copyOf(tableFilter);
        this.snapshotMode = snapshotMode;
        this.snapshotChunkSize = snapshotChunkSize;
        this.snapshotBackupFile = snapshotBackupFile;
        this.snapshotStartLsn = snapshotStartLsn;
        this.captureDdl = captureDdl;
        this.parser = new PgOutputParser(eventHandler, this.tableFilter, captureDdl, schemaHistoryStore);
    }

    /**
     * Start the CDC listener. Opens a replication connection to PostgreSQL and begins
     * consuming WAL messages in a background thread named {@code postgres-cdc-listener}.
     */
    public void start() throws SQLException {
        if (running.get()) {
            throw new IllegalStateException("CDC Listener is already running");
        }

        logger.info("Starting PostgreSQL CDC Listener...");

        connect();

        // Snapshot phase — slot is already created above so WAL is captured during snapshot
        if (snapshotMode == SnapshotMode.CHUNKED) {
            logger.info("Running Postgres chunked snapshot before starting CDC...");
            String schema = parseSchema(jdbcUrl);
            PostgresChunkedSnapshot.run(jdbcUrl, username, password,
                    schema, tableFilter, snapshotChunkSize, eventHandler);
            logger.info("Postgres snapshot complete");
        } else if (snapshotMode == SnapshotMode.BACKUP_FILE) {
            if (snapshotBackupFile == null) {
                throw new IllegalStateException("BACKUP_FILE snapshot mode requires snapshotBackupFile to be set");
            }
            logger.info("Loading Postgres snapshot from backup file: {}", snapshotBackupFile);
            try {
                PostgresDumpSnapshot.run(snapshotBackupFile, snapshotStartLsn, eventHandler);
                logger.info("Postgres dump snapshot complete");
            } catch (java.io.IOException e) {
                throw new IllegalStateException("Failed to load backup file: " + snapshotBackupFile, e);
            }
        }

        running.set(true);
        lastMessageTime = System.currentTimeMillis();
        logger.info("CDC Listener started successfully");

        Thread listenerThread = new Thread(this::listen);
        listenerThread.setDaemon(false);
        listenerThread.setName("postgres-cdc-listener");
        listenerThread.start();
    }

    /**
     * Opens the replication connection, creates publication/slot if needed,
     * and starts the replication stream.
     */
    private void connect() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);
        props.setProperty("assumeMinServerVersion", "9.4");
        props.setProperty("replication", "database");
        props.setProperty("preferQueryMode", "simple");

        connection = DriverManager.getConnection(jdbcUrl, props);

        if (createPublicationIfNotExists) {
            createPublicationIfNotExists();
        }

        if (createSlotIfNotExists) {
            createReplicationSlotIfNotExists();
        }

        if (captureDdl) {
            createDdlTriggerIfNotExists();
        }

        PGConnection pgConnection = connection.unwrap(PGConnection.class);
        ChainedLogicalStreamBuilder streamBuilder = pgConnection
                .getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(slotName)
                .withSlotOption("proto_version", 1)
                .withSlotOption("publication_names", publicationName);

        stream = streamBuilder.start();
        parser.setStream(stream);
    }

    /**
     * Stop the CDC listener and close all connections.
     */
    public void stop() {
        if (!running.get()) {
            return;
        }

        logger.info("Stopping PostgreSQL CDC Listener...");
        running.set(false);

        try {
            if (stream != null) {
                stream.close();
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (Exception e) {
            logger.error("Error stopping CDC listener", e);
        }

        logger.info("CDC Listener stopped");
    }

    private void listen() {
        logger.info("Starting to listen for CDC events...");
        int reconnectAttempt = 0;

        while (running.get()) {
            try {
                ByteBuffer msg = stream.readPending();

                if (msg == null) {
                    long now = System.currentTimeMillis();
                    if (lastMessageTime > 0 && now - lastMessageTime >= HEARTBEAT_INTERVAL_MS) {
                        eventHandler.accept(new CDCEvent(CDCEvent.Type.HEARTBEAT, null, null,
                                null, "HEARTBEAT", stream.getLastReceiveLSN() != null
                                        ? stream.getLastReceiveLSN().asString() : null));
                        stream.forceUpdateStatus();
                        lastMessageTime = now;
                        lastStatusUpdate = now;
                    }
                    Thread.sleep(10L);
                    continue;
                }

                lastMessageTime = System.currentTimeMillis();
                reconnectAttempt = 0;

                CDCEvent event = parser.parse(msg, stream.getLastReceiveLSN());
                if (event != null) {
                    eventHandler.accept(event);
                }

                stream.setFlushedLSN(stream.getLastReceiveLSN());
                long now = System.currentTimeMillis();
                if (now - lastStatusUpdate >= STATUS_INTERVAL_MS) {
                    stream.forceUpdateStatus();
                    lastStatusUpdate = now;
                }

            } catch (SQLException e) {
                if (!running.get()) break;
                reconnectAttempt++;
                long delayMs = Math.min(1000L * reconnectAttempt, 30_000L);
                logger.warn("WAL read error (attempt {}), reconnecting in {}ms", reconnectAttempt, delayMs, e);

                closeQuietly();

                try {
                    Thread.sleep(delayMs);
                    if (!running.get()) break;
                    connect();
                    logger.info("Reconnected successfully after attempt {}", reconnectAttempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception reconnectEx) {
                    logger.error("Reconnect attempt {} failed", reconnectAttempt, reconnectEx);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Unexpected error in CDC listener", e);
            }
        }

        running.set(false);
        logger.info("CDC listener thread terminated");
    }

    // -------------------------------------------------------------------------
    // DB setup helpers
    // -------------------------------------------------------------------------

    private void createPublicationIfNotExists() throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT 1 FROM pg_publication WHERE pubname = ?")) {
            ps.setString(1, publicationName);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    try (Statement stmt = connection.createStatement()) {
                        stmt.execute("CREATE PUBLICATION " + validateIdentifier(publicationName) + " FOR ALL TABLES");
                    }
                    logger.info("Created publication: {}", publicationName);
                }
            }
        }
    }

    private void createReplicationSlotIfNotExists() throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = ?")) {
            ps.setString(1, slotName);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    try (PreparedStatement create = connection.prepareStatement(
                            "SELECT pg_create_logical_replication_slot(?, 'pgoutput')")) {
                        create.setString(1, slotName);
                        create.execute();
                    }
                    logger.info("Created replication slot: {}", slotName);
                }
            }
        }
    }

    private void createDdlTriggerIfNotExists() throws SQLException {
        // Safe: DDL_LOG_TABLE is a compile-time constant, not user input
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS %s (
                        id SERIAL PRIMARY KEY,
                        command_tag TEXT,
                        object_type TEXT,
                        schema_name TEXT,
                        object_identity TEXT,
                        query TEXT
                    )""".formatted(DDL_LOG_TABLE));

            stmt.execute("""
                    CREATE OR REPLACE FUNCTION _cdc_ddl_log_fn() RETURNS event_trigger LANGUAGE plpgsql AS $$
                    DECLARE r RECORD;
                    BEGIN
                      FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
                        INSERT INTO %s (command_tag, object_type, schema_name, object_identity, query)
                        VALUES (r.command_tag, r.object_type, r.schema_name, r.object_identity, current_query());
                      END LOOP;
                    END;$$""".formatted(DDL_LOG_TABLE));

            try {
                stmt.execute("CREATE EVENT TRIGGER cdc_ddl_capture ON ddl_command_end EXECUTE FUNCTION _cdc_ddl_log_fn()");
                logger.info("Created DDL capture event trigger");
            } catch (SQLException e) {
                if (!"42710".equals(e.getSQLState())) throw e;
            }

            // DROP fires ddl_command_end but pg_event_trigger_ddl_commands() returns empty.
            // Use sql_drop with pg_event_trigger_dropped_objects() instead.
            try {
                stmt.execute("""
                        CREATE OR REPLACE FUNCTION _cdc_ddl_drop_fn() RETURNS event_trigger LANGUAGE plpgsql AS $$
                        DECLARE r RECORD;
                        BEGIN
                          FOR r IN SELECT * FROM pg_event_trigger_dropped_objects() WHERE original LOOP
                            INSERT INTO %s (command_tag, object_type, schema_name, object_identity, query)
                            VALUES (tg_tag, r.object_type, r.schema_name, r.object_identity, current_query());
                          END LOOP;
                        END;$$""".formatted(DDL_LOG_TABLE));
                stmt.execute("CREATE EVENT TRIGGER cdc_ddl_drop_capture ON sql_drop EXECUTE FUNCTION _cdc_ddl_drop_fn()");
                logger.info("Created DDL drop capture event trigger");
            } catch (SQLException e) {
                if (!"42710".equals(e.getSQLState())) throw e;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Utilities
    // -------------------------------------------------------------------------

    static String parseSchema(String jdbcUrl) {
        int queryStart = jdbcUrl.indexOf('?');
        if (queryStart >= 0) {
            String query = jdbcUrl.substring(queryStart + 1);
            for (String param : query.split("&")) {
                String[] kv = param.split("=", 2);
                if (kv.length == 2 && "currentSchema".equals(kv[0])) {
                    return kv[1];
                }
            }
        }
        return "public";
    }

    private static String validateIdentifier(String name) {
        if (!name.matches("[a-zA-Z_][a-zA-Z0-9_$]*")) {
            throw new IllegalArgumentException("Invalid PostgreSQL identifier: " + name);
        }
        return name;
    }

    private void closeQuietly() {
        try {
            if (stream != null) stream.close();
        } catch (Exception ignored) {}
        try {
            if (connection != null && !connection.isClosed()) connection.close();
        } catch (Exception ignored) {}
    }

    // -------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------

    public static class Builder {
        private String jdbcUrl;
        private String username;
        private String password;
        private String slotName = "cdc_slot";
        private String publicationName = "cdc_publication";
        private boolean createSlotIfNotExists = true;
        private boolean createPublicationIfNotExists = true;
        private Consumer<CDCEvent> eventHandler;
        private List<String> tables = new ArrayList<>();
        private SnapshotMode snapshotMode = SnapshotMode.NONE;
        private int snapshotChunkSize = 10_000;
        private java.nio.file.Path snapshotBackupFile;
        private String snapshotStartLsn;
        private boolean captureDdl = false;
        private SchemaHistoryStore schemaHistoryStore;

        public Builder jdbcUrl(String jdbcUrl) { this.jdbcUrl = jdbcUrl; return this; }
        public Builder credentials(String username, String password) { this.username = username; this.password = password; return this; }
        public Builder slotName(String slotName) { this.slotName = slotName; return this; }
        public Builder publicationName(String publicationName) { this.publicationName = publicationName; return this; }
        public Builder eventHandler(Consumer<CDCEvent> eventHandler) { this.eventHandler = eventHandler; return this; }
        public Builder createSlotIfNotExists(boolean v) { this.createSlotIfNotExists = v; return this; }
        public Builder createPublicationIfNotExists(boolean v) { this.createPublicationIfNotExists = v; return this; }
        public Builder tables(List<String> tables) { this.tables = new ArrayList<>(tables); return this; }
        public Builder snapshotMode(SnapshotMode mode) { this.snapshotMode = mode; return this; }
        public Builder snapshotChunkSize(int size) { this.snapshotChunkSize = size; return this; }
        public Builder snapshotBackupFile(java.nio.file.Path path) { this.snapshotBackupFile = path; return this; }
        public Builder snapshotStartLsn(String lsn) { this.snapshotStartLsn = lsn; return this; }
        public Builder captureDdl(boolean v) { this.captureDdl = v; return this; }
        public Builder schemaHistoryStore(SchemaHistoryStore store) { this.schemaHistoryStore = store; return this; }

        public PostgresCDCListener build() {
            if (jdbcUrl == null || username == null || password == null || eventHandler == null) {
                throw new IllegalArgumentException(
                        "Missing required configuration: jdbcUrl, username, password, and eventHandler are required");
            }
            return new PostgresCDCListener(jdbcUrl, username, password, slotName, publicationName,
                    eventHandler, createSlotIfNotExists, createPublicationIfNotExists,
                    new java.util.HashSet<>(tables),
                    snapshotMode, snapshotChunkSize, snapshotBackupFile, snapshotStartLsn,
                    captureDdl, schemaHistoryStore);
        }
    }
}
