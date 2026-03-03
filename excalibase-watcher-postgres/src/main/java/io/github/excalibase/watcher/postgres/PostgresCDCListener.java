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
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Low-level PostgreSQL CDC listener using logical replication (pgoutput protocol).
 * <p>
 * Requires PostgreSQL 9.4+ with {@code wal_level = logical} in postgresql.conf.
 * Consumes the Write-Ahead Log (WAL) stream and emits {@link CDCEvent} objects
 * for each INSERT, UPDATE, DELETE, BEGIN, and COMMIT message.
 * </p>
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
    private final AtomicBoolean running = new AtomicBoolean(false);

    private Connection connection;
    private PGReplicationStream stream;

    private final Map<Integer, RelationInfo> relationMap = new ConcurrentHashMap<>();

    public PostgresCDCListener(String jdbcUrl, String username, String password,
                               String slotName, String publicationName,
                               Consumer<CDCEvent> eventHandler,
                               boolean createSlotIfNotExists, boolean createPublicationIfNotExists) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.slotName = slotName;
        this.publicationName = publicationName;
        this.eventHandler = eventHandler;
        this.createSlotIfNotExists = createSlotIfNotExists;
        this.createPublicationIfNotExists = createPublicationIfNotExists;
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

        PGConnection pgConnection = connection.unwrap(PGConnection.class);
        ChainedLogicalStreamBuilder streamBuilder = pgConnection
                .getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(slotName)
                .withSlotOption("proto_version", 1)
                .withSlotOption("publication_names", publicationName);

        stream = streamBuilder.start();
        running.set(true);

        logger.info("CDC Listener started successfully");

        Thread listenerThread = new Thread(this::listen);
        listenerThread.setDaemon(false);
        listenerThread.setName("postgres-cdc-listener");
        listenerThread.start();
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

        while (running.get()) {
            try {
                ByteBuffer msg = stream.readPending();

                if (msg == null) {
                    Thread.sleep(10L);
                    continue;
                }

                processWALMessage(msg, stream.getLastReceiveLSN());

                stream.setFlushedLSN(stream.getLastReceiveLSN());
                stream.forceUpdateStatus();

            } catch (SQLException e) {
                logger.error("Error reading WAL message", e);
                if (!running.get()) break;

                try {
                    Thread.sleep(1000);
                    reconnect();
                } catch (Exception reconnectEx) {
                    logger.error("Failed to reconnect", reconnectEx);
                    break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Unexpected error in CDC listener", e);
            }
        }

        logger.info("CDC listener thread terminated");
    }

    private void processWALMessage(ByteBuffer buffer, LogSequenceNumber lsn) {
        try {
            buffer.rewind();
            CDCEvent event = parsePgOutputMessage(buffer, lsn);
            if (event != null) {
                eventHandler.accept(event);
            }
        } catch (Exception e) {
            logger.error("Error processing WAL message", e);
        }
    }

    private CDCEvent parsePgOutputMessage(ByteBuffer buffer, LogSequenceNumber lsn) {
        if (!buffer.hasRemaining()) {
            return null;
        }

        char msgType = (char) buffer.get();
        String lsnStr = lsn != null ? lsn.asString() : null;

        return switch (msgType) {
            case 'B' -> new CDCEvent(CDCEvent.Type.BEGIN, null, null, null, "BEGIN", lsnStr);
            case 'C' -> new CDCEvent(CDCEvent.Type.COMMIT, null, null, null, "COMMIT", lsnStr);
            case 'R' -> parseRelation(buffer);
            case 'I' -> parseInsert(buffer, lsnStr);
            case 'U' -> parseUpdate(buffer, lsnStr);
            case 'D' -> parseDelete(buffer, lsnStr);
            default -> {
                logger.debug("Unknown message type: {}", msgType);
                yield null;
            }
        };
    }

    private CDCEvent parseRelation(ByteBuffer buffer) {
        int relationId = buffer.getInt();
        String namespace = readString(buffer);
        String relationName = readString(buffer);
        relationMap.put(relationId, new RelationInfo(namespace, relationName));
        return null;
    }

    private CDCEvent parseInsert(ByteBuffer buffer, String lsn) {
        int relationId = buffer.getInt();
        RelationInfo relation = relationMap.get(relationId);

        if (relation == null) {
            logger.warn("Unknown relation ID: {}", relationId);
            return null;
        }

        buffer.get(); // skip tuple type
        String data = parseTupleData(buffer);

        return new CDCEvent(CDCEvent.Type.INSERT, relation.namespace, relation.name, data, "INSERT", lsn);
    }

    private CDCEvent parseUpdate(ByteBuffer buffer, String lsn) {
        int relationId = buffer.getInt();
        RelationInfo relation = relationMap.get(relationId);

        if (relation == null) {
            logger.warn("Unknown relation ID: {}", relationId);
            return null;
        }

        String data = "{";

        if (buffer.hasRemaining()) {
            byte tupleType = buffer.get();
            if (tupleType == 'K' || tupleType == 'O') {
                String oldData = parseTupleData(buffer);
                data += "\"old\":" + oldData;
            } else {
                buffer.position(buffer.position() - 1);
            }
        }

        if (buffer.hasRemaining()) {
            byte tupleType = buffer.get();
            if (tupleType == 'N') {
                String newData = parseTupleData(buffer);
                if (data.length() > 1) data += ", ";
                data += "\"new\":" + newData;
            }
        }

        data += "}";
        return new CDCEvent(CDCEvent.Type.UPDATE, relation.namespace, relation.name, data, "UPDATE", lsn);
    }

    private CDCEvent parseDelete(ByteBuffer buffer, String lsn) {
        int relationId = buffer.getInt();
        RelationInfo relation = relationMap.get(relationId);

        if (relation == null) {
            logger.warn("Unknown relation ID: {}", relationId);
            return null;
        }

        if (buffer.hasRemaining()) {
            buffer.get(); // skip tuple type
            String data = parseTupleData(buffer);
            return new CDCEvent(CDCEvent.Type.DELETE, relation.namespace, relation.name, data, "DELETE", lsn);
        }

        return new CDCEvent(CDCEvent.Type.DELETE, relation.namespace, relation.name, "{}", "DELETE", lsn);
    }

    private String readString(ByteBuffer buffer) {
        StringBuilder sb = new StringBuilder();
        byte b;
        while (buffer.hasRemaining() && (b = buffer.get()) != 0) {
            sb.append((char) b);
        }
        return sb.toString();
    }

    private String parseTupleData(ByteBuffer buffer) {
        if (!buffer.hasRemaining()) {
            return "{}";
        }

        StringBuilder data = new StringBuilder("{");

        try {
            short numColumns = buffer.getShort();

            for (int i = 0; i < numColumns; i++) {
                if (i > 0) data.append(", ");

                byte colType = buffer.get();
                data.append("\"col_").append(i).append("\":");

                switch (colType) {
                    case 'n' -> data.append("null");
                    case 't' -> {
                        int length = buffer.getInt();
                        byte[] bytes = new byte[length];
                        buffer.get(bytes);
                        String value = new String(bytes, StandardCharsets.UTF_8);
                        String escaped = value.replace("\\", "\\\\")
                                .replace("\"", "\\\"")
                                .replace("\n", "\\n")
                                .replace("\r", "\\r")
                                .replace("\t", "\\t");
                        data.append("\"").append(escaped).append("\"");
                    }
                    case 'u' -> data.append("\"unchanged\"");
                    default -> data.append("\"unknown\"");
                }
            }
        } catch (Exception e) {
            logger.debug("Error parsing tuple data: {}", e.getMessage());
            return "{\"parse_error\":\"" + e.getMessage() + "\"}";
        }

        data.append("}");
        return data.toString();
    }

    private void createPublicationIfNotExists() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            var rs = stmt.executeQuery("SELECT 1 FROM pg_publication WHERE pubname = '" + publicationName + "'");
            if (!rs.next()) {
                stmt.execute("CREATE PUBLICATION " + publicationName + " FOR ALL TABLES");
                logger.info("Created publication: {}", publicationName);
            }
        }
    }

    private void createReplicationSlotIfNotExists() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            var rs = stmt.executeQuery("SELECT 1 FROM pg_replication_slots WHERE slot_name = '" + slotName + "'");
            if (!rs.next()) {
                stmt.execute("SELECT pg_create_logical_replication_slot('" + slotName + "', 'pgoutput')");
                logger.info("Created replication slot: {}", slotName);
            }
        }
    }

    private void reconnect() throws SQLException, InterruptedException {
        logger.info("Attempting to reconnect...");
        stop();
        Thread.sleep(2000);
        start();
    }

    private static class RelationInfo {
        final String namespace;
        final String name;

        RelationInfo(String namespace, String name) {
            this.namespace = namespace;
            this.name = name;
        }
    }

    /**
     * Builder for {@link PostgresCDCListener}.
     */
    public static class Builder {
        private String jdbcUrl;
        private String username;
        private String password;
        private String slotName = "cdc_slot";
        private String publicationName = "cdc_publication";
        private boolean createSlotIfNotExists = true;
        private boolean createPublicationIfNotExists = true;
        private Consumer<CDCEvent> eventHandler;

        public Builder jdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        public Builder credentials(String username, String password) {
            this.username = username;
            this.password = password;
            return this;
        }

        public Builder slotName(String slotName) {
            this.slotName = slotName;
            return this;
        }

        public Builder publicationName(String publicationName) {
            this.publicationName = publicationName;
            return this;
        }

        public Builder eventHandler(Consumer<CDCEvent> eventHandler) {
            this.eventHandler = eventHandler;
            return this;
        }

        public Builder createSlotIfNotExists(boolean createSlotIfNotExists) {
            this.createSlotIfNotExists = createSlotIfNotExists;
            return this;
        }

        public Builder createPublicationIfNotExists(boolean createPublicationIfNotExists) {
            this.createPublicationIfNotExists = createPublicationIfNotExists;
            return this;
        }

        public PostgresCDCListener build() {
            if (jdbcUrl == null || username == null || password == null || eventHandler == null) {
                throw new IllegalArgumentException(
                        "Missing required configuration: jdbcUrl, username, password, and eventHandler are required");
            }
            return new PostgresCDCListener(jdbcUrl, username, password, slotName, publicationName,
                    eventHandler, createSlotIfNotExists, createPublicationIfNotExists);
        }
    }
}
