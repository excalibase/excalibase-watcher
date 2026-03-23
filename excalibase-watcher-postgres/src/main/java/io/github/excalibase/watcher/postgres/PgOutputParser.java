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
import io.github.excalibase.watcher.schema.SchemaHistoryEntry;
import io.github.excalibase.watcher.schema.SchemaHistoryStore;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Parses pgoutput protocol messages from a PostgreSQL WAL stream into {@link CDCEvent} objects.
 * <p>
 * Handles message types: BEGIN (B), COMMIT (C), RELATION (R), INSERT (I),
 * UPDATE (U), DELETE (D), TRUNCATE (T). Maintains a relation map for column
 * names and type OIDs. Optionally intercepts DDL log table inserts and saves
 * schema history snapshots.
 * </p>
 */
class PgOutputParser {

    private static final Logger logger = LoggerFactory.getLogger(PgOutputParser.class);

    /** PostgreSQL epoch: 2000-01-01 00:00:00 UTC in Unix epoch millis */
    private static final long PG_EPOCH_OFFSET_MILLIS = 946684800000L;

    private static final String DDL_LOG_TABLE = "_cdc_ddl_log";

    // Postgres type OIDs for numeric/boolean types (emit as JSON numbers/booleans, not strings)
    private static final Set<Integer> NUMERIC_OIDS = Set.of(
            20,   // INT8 (bigint)
            21,   // INT2 (smallint)
            23,   // INT4 (integer)
            26,   // OID
            700,  // FLOAT4 (real)
            701,  // FLOAT8 (double precision)
            1700  // NUMERIC (decimal)
    );
    private static final int BOOL_OID = 16;

    private final Consumer<CDCEvent> eventHandler;
    private final Set<String> tableFilter;
    private final boolean captureDdl;
    private final SchemaHistoryStore schemaHistoryStore; // nullable
    private final Map<Integer, RelationInfo> relationMap = new ConcurrentHashMap<>();

    /** Reference to the stream for LSN access during schema history saves. Set by the listener. */
    private volatile PGReplicationStream stream;

    PgOutputParser(Consumer<CDCEvent> eventHandler, Set<String> tableFilter,
                   boolean captureDdl, SchemaHistoryStore schemaHistoryStore) {
        this.eventHandler = eventHandler;
        this.tableFilter = tableFilter;
        this.captureDdl = captureDdl;
        this.schemaHistoryStore = schemaHistoryStore;
    }

    void setStream(PGReplicationStream stream) {
        this.stream = stream;
    }

    /**
     * Parse a single pgoutput WAL message and return a {@link CDCEvent}, or {@code null}
     * if the message should be skipped. For multi-event messages (TRUNCATE), additional
     * events are emitted directly via the eventHandler.
     */
    CDCEvent parse(ByteBuffer buffer, LogSequenceNumber lsn) {
        buffer.rewind();
        if (!buffer.hasRemaining()) {
            return null;
        }

        char msgType = (char) buffer.get();
        String lsnStr = lsn != null ? lsn.asString() : null;

        return switch (msgType) {
            case 'B' -> parseBegin(buffer, lsnStr);
            case 'C' -> new CDCEvent(CDCEvent.Type.COMMIT, null, null, null, "COMMIT", lsnStr);
            case 'R' -> parseRelation(buffer);
            case 'I' -> parseInsert(buffer, lsnStr);
            case 'U' -> parseUpdate(buffer, lsnStr);
            case 'D' -> parseDelete(buffer, lsnStr);
            case 'T' -> parseTruncate(buffer, lsnStr);
            default -> {
                logger.debug("Unknown message type: {}", msgType);
                yield null;
            }
        };
    }

    // -------------------------------------------------------------------------
    // Message parsers
    // -------------------------------------------------------------------------

    private CDCEvent parseBegin(ByteBuffer buffer, String lsn) {
        if (buffer.remaining() >= 20) {
            buffer.getLong(); // finalLSN — skip
            long pgMicros = buffer.getLong(); // microseconds since 2000-01-01
            long sourceTs = PG_EPOCH_OFFSET_MILLIS + (pgMicros / 1000);
            buffer.getInt(); // xid — skip
            return new CDCEvent(CDCEvent.Type.BEGIN, null, null, null, "BEGIN", lsn, sourceTs);
        }
        return new CDCEvent(CDCEvent.Type.BEGIN, null, null, null, "BEGIN", lsn);
    }

    private CDCEvent parseRelation(ByteBuffer buffer) {
        int relationId = buffer.getInt();
        String namespace = readString(buffer);
        String relationName = readString(buffer);

        // replica identity setting (1 byte)
        buffer.get();

        short numColumns = buffer.getShort();
        List<String> columns = new ArrayList<>(numColumns);
        List<Integer> typeOids = new ArrayList<>(numColumns);
        for (int i = 0; i < numColumns; i++) {
            buffer.get(); // column flags
            columns.add(readString(buffer));
            typeOids.add(buffer.getInt()); // column type OID
            buffer.getInt(); // type modifier
        }

        relationMap.put(relationId, new RelationInfo(namespace, relationName, columns, typeOids));

        if (schemaHistoryStore != null) {
            try {
                var colDefs = new ArrayList<SchemaHistoryEntry.ColumnDef>();
                for (int i = 0; i < columns.size(); i++) {
                    colDefs.add(new SchemaHistoryEntry.ColumnDef(columns.get(i), null, typeOids.get(i)));
                }
                String lsn = stream != null && stream.getLastReceiveLSN() != null
                        ? stream.getLastReceiveLSN().asString() : "unknown";
                schemaHistoryStore.save(new SchemaHistoryEntry(
                        lsn, namespace, relationName, colDefs, System.currentTimeMillis()));
            } catch (Exception e) {
                logger.warn("Failed to save schema history for {}.{}", namespace, relationName, e);
            }
        }

        return null;
    }

    private CDCEvent parseInsert(ByteBuffer buffer, String lsn) {
        int relationId = buffer.getInt();
        RelationInfo relation = relationMap.get(relationId);

        if (relation == null) {
            logger.warn("Unknown relation ID: {}", relationId);
            return null;
        }

        if (captureDdl && DDL_LOG_TABLE.equals(relation.name)) {
            buffer.get(); // skip tuple type
            String data = parseTupleData(buffer, relation.columns, relation.typeOids);
            return new CDCEvent(CDCEvent.Type.DDL, relation.namespace, null, data, "DDL", lsn);
        }

        if (!tableFilter.isEmpty() && !tableFilter.contains(relation.name)) return null;

        buffer.get(); // skip tuple type
        String data = parseTupleData(buffer, relation.columns, relation.typeOids);
        return new CDCEvent(CDCEvent.Type.INSERT, relation.namespace, relation.name, data, "INSERT", lsn);
    }

    private CDCEvent parseUpdate(ByteBuffer buffer, String lsn) {
        int relationId = buffer.getInt();
        RelationInfo relation = relationMap.get(relationId);

        if (relation == null) {
            logger.warn("Unknown relation ID: {}", relationId);
            return null;
        }
        if (!tableFilter.isEmpty() && !tableFilter.contains(relation.name)) return null;

        String data = "{";

        if (buffer.hasRemaining()) {
            byte tupleType = buffer.get();
            if (tupleType == 'K' || tupleType == 'O') {
                String oldData = parseTupleData(buffer, relation.columns, relation.typeOids);
                data += "\"old\":" + oldData;
            } else {
                buffer.position(buffer.position() - 1);
            }
        }

        if (buffer.hasRemaining()) {
            byte tupleType = buffer.get();
            if (tupleType == 'N') {
                String newData = parseTupleData(buffer, relation.columns, relation.typeOids);
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
        if (!tableFilter.isEmpty() && !tableFilter.contains(relation.name)) return null;

        if (buffer.hasRemaining()) {
            buffer.get(); // skip tuple type
            String data = parseTupleData(buffer, relation.columns, relation.typeOids);
            return new CDCEvent(CDCEvent.Type.DELETE, relation.namespace, relation.name, data, "DELETE", lsn);
        }

        return new CDCEvent(CDCEvent.Type.DELETE, relation.namespace, relation.name, "{}", "DELETE", lsn);
    }

    private CDCEvent parseTruncate(ByteBuffer buffer, String lsn) {
        int numRelations = buffer.getInt();
        byte options = buffer.get();

        CDCEvent firstEvent = null;
        for (int i = 0; i < numRelations; i++) {
            int relationId = buffer.getInt();
            RelationInfo relation = relationMap.get(relationId);
            if (relation == null) continue;
            if (!tableFilter.isEmpty() && !tableFilter.contains(relation.name)) continue;

            CDCEvent event = new CDCEvent(CDCEvent.Type.TRUNCATE, relation.namespace, relation.name,
                    "{\"options\":" + options + "}", "TRUNCATE", lsn);
            if (firstEvent == null) {
                firstEvent = event;
            } else {
                eventHandler.accept(event);
            }
        }
        return firstEvent;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private String readString(ByteBuffer buffer) {
        StringBuilder sb = new StringBuilder();
        byte b;
        while (buffer.hasRemaining() && (b = buffer.get()) != 0) {
            sb.append((char) b);
        }
        return sb.toString();
    }

    private String parseTupleData(ByteBuffer buffer, List<String> columns, List<Integer> typeOids) {
        if (!buffer.hasRemaining()) {
            return "{}";
        }

        StringBuilder data = new StringBuilder("{");

        try {
            short numColumns = buffer.getShort();

            for (int i = 0; i < numColumns; i++) {
                if (i > 0) data.append(", ");

                byte colType = buffer.get();
                String colName = (columns != null && i < columns.size()) ? columns.get(i) : "col_" + i;
                int typeOid = (typeOids != null && i < typeOids.size()) ? typeOids.get(i) : 0;
                data.append("\"").append(colName).append("\":");

                switch (colType) {
                    case 'n' -> data.append("null");
                    case 't' -> {
                        int length = buffer.getInt();
                        byte[] bytes = new byte[length];
                        buffer.get(bytes);
                        String value = new String(bytes, StandardCharsets.UTF_8);

                        if (NUMERIC_OIDS.contains(typeOid)) {
                            data.append(value);
                        } else if (typeOid == BOOL_OID) {
                            data.append("t".equals(value) ? "true" : "false");
                        } else {
                            String escaped = value.replace("\\", "\\\\")
                                    .replace("\"", "\\\"")
                                    .replace("\n", "\\n")
                                    .replace("\r", "\\r")
                                    .replace("\t", "\\t");
                            data.append("\"").append(escaped).append("\"");
                        }
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

    // -------------------------------------------------------------------------
    // Inner types
    // -------------------------------------------------------------------------

    static class RelationInfo {
        final String namespace;
        final String name;
        final List<String> columns;
        final List<Integer> typeOids;

        RelationInfo(String namespace, String name, List<String> columns, List<Integer> typeOids) {
            this.namespace = namespace;
            this.name = name;
            this.columns = columns;
            this.typeOids = typeOids;
        }
    }
}
