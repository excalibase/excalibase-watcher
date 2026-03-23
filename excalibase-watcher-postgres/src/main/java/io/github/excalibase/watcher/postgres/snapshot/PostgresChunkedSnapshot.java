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
package io.github.excalibase.watcher.postgres.snapshot;

import io.github.excalibase.watcher.CDCEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Performs an initial snapshot of existing PostgreSQL table data before CDC begins.
 *
 * <p><b>Important:</b> The replication slot must be created BEFORE this snapshot runs.
 * The slot captures all WAL changes from its creation point onward, so any modifications
 * that happen during the snapshot are preserved and will be delivered by CDC after the
 * snapshot completes — there is no gap.</p>
 *
 * <p>Tables with a single-column primary key are chunked by PK value (efficient).
 * Tables without a PK fall back to LIMIT/OFFSET.</p>
 */
public class PostgresChunkedSnapshot {

    private static final Logger log = LoggerFactory.getLogger(PostgresChunkedSnapshot.class);

    private PostgresChunkedSnapshot() {}

    /**
     * Snapshot all (or specified) tables and emit their rows as INSERT CDCEvents.
     *
     * @param jdbcUrl      JDBC URL (standard, not replication)
     * @param username     database username
     * @param password     database password
     * @param schema       schema name (e.g. "public")
     * @param tables       tables to snapshot; empty set means all tables in the schema
     * @param chunkSize    rows per SELECT batch
     * @param eventHandler receives each row as a {@link CDCEvent.Type#INSERT} event
     */
    public static void run(String jdbcUrl, String username, String password,
                           String schema, Set<String> tables, int chunkSize,
                           Consumer<CDCEvent> eventHandler) throws SQLException {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            List<String> tablesToSnapshot = tables.isEmpty()
                    ? fetchAllTables(conn, schema)
                    : new ArrayList<>(tables);

            log.info("Starting Postgres chunked snapshot: {} tables in schema '{}'",
                    tablesToSnapshot.size(), schema);

            for (String table : tablesToSnapshot) {
                snapshotTable(conn, schema, table, chunkSize, eventHandler);
            }

            log.info("Postgres snapshot complete — {} tables", tablesToSnapshot.size());
        }
    }

    private static List<String> fetchAllTables(Connection conn, String schema) throws SQLException {
        List<String> tables = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT table_name FROM information_schema.tables " +
                "WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name")) {
            ps.setString(1, schema);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) tables.add(rs.getString(1));
            }
        }
        return tables;
    }

    private static void snapshotTable(Connection conn, String schema, String table,
                                      int chunkSize, Consumer<CDCEvent> eventHandler) throws SQLException {
        List<String> columns = fetchColumnNames(conn, schema, table);
        String pkCol = fetchPrimaryKey(conn, schema, table);

        log.info("Snapshotting {}.{} (pk={}, columns={})", schema, table, pkCol, columns.size());
        long rowCount = 0;

        conn.setAutoCommit(false);
        try {
            if (pkCol != null) {
                rowCount = snapshotByPk(conn, schema, table, pkCol, columns, chunkSize, eventHandler);
            } else {
                rowCount = snapshotByOffset(conn, schema, table, columns, chunkSize, eventHandler);
            }
        } finally {
            conn.setAutoCommit(true);
        }

        log.info("Snapshot complete for {}.{}: {} rows", schema, table, rowCount);
    }

    private static long snapshotByPk(Connection conn, String schema, String table,
                                     String pkCol, List<String> columns, int chunkSize,
                                     Consumer<CDCEvent> eventHandler) throws SQLException {
        long rowCount = 0;
        Object lastPk = null;

        while (true) {
            String sql = lastPk == null
                    ? "SELECT * FROM \"" + schema + "\".\"" + table + "\" ORDER BY \"" + pkCol + "\" LIMIT " + chunkSize
                    : "SELECT * FROM \"" + schema + "\".\"" + table + "\" WHERE \"" + pkCol + "\" > ? ORDER BY \"" + pkCol + "\" LIMIT " + chunkSize;

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setFetchSize(chunkSize);
                if (lastPk != null) ps.setObject(1, lastPk);

                int batchCount = 0;
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        eventHandler.accept(new CDCEvent(
                                CDCEvent.Type.INSERT, schema, table,
                                resultSetToJson(rs, columns),
                                "SNAPSHOT", null));
                        lastPk = rs.getObject(pkCol);
                        batchCount++;
                        rowCount++;
                    }
                }
                if (batchCount < chunkSize) break;
            }
        }
        return rowCount;
    }

    private static long snapshotByOffset(Connection conn, String schema, String table,
                                         List<String> columns, int chunkSize,
                                         Consumer<CDCEvent> eventHandler) throws SQLException {
        long rowCount = 0;
        long offset = 0;

        while (true) {
            String sql = "SELECT * FROM \"" + schema + "\".\"" + table + "\" LIMIT " + chunkSize + " OFFSET " + offset;
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setFetchSize(chunkSize);
                int batchCount = 0;
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        eventHandler.accept(new CDCEvent(
                                CDCEvent.Type.INSERT, schema, table,
                                resultSetToJson(rs, columns),
                                "SNAPSHOT", null));
                        batchCount++;
                        rowCount++;
                    }
                }
                if (batchCount < chunkSize) break;
                offset += chunkSize;
            }
        }
        return rowCount;
    }

    private static String fetchPrimaryKey(Connection conn, String schema, String table) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT a.attname FROM pg_index i " +
                "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) " +
                "WHERE i.indrelid = (? || '.' || ?)::regclass AND i.indisprimary " +
                "ORDER BY a.attnum LIMIT 1")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    private static List<String> fetchColumnNames(Connection conn, String schema, String table) throws SQLException {
        List<String> names = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT column_name FROM information_schema.columns " +
                "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position")) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) names.add(rs.getString(1));
            }
        }
        return names;
    }

    private static String resultSetToJson(ResultSet rs, List<String> columns) throws SQLException {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append("\"").append(columns.get(i)).append("\": ");
            appendValue(sb, rs.getObject(i + 1));
        }
        sb.append("}");
        return sb.toString();
    }

    private static void appendValue(StringBuilder sb, Object value) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof byte[] bytes) {
            String str = new String(bytes, StandardCharsets.UTF_8)
                    .replace("\\", "\\\\").replace("\"", "\\\"")
                    .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
            sb.append("\"").append(str).append("\"");
        } else if (value instanceof Number || value instanceof Boolean) {
            sb.append(value);
        } else {
            String str = value.toString()
                    .replace("\\", "\\\\").replace("\"", "\\\"")
                    .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
            sb.append("\"").append(str).append("\"");
        }
    }
}
