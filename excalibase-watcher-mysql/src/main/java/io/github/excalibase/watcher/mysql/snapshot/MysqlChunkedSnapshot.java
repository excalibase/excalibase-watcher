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
package io.github.excalibase.watcher.mysql.snapshot;

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
 * Performs an initial snapshot of existing MySQL table data before CDC begins.
 *
 * <p>Captures the current binlog position FIRST, then streams all rows via chunked
 * JDBC SELECTs (ordered by primary key). The returned {@link BinlogPosition} is used
 * as the CDC start point so no changes are missed.</p>
 *
 * <p>Tables with a single-column primary key are chunked by PK value (efficient).
 * Tables without a PK fall back to LIMIT/OFFSET (slower but correct).</p>
 */
public class MysqlChunkedSnapshot {

    private static final Logger log = LoggerFactory.getLogger(MysqlChunkedSnapshot.class);

    private MysqlChunkedSnapshot() {}

    /**
     * Run the snapshot and return the binlog position captured before any rows were read.
     *
     * @param jdbcUrl      JDBC URL of the source MySQL instance
     * @param username     database username
     * @param password     database password
     * @param schema       database/schema name
     * @param tables       tables to snapshot; empty set means all tables
     * @param chunkSize    rows per SELECT batch
     * @param eventHandler receives each row as a {@link CDCEvent.Type#INSERT} event
     * @return the binlog position at which CDC should begin
     */
    public static BinlogPosition run(String jdbcUrl, String username, String password,
                                     String schema, Set<String> tables, int chunkSize,
                                     Consumer<CDCEvent> eventHandler) throws SQLException {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            BinlogPosition position = captureBinlogPosition(conn);
            log.debug("Snapshot binlog position locked at: {}", position.asString());

            List<String> tablesToSnapshot = tables.isEmpty()
                    ? fetchAllTables(conn, schema)
                    : new ArrayList<>(tables);

            for (String table : tablesToSnapshot) {
                snapshotTable(conn, schema, table, chunkSize, position, eventHandler);
            }

            log.debug("Snapshot complete — {} tables", tablesToSnapshot.size());
            return position;
        }
    }

    private static BinlogPosition captureBinlogPosition(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW MASTER STATUS")) {
            if (!rs.next()) {
                throw new IllegalStateException(
                        "SHOW MASTER STATUS returned no rows. Ensure log_bin=ON in MySQL config.");
            }
            return new BinlogPosition(rs.getString("File"), rs.getLong("Position"));
        }
    }

    private static List<String> fetchAllTables(Connection conn, String schema) throws SQLException {
        List<String> tables = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES " +
                "WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME")) {
            ps.setString(1, schema);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) tables.add(rs.getString(1));
            }
        }
        return tables;
    }

    private static void snapshotTable(Connection conn, String schema, String table,
                                      int chunkSize, BinlogPosition position,
                                      Consumer<CDCEvent> eventHandler) throws SQLException {
        List<String> columns = fetchColumnNames(conn, schema, table);
        String pkCol = fetchPrimaryKey(conn, schema, table);

        log.debug("Snapshotting {}.{} (pk={}, columns={})", schema, table, pkCol, columns.size());
        long rowCount = 0;

        conn.setAutoCommit(false);
        try {
            if (pkCol != null) {
                rowCount = snapshotByPk(conn, schema, table, pkCol, columns, chunkSize, position, eventHandler);
            } else {
                rowCount = snapshotByOffset(conn, schema, table, columns, chunkSize, position, eventHandler);
            }
        } finally {
            conn.setAutoCommit(true);
        }

        log.debug("Snapshot complete for {}.{}: {} rows", schema, table, rowCount);
    }

    private static long snapshotByPk(Connection conn, String schema, String table,
                                     String pkCol, List<String> columns, int chunkSize,
                                     BinlogPosition position, Consumer<CDCEvent> eventHandler) throws SQLException {
        long rowCount = 0;
        Object lastPk = null;

        while (true) {
            String sql = lastPk == null
                    ? "SELECT * FROM `" + schema + "`.`" + table + "` ORDER BY `" + pkCol + "` LIMIT " + chunkSize
                    : "SELECT * FROM `" + schema + "`.`" + table + "` WHERE `" + pkCol + "` > ? ORDER BY `" + pkCol + "` LIMIT " + chunkSize;

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setFetchSize(chunkSize);
                if (lastPk != null) ps.setObject(1, lastPk);

                int batchCount = 0;
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        eventHandler.accept(new CDCEvent(
                                CDCEvent.Type.INSERT, schema, table,
                                resultSetToJson(rs, columns),
                                "SNAPSHOT", position.asString()));
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
                                         BinlogPosition position, Consumer<CDCEvent> eventHandler) throws SQLException {
        long rowCount = 0;
        long offset = 0;

        while (true) {
            String sql = "SELECT * FROM `" + schema + "`.`" + table + "` LIMIT " + chunkSize + " OFFSET " + offset;
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setFetchSize(chunkSize);
                int batchCount = 0;
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        eventHandler.accept(new CDCEvent(
                                CDCEvent.Type.INSERT, schema, table,
                                resultSetToJson(rs, columns),
                                "SNAPSHOT", position.asString()));
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
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' " +
                "ORDER BY ORDINAL_POSITION LIMIT 1")) {
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
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION")) {
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
