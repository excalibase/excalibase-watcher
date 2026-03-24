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

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses a {@code mysqldump} file and emits each row as a {@link CDCEvent.Type#INSERT} event.
 *
 * <p>The dump must be produced with {@code --complete-insert --master-data=2}:</p>
 * <pre>{@code
 * mysqldump --complete-insert --master-data=2 --single-transaction mydb > dump.sql
 * }</pre>
 *
 * <p>The {@code --master-data=2} flag embeds the binlog position as a comment:</p>
 * <pre>{@code
 * -- CHANGE MASTER TO MASTER_LOG_FILE='mysql-bin.000001', MASTER_LOG_POS=12345;
 * }</pre>
 * This position is used as the CDC start point so there is no gap between the dump
 * and the live binlog stream.
 *
 * <p>{@code --complete-insert} produces INSERT statements with explicit column names:</p>
 * <pre>{@code
 * INSERT INTO `orders` (`id`, `name`, `amount`) VALUES (1,'test',100.00);
 * }</pre>
 */
public class MysqlDumpSnapshot {

    private static final Logger log = LoggerFactory.getLogger(MysqlDumpSnapshot.class);

    // Matches: -- CHANGE MASTER TO MASTER_LOG_FILE='file', MASTER_LOG_POS=pos;
    // Also matches MySQL 8.0.23+ CHANGE REPLICATION SOURCE TO SOURCE_LOG_FILE=..., SOURCE_LOG_POS=...
    private static final Pattern BINLOG_POS_PATTERN = Pattern.compile(
            "(?:MASTER_LOG_FILE|SOURCE_LOG_FILE)='([^']+)'.*?(?:MASTER_LOG_POS|SOURCE_LOG_POS)=(\\d+)",
            Pattern.CASE_INSENSITIVE);

    // Matches: INSERT INTO `table` (`col1`, `col2`) VALUES (val1, val2), (val3, val4);
    private static final Pattern INSERT_PATTERN = Pattern.compile(
            "INSERT INTO `([^`]+)` \\(([^)]+)\\) VALUES (.+);\\s*$",
            Pattern.CASE_INSENSITIVE);

    private MysqlDumpSnapshot() {}

    /**
     * Parse a mysqldump file and emit rows as INSERT CDCEvents.
     *
     * @param dumpFile     path to the mysqldump SQL file
     * @param schema       the database/schema name to assign to emitted events
     * @param eventHandler receives each row as a CDCEvent
     * @return the binlog position parsed from the dump file header
     * @throws IOException  if the file cannot be read
     * @throws IllegalStateException if no binlog position comment is found in the dump
     */
    public static BinlogPosition run(Path dumpFile, String schema,
                                     Consumer<CDCEvent> eventHandler) throws IOException {
        BinlogPosition position = null;
        long rowCount = 0;

        try (BufferedReader reader = Files.newBufferedReader(dumpFile)) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();

                if (position == null && line.startsWith("--")) {
                    Matcher m = BINLOG_POS_PATTERN.matcher(line);
                    if (m.find()) {
                        position = new BinlogPosition(m.group(1), Long.parseLong(m.group(2)));
                        log.debug("Parsed binlog position from dump: {}", position.asString());
                    }
                    continue;
                }

                if (line.toUpperCase().startsWith("INSERT INTO")) {
                    Matcher m = INSERT_PATTERN.matcher(line);
                    if (m.matches()) {
                        String table = m.group(1);
                        List<String> columns = parseColumnList(m.group(2));
                        List<List<String>> rows = parseValuesList(m.group(3));
                        String lsn = position != null ? position.asString() : null;

                        for (List<String> row : rows) {
                            String json = buildJson(columns, row);
                            eventHandler.accept(new CDCEvent(
                                    CDCEvent.Type.INSERT, schema, table, json, "SNAPSHOT", lsn));
                            rowCount++;
                        }
                    }
                }
            }
        }

        if (position == null) {
            throw new IllegalStateException(
                    "No binlog position found in dump file. " +
                    "Ensure the dump was created with --master-data=2.");
        }

        log.debug("Dump snapshot complete: {} rows loaded, position {}", rowCount, position.asString());
        return position;
    }

    // -------------------------------------------------------------------------
    // Parsing helpers
    // -------------------------------------------------------------------------

    private static List<String> parseColumnList(String colStr) {
        List<String> cols = new ArrayList<>();
        for (String part : colStr.split(",")) {
            cols.add(part.trim().replaceAll("^`|`$", ""));
        }
        return cols;
    }

    /**
     * Parse the VALUES portion of an INSERT, handling multi-row tuples:
     * {@code (1,'a',NULL),(2,'b',3)}
     */
    static List<List<String>> parseValuesList(String valuesStr) {
        List<List<String>> rows = new ArrayList<>();
        int i = 0;
        int len = valuesStr.length();

        while (i < len) {
            // skip whitespace and commas between tuples
            while (i < len && (valuesStr.charAt(i) == ',' || valuesStr.charAt(i) == ' ')) i++;
            if (i >= len) break;

            if (valuesStr.charAt(i) == '(') {
                i++; // consume '('
                List<String> row = new ArrayList<>();
                while (i < len && valuesStr.charAt(i) != ')') {
                    // skip leading space/comma within tuple
                    while (i < len && valuesStr.charAt(i) == ' ') i++;
                    if (i < len && valuesStr.charAt(i) == ',') { i++; continue; }
                    if (i >= len || valuesStr.charAt(i) == ')') break;

                    row.add(readValue(valuesStr, i, len));
                    // advance i past the value we just read
                    i = skipValue(valuesStr, i, len);
                }
                if (i < len) i++; // consume ')'
                rows.add(row);
            } else {
                i++;
            }
        }
        return rows;
    }

    private static String readValue(String s, int start, int len) {
        char c = s.charAt(start);
        if (c == '\'') {
            // quoted string
            StringBuilder sb = new StringBuilder();
            int i = start + 1;
            while (i < len) {
                char ch = s.charAt(i);
                if (ch == '\\' && i + 1 < len) {
                    char next = s.charAt(i + 1);
                    sb.append(unescape(next));
                    i += 2;
                } else if (ch == '\'') {
                    break;
                } else {
                    sb.append(ch);
                    i++;
                }
            }
            return sb.toString();
        } else {
            // unquoted: NULL, number, etc.
            int end = start;
            while (end < len && s.charAt(end) != ',' && s.charAt(end) != ')') end++;
            return s.substring(start, end).trim();
        }
    }

    private static int skipValue(String s, int start, int len) {
        char c = s.charAt(start);
        if (c == '\'') {
            int i = start + 1;
            while (i < len) {
                char ch = s.charAt(i);
                if (ch == '\\' && i + 1 < len) { i += 2; }
                else if (ch == '\'') { i++; break; }
                else { i++; }
            }
            return i;
        } else {
            int i = start;
            while (i < len && s.charAt(i) != ',' && s.charAt(i) != ')') i++;
            return i;
        }
    }

    private static char unescape(char c) {
        return switch (c) {
            case 'n' -> '\n';
            case 'r' -> '\r';
            case 't' -> '\t';
            case '0' -> '\0';
            default -> c;
        };
    }

    private static String buildJson(List<String> columns, List<String> values) {
        StringBuilder sb = new StringBuilder("{");
        int count = Math.min(columns.size(), values.size());
        for (int i = 0; i < count; i++) {
            if (i > 0) sb.append(", ");
            sb.append("\"").append(escapeJson(columns.get(i))).append("\": ");
            String val = values.get(i);
            if (val == null || val.equalsIgnoreCase("NULL")) {
                sb.append("null");
            } else if (isNumeric(val)) {
                sb.append(val);
            } else {
                sb.append("\"").append(escapeJson(val)).append("\"");
            }
        }
        sb.append("}");
        return sb.toString();
    }

    private static boolean isNumeric(String s) {
        if (s.isEmpty()) return false;
        int start = (s.charAt(0) == '-') ? 1 : 0;
        boolean hasDot = false;
        for (int i = start; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '.' && !hasDot) { hasDot = true; }
            else if (!Character.isDigit(c)) return false;
        }
        return start < s.length();
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }
}
