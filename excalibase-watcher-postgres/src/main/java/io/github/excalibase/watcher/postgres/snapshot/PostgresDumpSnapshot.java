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

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses a {@code pg_dump} plain-format file and emits each row as a
 * {@link CDCEvent.Type#INSERT} event.
 *
 * <p>Produce the dump with:</p>
 * <pre>{@code
 * pg_dump --format=plain --no-owner mydb > dump.sql
 * }</pre>
 *
 * <p>The dump uses PostgreSQL COPY format for table data:</p>
 * <pre>{@code
 * COPY public.orders (id, name, amount) FROM stdin;
 * 1	test order	100.00
 * 2	second order	200.00
 * \.
 * }</pre>
 *
 * <p><b>Note:</b> {@code pg_dump} does not embed the WAL LSN. Pass the LSN at the
 * time the backup was taken via the {@code startLsn} parameter. Create the replication
 * slot BEFORE taking the dump to ensure no WAL is lost between dump and CDC start.</p>
 */
public class PostgresDumpSnapshot {

    private static final Logger log = LoggerFactory.getLogger(PostgresDumpSnapshot.class);

    // Matches: COPY schema.table (col1, col2, ...) FROM stdin;
    private static final Pattern COPY_HEADER = Pattern.compile(
            "COPY (\\S+)\\.(\\S+) \\(([^)]+)\\) FROM stdin;",
            Pattern.CASE_INSENSITIVE);

    private static final String COPY_END = "\\.";

    private PostgresDumpSnapshot() {}

    /**
     * Parse a pg_dump file and emit rows as INSERT CDCEvents.
     *
     * @param dumpFile     path to the pg_dump plain-format file
     * @param startLsn     WAL LSN at the time the backup was taken (assigned to all events)
     * @param eventHandler receives each row as a CDCEvent
     * @throws IOException if the file cannot be read
     */
    public static void run(Path dumpFile, String startLsn,
                           Consumer<CDCEvent> eventHandler) throws IOException {
        long rowCount = 0;
        int tableCount = 0;

        try (BufferedReader reader = Files.newBufferedReader(dumpFile)) {
            String line;
            while ((line = reader.readLine()) != null) {
                Matcher m = COPY_HEADER.matcher(line.trim());
                if (!m.matches()) continue;

                String schema = m.group(1);
                String table = m.group(2);
                List<String> columns = parseColumns(m.group(3));
                tableCount++;

                int tableRows = 0;

                while ((line = reader.readLine()) != null) {
                    if (line.equals(COPY_END)) break;

                    String json = copyLineToJson(line, columns);
                    eventHandler.accept(new CDCEvent(
                            CDCEvent.Type.INSERT, schema, table, json, "SNAPSHOT", startLsn));
                    tableRows++;
                    rowCount++;
                }

                log.debug("Loaded {} rows for {}.{}", tableRows, schema, table);
            }
        }

        log.debug("Dump snapshot complete: {} rows from {} tables", rowCount, tableCount);
    }

    // -------------------------------------------------------------------------

    private static List<String> parseColumns(String colStr) {
        List<String> cols = new ArrayList<>();
        for (String part : colStr.split(",")) {
            cols.add(part.trim());
        }
        return cols;
    }

    /**
     * Convert a tab-separated COPY line to JSON.
     * PostgreSQL COPY text format:
     * - columns separated by tab
     * - NULL represented as {@code \N}
     * - special chars escaped: {@code \t}, {@code \n}, {@code \r}, {@code \\}
     */
    private static String copyLineToJson(String line, List<String> columns) {
        String[] parts = line.split("\t", -1);
        StringBuilder sb = new StringBuilder("{");

        int count = Math.min(columns.size(), parts.length);
        for (int i = 0; i < count; i++) {
            if (i > 0) sb.append(", ");
            sb.append("\"").append(escapeJson(columns.get(i))).append("\": ");
            String val = parts[i];
            if ("\\N".equals(val)) {
                sb.append("null");
            } else {
                String unescaped = unescapeCopy(val);
                if (isNumeric(unescaped) || "true".equalsIgnoreCase(unescaped) || "false".equalsIgnoreCase(unescaped)) {
                    sb.append(unescaped.toLowerCase());
                } else {
                    sb.append("\"").append(escapeJson(unescaped)).append("\"");
                }
            }
        }
        sb.append("}");
        return sb.toString();
    }

    private static String unescapeCopy(String s) {
        if (!s.contains("\\")) return s;
        StringBuilder sb = new StringBuilder();
        int i = 0;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char next = s.charAt(i + 1);
                switch (next) {
                    case 't' -> { sb.append('\t'); i += 2; }
                    case 'n' -> { sb.append('\n'); i += 2; }
                    case 'r' -> { sb.append('\r'); i += 2; }
                    case '\\' -> { sb.append('\\'); i += 2; }
                    default -> { sb.append(c); i++; }
                }
            } else {
                sb.append(c);
                i++;
            }
        }
        return sb.toString();
    }

    private static boolean isNumeric(String s) {
        if (s.isEmpty()) return false;
        int start = (s.charAt(0) == '-') ? 1 : 0;
        boolean hasDot = false;
        for (int i = start; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '.' && !hasDot) hasDot = true;
            else if (!Character.isDigit(c)) return false;
        }
        return start < s.length();
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }
}
