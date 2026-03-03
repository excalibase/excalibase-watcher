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
package io.github.excalibase.watcher;

/**
 * Represents a CDC (Change Data Capture) event captured from a database.
 * <p>
 * Each event corresponds to a DML operation (INSERT, UPDATE, DELETE) or a transaction
 * boundary (BEGIN, COMMIT). The {@link #data} field is a JSON string containing the
 * changed row data. For UPDATE events, data is {@code {"old": {...}, "new": {...}}}.
 * </p>
 */
public class CDCEvent {

    public enum Type {
        BEGIN, COMMIT, INSERT, UPDATE, DELETE
    }

    private final Type type;
    private final String schema;
    private final String table;
    private final String data;
    private final String rawMessage;
    private final String lsn;
    private final long timestamp;

    public CDCEvent(Type type, String schema, String table, String data,
                    String rawMessage, String lsn) {
        this.type = type;
        this.schema = schema;
        this.table = table;
        this.data = data;
        this.rawMessage = rawMessage;
        this.lsn = lsn;
        this.timestamp = System.currentTimeMillis();
    }

    public Type getType() { return type; }
    public String getSchema() { return schema; }
    public String getTable() { return table; }
    public String getData() { return data; }
    public String getRawMessage() { return rawMessage; }

    /** Log Sequence Number as a string, or {@code null} if not applicable */
    public String getLsn() { return lsn; }
    public long getTimestamp() { return timestamp; }

    @Override
    public String toString() {
        return String.format("CDCEvent{type=%s, schema=%s, table=%s, data=%s, lsn=%s, timestamp=%d}",
                type, schema, table, data, lsn, timestamp);
    }
}
