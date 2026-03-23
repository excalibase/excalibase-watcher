package io.github.excalibase.watcher.schema;

import java.util.List;

/**
 * A snapshot of a table's schema at a specific point in the CDC stream.
 *
 * @param position  LSN (Postgres) or binlog position (MySQL) where this schema was captured
 * @param schema    database schema name (e.g. "public", "shop")
 * @param table     table name
 * @param columns   ordered list of column definitions
 * @param timestamp epoch millis when this entry was recorded
 */
public record SchemaHistoryEntry(
        String position,
        String schema,
        String table,
        List<ColumnDef> columns,
        long timestamp
) {
    public record ColumnDef(String name, String typeName, int typeOid) {}
}
