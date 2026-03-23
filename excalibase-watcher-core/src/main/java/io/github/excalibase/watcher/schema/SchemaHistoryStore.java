package io.github.excalibase.watcher.schema;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Persistent store for table schema snapshots captured during CDC.
 * Each entry records the column layout of a table at a specific LSN/binlog position.
 * Consumers can look up the schema that was active at any position.
 */
public interface SchemaHistoryStore {

    /**
     * Store a schema snapshot.
     */
    void save(SchemaHistoryEntry entry) throws IOException;

    /**
     * Get the most recent schema for a given table at or before the specified position.
     * Returns empty if no schema has been recorded for this table.
     */
    Optional<SchemaHistoryEntry> getSchemaAt(String schema, String table, String position) throws IOException;

    /**
     * Get the latest (current) schema for a given table.
     */
    Optional<SchemaHistoryEntry> getLatestSchema(String schema, String table) throws IOException;

    /**
     * Get all schema history entries for a given table, ordered by position.
     */
    List<SchemaHistoryEntry> getHistory(String schema, String table) throws IOException;
}
