package io.github.excalibase.watcher.schema;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * File-based schema history store. Each table's history is stored in a separate JSON file
 * under the configured directory: {@code {dir}/{schema}.{table}.json}.
 *
 * <p>Thread-safe: uses ConcurrentHashMap for in-memory cache and synchronizes file writes.</p>
 */
public class FileSchemaHistoryStore implements SchemaHistoryStore {

    private static final Logger log = LoggerFactory.getLogger(FileSchemaHistoryStore.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final Path directory;
    private final ConcurrentHashMap<String, List<SchemaHistoryEntry>> cache = new ConcurrentHashMap<>();

    public FileSchemaHistoryStore(Path directory) throws IOException {
        this.directory = directory;
        Files.createDirectories(directory);
    }

    @Override
    public void save(SchemaHistoryEntry entry) throws IOException {
        String key = tableKey(entry.schema(), entry.table());
        List<SchemaHistoryEntry> history = cache.computeIfAbsent(key, k -> loadFromFile(k));

        // Don't save duplicate if same position already exists
        if (history.stream().anyMatch(e -> e.position().equals(entry.position()))) {
            return;
        }

        synchronized (history) {
            history.add(entry);
            writeToFile(key, history);
        }
        log.debug("Saved schema history: {}.{} at {}", entry.schema(), entry.table(), entry.position());
    }

    @Override
    public Optional<SchemaHistoryEntry> getSchemaAt(String schema, String table, String position) throws IOException {
        List<SchemaHistoryEntry> history = getHistory(schema, table);
        // Return the last entry at or before the given position (string comparison works for LSN/binlog format)
        SchemaHistoryEntry result = null;
        for (SchemaHistoryEntry e : history) {
            if (e.position().compareTo(position) <= 0) {
                result = e;
            } else {
                break;
            }
        }
        return Optional.ofNullable(result);
    }

    @Override
    public Optional<SchemaHistoryEntry> getLatestSchema(String schema, String table) throws IOException {
        List<SchemaHistoryEntry> history = getHistory(schema, table);
        return history.isEmpty() ? Optional.empty() : Optional.of(history.get(history.size() - 1));
    }

    @Override
    public List<SchemaHistoryEntry> getHistory(String schema, String table) throws IOException {
        String key = tableKey(schema, table);
        return Collections.unmodifiableList(cache.computeIfAbsent(key, k -> loadFromFile(k)));
    }

    private String tableKey(String schema, String table) {
        return schema.replace(".", "_") + "." + table.replace(".", "_");
    }

    private Path filePath(String key) {
        return directory.resolve(key + ".json");
    }

    private List<SchemaHistoryEntry> loadFromFile(String key) {
        Path file = filePath(key);
        if (!Files.exists(file)) {
            return new ArrayList<>();
        }
        try {
            return new ArrayList<>(mapper.readValue(file.toFile(), new TypeReference<List<SchemaHistoryEntry>>() {}));
        } catch (IOException e) {
            log.warn("Failed to load schema history from {}: {}", file, e.getMessage());
            return new ArrayList<>();
        }
    }

    private void writeToFile(String key, List<SchemaHistoryEntry> history) throws IOException {
        Path file = filePath(key);
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        mapper.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), history);
        Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }
}
