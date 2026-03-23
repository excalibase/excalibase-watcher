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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

/**
 * File-based {@link BinlogOffsetStore} that persists the binlog position as a
 * single line in a plain text file: {@code <filename>:<position>}.
 *
 * <p>Configure via:</p>
 * <pre>{@code
 * app.cdc.mysql.offset-store.file=/var/lib/myapp/binlog.offset
 * }</pre>
 */
public class FileBinlogOffsetStore implements BinlogOffsetStore {

    private static final Logger log = LoggerFactory.getLogger(FileBinlogOffsetStore.class);

    private final Path filePath;

    public FileBinlogOffsetStore(Path filePath) {
        this.filePath = filePath;
    }

    @Override
    public Optional<BinlogPosition> load() throws IOException {
        if (!Files.exists(filePath)) {
            return Optional.empty();
        }
        String content = Files.readString(filePath).trim();
        if (content.isBlank()) {
            return Optional.empty();
        }
        int colon = content.lastIndexOf(':');
        if (colon < 0) {
            log.warn("Invalid binlog offset file content: {}", content);
            return Optional.empty();
        }
        String file = content.substring(0, colon);
        long position = Long.parseLong(content.substring(colon + 1));
        log.info("Loaded binlog offset: {}:{}", file, position);
        return Optional.of(new BinlogPosition(file, position));
    }

    @Override
    public void save(BinlogPosition position) throws IOException {
        String content = position.file() + ":" + position.position();
        Files.writeString(filePath, content, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }
}
