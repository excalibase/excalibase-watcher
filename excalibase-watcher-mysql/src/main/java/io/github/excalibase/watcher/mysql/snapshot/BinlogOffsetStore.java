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

import java.io.IOException;
import java.util.Optional;

/**
 * Persists the MySQL binlog position so the listener can resume after a restart
 * without missing events.
 *
 * <p>Implement this interface to store the offset in any durable medium
 * (file, database, ZooKeeper, etc.). A file-based implementation is provided
 * by {@link FileBinlogOffsetStore}.</p>
 */
public interface BinlogOffsetStore {

    /**
     * Load the last saved binlog position.
     *
     * @return the saved position, or empty if no position has been saved yet
     */
    Optional<BinlogPosition> load() throws IOException;

    /**
     * Persist the current binlog position.
     *
     * @param position the position to save
     */
    void save(BinlogPosition position) throws IOException;
}
