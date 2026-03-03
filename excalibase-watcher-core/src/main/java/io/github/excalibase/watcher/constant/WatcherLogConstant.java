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
package io.github.excalibase.watcher.constant;

/**
 * Log message constants for the CDC watcher service.
 */
public class WatcherLogConstant {

    private WatcherLogConstant() {
    }

    // CDC Service lifecycle
    public static final String CDC_SERVICE_STARTED = "CDC Service started successfully";
    public static final String CDC_SERVICE_FAILED = "Failed to start CDC Service";
    public static final String CDC_SERVICE_STOPPED = "CDC Service stopped";

    // Table stream subscription tracking
    public static final String TABLE_STREAM_CLIENT_SUBSCRIBED = "📡 Table stream: Client subscribed to table events: {} (count: {})";
    public static final String TABLE_STREAM_CLIENT_UNSUBSCRIBED = "📡 Table stream: Client unsubscribed from table events: {} (count: {})";
    public static final String TABLE_STREAM_COMPLETED = "📡 Table stream: Stream completed unexpectedly for table: {}";
    public static final String TABLE_STREAM_ERROR = "📡 Table stream: Error occurred while streaming table events for {}";

    // CDC event processing
    public static final String PROCESSING_CDC_EVENT = "Processing CDC event: type={}, table={}, data={}";
    public static final String INVALID_JSON_CDC = "Invalid JSON format for CDC event, table {}: {}";
    public static final String ERROR_VALIDATING_CDC = "Error validating CDC event data for table {}: {}";
    public static final String FAILED_EMIT_CDC = "Failed to emit CDC event for table {}: {}";
    public static final String SUCCESS_EMIT_CDC = "Successfully emitted CDC event for table: {}";
    public static final String UNEXPECTED_ERROR_CDC = "Unexpected error handling CDC event: ";

    // Sink management
    public static final String CREATED_NEW_SINK = "Created new CDC sink for table: {}";
    public static final String SINK_TERMINATED_RECREATING = "Table sink for {} is terminated, recreating it";
    public static final String SUCCESS_RECREATED_SINK = "Successfully recreated table sink for: {}";
    public static final String SINK_ACTIVE = "Table sink for {} is active, current subscriber count: {}";
    public static final String RECREATING_SINK = "Recreating terminated sink for table: {}";
    public static final String FAILED_EMIT_AFTER_RECREATION = "Failed to emit CDC event after sink recreation for table {}: {}";
    public static final String SUCCESS_EMIT_AFTER_RECREATION = "Successfully emitted CDC event after sink recreation for table: {}";

    // Cleanup
    public static final String CLEANUP_CHECK = "📡 Table stream: Cleanup check for table {}, current subscribers: {}";
    public static final String NO_MORE_SUBSCRIBERS = "📡 Table stream: No more subscribers for table {}, removing sink";
    public static final String STILL_HAS_SUBSCRIBERS = "📡 Table stream: Table {} still has {} subscribers, keeping sink";
    public static final String NO_SINK_FOUND = "📡 Table stream: No sink or subscriber count found for table {} during cleanup";
}
