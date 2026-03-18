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

import io.github.excalibase.watcher.constant.WatcherLogConstant;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Core CDC streaming service that manages per-table reactive event streams.
 * <p>
 * This service is database-agnostic. It receives {@link CDCEvent} objects via
 * {@link #handleCDCEvent(CDCEvent)} (called by a database-specific listener such as
 * {@code PostgresCDCStartup}) and routes them to the appropriate per-table
 * {@link Flux} streams that consumers can subscribe to.
 * </p>
 *
 * <p>Configuration:</p>
 * <pre>{@code
 * app.cdc.enabled=true   # enable/disable CDC streaming (default: true)
 * }</pre>
 */
@Service
public class CDCService {

    private static final Logger log = LoggerFactory.getLogger(CDCService.class);

    @Value("${app.cdc.enabled:true}")
    private boolean cdcEnabled;

    private volatile boolean listenerRunning = false;

    private final Map<String, Sinks.Many<CDCEvent>> tableSinks = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> tableSubscriberCounts = new ConcurrentHashMap<>();
    private final Sinks.Many<CDCEvent> globalSink = Sinks.many().multicast().onBackpressureBuffer();

    @PreDestroy
    public void shutdown() {
        tableSinks.values().forEach(Sinks.Many::tryEmitComplete);
        globalSink.tryEmitComplete();
        tableSubscriberCounts.clear();
        listenerRunning = false;
        log.info(WatcherLogConstant.CDC_SERVICE_STOPPED);
    }

    /**
     * Called by a database-specific listener startup bean to signal that CDC is running.
     */
    public void markRunning() {
        this.listenerRunning = true;
    }

    /**
     * Returns a reactive stream of CDC events for the given table name.
     * Supports multiple concurrent subscribers per table.
     *
     * @param tableName the table to watch
     * @return {@link Flux} of {@link CDCEvent} for that table
     */
    public Flux<CDCEvent> getTableEventStream(String tableName) {
        return getOrCreateTableSink(tableName).asFlux()
                .doOnSubscribe(s -> {
                    tableSubscriberCounts.computeIfAbsent(tableName, k -> new AtomicInteger(0)).incrementAndGet();
                    log.info(WatcherLogConstant.TABLE_STREAM_CLIENT_SUBSCRIBED,
                            tableName, tableSubscriberCounts.get(tableName).get());
                })
                .doOnCancel(() -> {
                    int currentCount = tableSubscriberCounts.get(tableName).decrementAndGet();
                    log.info(WatcherLogConstant.TABLE_STREAM_CLIENT_UNSUBSCRIBED, tableName, currentCount);
                    cleanupTableSinkIfNoSubscribers(tableName);
                })
                .doOnComplete(() -> log.warn(WatcherLogConstant.TABLE_STREAM_COMPLETED, tableName))
                .doOnError(t -> log.error(WatcherLogConstant.TABLE_STREAM_ERROR, tableName, t));
    }

    /**
     * Returns a reactive stream of ALL CDC events across all tables.
     * Intended for external publishers (e.g. NATS, Kafka) that need a single
     * subscription point rather than per-table subscriptions.
     */
    public Flux<CDCEvent> getAllEventsFlux() {
        return globalSink.asFlux();
    }

    /**
     * Route an incoming CDC event to the appropriate table sink.
     * Called by the database-specific listener (e.g. {@code PostgresCDCStartup}).
     */
    public void handleCDCEvent(CDCEvent event) {
        try {
            log.debug(WatcherLogConstant.PROCESSING_CDC_EVENT,
                    event.getType(), event.getTable(),
                    event.getData() != null ? event.getData().substring(0, Math.min(100, event.getData().length())) : "null");

            if (event.getTable() != null &&
                    (event.getType() == CDCEvent.Type.INSERT ||
                            event.getType() == CDCEvent.Type.UPDATE ||
                            event.getType() == CDCEvent.Type.DELETE)) {

                String tableName = event.getTable();
                Sinks.Many<CDCEvent> sink = getOrCreateTableSink(tableName);

                if (event.getData() != null && !event.getData().trim().isEmpty()) {
                    if (!event.getData().startsWith("{") || !event.getData().endsWith("}")) {
                        log.warn(WatcherLogConstant.INVALID_JSON_CDC, tableName, event.getData());
                        return;
                    }
                }

                globalSink.tryEmitNext(event);
                Sinks.EmitResult result = sink.tryEmitNext(event);
                if (result.isFailure()) {
                    log.warn(WatcherLogConstant.FAILED_EMIT_CDC, tableName, result);

                    if (result == Sinks.EmitResult.FAIL_TERMINATED) {
                        log.info(WatcherLogConstant.RECREATING_SINK, tableName);
                        sink = getOrCreateTableSink(tableName);
                        result = sink.tryEmitNext(event);
                        if (result.isFailure()) {
                            log.error(WatcherLogConstant.FAILED_EMIT_AFTER_RECREATION, tableName, result);
                        } else {
                            log.info(WatcherLogConstant.SUCCESS_EMIT_AFTER_RECREATION, tableName);
                        }
                    }
                } else {
                    log.debug(WatcherLogConstant.SUCCESS_EMIT_CDC, tableName);
                }
            }
        } catch (Exception e) {
            log.error(WatcherLogConstant.UNEXPECTED_ERROR_CDC, e);
        }
    }

    private synchronized void cleanupTableSinkIfNoSubscribers(String tableName) {
        AtomicInteger subscriberCount = tableSubscriberCounts.get(tableName);
        Sinks.Many<CDCEvent> sink = tableSinks.get(tableName);

        if (subscriberCount != null && sink != null) {
            int currentCount = subscriberCount.get();
            log.debug(WatcherLogConstant.CLEANUP_CHECK, tableName, currentCount);

            if (currentCount <= 0) {
                log.info(WatcherLogConstant.NO_MORE_SUBSCRIBERS, tableName);
                sink.tryEmitComplete();
                tableSinks.remove(tableName);
                tableSubscriberCounts.remove(tableName);
            } else {
                log.debug(WatcherLogConstant.STILL_HAS_SUBSCRIBERS, tableName, currentCount);
            }
        } else {
            log.debug(WatcherLogConstant.NO_SINK_FOUND, tableName);
        }
    }

    private synchronized Sinks.Many<CDCEvent> getOrCreateTableSink(String tableName) {
        Sinks.Many<CDCEvent> sink = tableSinks.get(tableName);

        if (sink == null) {
            sink = Sinks.many().multicast().onBackpressureBuffer();
            tableSinks.put(tableName, sink);
            log.debug(WatcherLogConstant.CREATED_NEW_SINK, tableName);
        } else {
            Boolean isTerminated = sink.scan(reactor.core.Scannable.Attr.TERMINATED);
            if (isTerminated == Boolean.TRUE) {
                log.warn(WatcherLogConstant.SINK_TERMINATED_RECREATING, tableName);
                sink = Sinks.many().multicast().onBackpressureBuffer();
                tableSinks.put(tableName, sink);
                log.info(WatcherLogConstant.SUCCESS_RECREATED_SINK, tableName);
            } else {
                log.debug(WatcherLogConstant.SINK_ACTIVE, tableName, sink.currentSubscriberCount());
            }
        }

        return sink;
    }

    /** @return {@code true} if the listener has been started via {@link #markRunning()} */
    public boolean isRunning() {
        return listenerRunning;
    }

    /** @return {@code true} if CDC is enabled in configuration */
    public boolean isCdcEnabled() {
        return cdcEnabled;
    }

    /** @return total number of active subscriptions across all tables */
    public int getActiveSubscriptionCount() {
        return tableSubscriberCounts.values().stream()
                .mapToInt(AtomicInteger::get)
                .sum();
    }

    /** @return number of active subscriptions for a specific table */
    public int getActiveSubscriptionCount(String tableName) {
        AtomicInteger count = tableSubscriberCounts.get(tableName);
        return count != null ? count.get() : 0;
    }
}
