package io.github.excalibase.watcher.nats;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.excalibase.watcher.CDCEvent;
import io.github.excalibase.watcher.CDCService;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;

import java.time.Duration;

/**
 * Bridges {@link CDCService} to NATS JetStream.
 *
 * <p>Subscribes to all CDC events via {@link CDCService#getAllEventsFlux()} and
 * publishes each event to the JetStream subject {@code {prefix}.{schema}.{table}},
 * e.g. {@code cdc.public.customer}.</p>
 *
 * <p>The JetStream stream is created (or updated) at startup with the configured
 * retention window. Events are stored in memory by default (5-minute TTL), so
 * consumers that reconnect within the window can replay missed events via their
 * {@code last_seen_seq}.</p>
 */
public class NatsEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(NatsEventPublisher.class);

    private final CDCService cdcService;
    private final NatsProperties props;
    private final ObjectMapper objectMapper;

    private Connection connection;
    private JetStream jetStream;
    private Disposable subscription;

    public NatsEventPublisher(CDCService cdcService, NatsProperties props, ObjectMapper objectMapper) {
        this.cdcService = cdcService;
        this.props = props;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void start() throws Exception {
        if (!props.isEnabled()) {
            log.info("NATS publisher disabled (app.nats.enabled=false)");
            return;
        }

        Options options = Options.builder()
                .server(props.getUrl())
                .reconnectWait(Duration.ofSeconds(2))
                .maxReconnects(-1)          // retry forever
                .connectionListener((conn, type) ->
                        log.info("NATS connection event: {}", type))
                .build();

        connection = Nats.connect(options);
        jetStream = connection.jetStream();

        ensureStream();

        // Subscribe to all CDC events and forward to NATS
        subscription = cdcService.getAllEventsFlux()
                .filter(e -> e.getType() == CDCEvent.Type.INSERT
                          || e.getType() == CDCEvent.Type.UPDATE
                          || e.getType() == CDCEvent.Type.DELETE)
                .subscribe(
                        this::publish,
                        err -> log.error("Error in CDC→NATS pipeline", err)
                );

        log.info("NATS JetStream publisher started — stream='{}', url='{}'",
                props.getStreamName(), props.getUrl());
    }

    @PreDestroy
    public void stop() {
        if (subscription != null && !subscription.isDisposed()) {
            subscription.dispose();
        }
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("NATS JetStream publisher stopped");
    }

    // ─── Private helpers ─────────────────────────────────────────────────────

    private void publish(CDCEvent event) {
        try {
            String subject = buildSubject(event);
            byte[] payload = objectMapper.writeValueAsBytes(event);
            jetStream.publish(subject, payload);
            log.debug("Published CDC event to NATS subject '{}': type={}", subject, event.getType());
        } catch (Exception e) {
            log.error("Failed to publish CDC event to NATS: table={}, type={}",
                    event.getTable(), event.getType(), e);
        }
    }

    private String buildSubject(CDCEvent event) {
        String schema = event.getSchema() != null ? event.getSchema() : "default";
        return props.getSubjectPrefix() + "." + schema + "." + event.getTable();
    }

    /**
     * Creates the JetStream stream if it does not exist, or updates its config if it does.
     */
    private void ensureStream() throws Exception {
        JetStreamManagement jsm = connection.jetStreamManagement();

        StorageType storageType = "file".equalsIgnoreCase(props.getStorage())
                ? StorageType.File
                : StorageType.Memory;

        StreamConfiguration config = StreamConfiguration.builder()
                .name(props.getStreamName())
                .subjects(props.getSubjectPrefix() + ".>")   // wildcard: cdc.>
                .maxAge(Duration.ofMinutes(props.getMaxAgeMinutes()))
                .storageType(storageType)
                .retentionPolicy(RetentionPolicy.Limits)
                .build();

        try {
            jsm.getStreamInfo(props.getStreamName());
            jsm.updateStream(config);
            log.info("NATS JetStream stream '{}' updated", props.getStreamName());
        } catch (io.nats.client.JetStreamApiException e) {
            if (e.getErrorCode() == 404) {
                jsm.addStream(config);
                log.info("NATS JetStream stream '{}' created (storage={}, maxAge={}min)",
                        props.getStreamName(), storageType, props.getMaxAgeMinutes());
            } else {
                throw e;
            }
        }
    }
}
