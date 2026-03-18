package io.github.excalibase.watcher.nats;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for NATS JetStream publisher.
 *
 * <pre>
 * app:
 *   nats:
 *     url: nats://localhost:4222
 *     stream-name: CDC
 *     subject-prefix: cdc
 *     max-age-minutes: 5
 *     storage: memory   # memory | file
 *     enabled: true
 * </pre>
 */
@ConfigurationProperties(prefix = "app.nats")
public class NatsProperties {

    /** NATS server URL. */
    private String url = "nats://localhost:4222";

    /** JetStream stream name. */
    private String streamName = "CDC";

    /**
     * Subject prefix. Events are published to {prefix}.{schema}.{table}.
     * e.g. "cdc.public.customer"
     */
    private String subjectPrefix = "cdc";

    /** Retention duration for events in the stream (minutes). */
    private long maxAgeMinutes = 5;

    /** Storage type: "memory" (default) or "file". */
    private String storage = "memory";

    /** Whether to publish CDC events to NATS. */
    private boolean enabled = true;

    public String getUrl() { return url; }
    public void setUrl(String url) { this.url = url; }

    public String getStreamName() { return streamName; }
    public void setStreamName(String streamName) { this.streamName = streamName; }

    public String getSubjectPrefix() { return subjectPrefix; }
    public void setSubjectPrefix(String subjectPrefix) { this.subjectPrefix = subjectPrefix; }

    public long getMaxAgeMinutes() { return maxAgeMinutes; }
    public void setMaxAgeMinutes(long maxAgeMinutes) { this.maxAgeMinutes = maxAgeMinutes; }

    public String getStorage() { return storage; }
    public void setStorage(String storage) { this.storage = storage; }

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
}
