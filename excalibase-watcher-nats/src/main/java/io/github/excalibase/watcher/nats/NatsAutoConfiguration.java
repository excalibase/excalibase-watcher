package io.github.excalibase.watcher.nats;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.excalibase.watcher.CDCService;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Auto-configuration for the NATS JetStream CDC publisher.
 *
 * <p>Activated when {@code app.nats.enabled=true} (default) and
 * {@code app.nats.url} is set in application properties.</p>
 */
@AutoConfiguration
@EnableConfigurationProperties(NatsProperties.class)
@ConditionalOnProperty(prefix = "app.nats", name = "enabled", havingValue = "true", matchIfMissing = true)
public class NatsAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public NatsEventPublisher natsEventPublisher(CDCService cdcService,
                                                  NatsProperties natsProperties,
                                                  ObjectMapper objectMapper) {
        return new NatsEventPublisher(cdcService, natsProperties, objectMapper);
    }
}
