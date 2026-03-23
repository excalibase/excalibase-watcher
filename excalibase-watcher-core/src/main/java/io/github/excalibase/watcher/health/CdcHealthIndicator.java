package io.github.excalibase.watcher.health;

import io.github.excalibase.watcher.CDCService;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration that registers a CDC health indicator when Spring Boot Actuator
 * is on the classpath.
 */
@Configuration
@ConditionalOnClass(HealthIndicator.class)
public class CdcHealthIndicator {

    @Bean
    HealthIndicator cdcHealth(CDCService cdcService) {
        return () -> {
            if (cdcService.isRunning()) {
                return Health.up()
                        .withDetail("cdc.enabled", cdcService.isCdcEnabled())
                        .withDetail("cdc.subscriptions", cdcService.getActiveSubscriptionCount())
                        .build();
            }
            return Health.down()
                    .withDetail("cdc.enabled", cdcService.isCdcEnabled())
                    .withDetail("reason", cdcService.isCdcEnabled()
                            ? "CDC listener has not started yet"
                            : "CDC is disabled in configuration")
                    .build();
        };
    }
}
