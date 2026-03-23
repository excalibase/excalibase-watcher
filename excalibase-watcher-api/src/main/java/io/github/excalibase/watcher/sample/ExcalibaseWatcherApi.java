package io.github.excalibase.watcher.sample;

import io.github.excalibase.watcher.CDCEvent;
import io.github.excalibase.watcher.CDCService;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Component;

/**
 * Excalibase Watcher API server.
 *
 * <p>Connects to one or more databases, captures every change (INSERT, UPDATE, DELETE)
 * via CDC (WAL / binlog), and publishes each event to NATS JetStream so downstream
 * consumers can react in real time.</p>
 *
 * <p>Configure in {@code application.properties} or environment variables:</p>
 * <pre>
 * # ── Postgres ──────────────────────────────────────────────────────────────
 * app.cdc.postgres.enabled=true
 * app.cdc.postgres.url=jdbc:postgresql://localhost:5432/mydb
 * app.cdc.postgres.username=postgres
 * app.cdc.postgres.password=secret
 * app.cdc.slot-name=watcher_slot
 * app.cdc.publication-name=watcher_pub
 *
 * # ── MySQL ─────────────────────────────────────────────────────────────────
 * app.cdc.mysql.enabled=true
 * app.cdc.mysql.url=jdbc:mysql://localhost:3306/mydb
 * app.cdc.mysql.username=root
 * app.cdc.mysql.password=secret
 *
 * # ── NATS JetStream ────────────────────────────────────────────────────────
 * app.nats.url=nats://localhost:4222
 * app.nats.stream-name=CDC
 * app.nats.subject-prefix=cdc
 * </pre>
 *
 * <p>Events are published to {@code cdc.{schema}.{table}} subjects.
 * For example, an insert into {@code public.orders} appears on {@code cdc.public.orders}.</p>
 */
@SpringBootApplication
public class ExcalibaseWatcherApi {

    public static void main(String[] args) {
        SpringApplication.run(ExcalibaseWatcherApi.class, args);
    }

    /**
     * Logs every CDC event to the console so you can see the server is working
     * even without a NATS consumer attached.
     */
    @Component
    static class CdcEventLogger {

        private static final Logger log = LoggerFactory.getLogger(CdcEventLogger.class);

        private final CDCService cdcService;

        CdcEventLogger(CDCService cdcService) {
            this.cdcService = cdcService;
        }

        @PostConstruct
        void subscribe() {
            cdcService.getAllEventsFlux()
                    .filter(e -> e.getType() != CDCEvent.Type.BEGIN && e.getType() != CDCEvent.Type.COMMIT)
                    .subscribe(event ->
                            log.info("[CDC] {} {}.{} lsn={} data={}",
                                    event.getType(),
                                    event.getSchema(),
                                    event.getTable(),
                                    event.getLsn(),
                                    event.getData()));
        }
    }
}
