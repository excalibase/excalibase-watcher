package io.github.excalibase.watcher.sample;

import io.github.excalibase.watcher.CDCService;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Component;

@SpringBootApplication
public class ExcalibaseSampleApp {

    public static void main(String[] args) {
        SpringApplication.run(ExcalibaseSampleApp.class, args);
    }

    @Component
    static class CdcEventLogger {

        private static final Logger log = LoggerFactory.getLogger(CdcEventLogger.class);

        private final CDCService cdcService;

        CdcEventLogger(CDCService cdcService) {
            this.cdcService = cdcService;
        }

        @PostConstruct
        void subscribe() {
            cdcService.getAllEventsFlux().subscribe(event ->
                    log.info("[CDC] type={} schema={} table={} lsn={} data={}",
                            event.getType(),
                            event.getSchema(),
                            event.getTable(),
                            event.getLsn(),
                            event.getData()));
        }
    }
}
