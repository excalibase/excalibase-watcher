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
package io.github.excalibase.watcher.postgres;

import io.github.excalibase.watcher.CDCService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Spring service that bootstraps the {@link PostgresCDCListener} and wires it to
 * {@link CDCService}.
 * <p>
 * On startup it creates a {@link PostgresCDCListener} using the datasource and CDC
 * configuration properties, then passes {@code cdcService::handleCDCEvent} as the
 * event callback so all WAL events flow into the core reactive streams.
 * </p>
 *
 * <p>Required configuration:</p>
 * <pre>{@code
 * spring.datasource.url=jdbc:postgresql://localhost:5432/mydb
 * spring.datasource.username=user
 * spring.datasource.password=secret
 *
 * app.cdc.enabled=true
 * app.cdc.slot-name=cdc_slot
 * app.cdc.publication-name=cdc_publication
 * app.cdc.create-slot-if-not-exists=true
 * app.cdc.create-publication-if-not-exists=true
 * }</pre>
 */
@Service
public class PostgresCDCStartup {

    private static final Logger log = LoggerFactory.getLogger(PostgresCDCStartup.class);

    @Autowired
    private CDCService cdcService;

    @Value("${spring.datasource.url}")
    private String jdbcUrl;

    @Value("${spring.datasource.username}")
    private String username;

    @Value("${spring.datasource.password}")
    private String password;

    @Value("${app.cdc.enabled:true}")
    private boolean cdcEnabled;

    @Value("${app.cdc.slot-name:cdc_slot}")
    private String slotName;

    @Value("${app.cdc.publication-name:cdc_publication}")
    private String publicationName;

    @Value("${app.cdc.create-slot-if-not-exists:true}")
    private boolean createSlotIfNotExists;

    @Value("${app.cdc.create-publication-if-not-exists:true}")
    private boolean createPublicationIfNotExists;

    private PostgresCDCListener cdcListener;

    @PostConstruct
    public void start() {
        if (!cdcEnabled) {
            log.info("CDC is disabled in configuration. Skipping PostgreSQL CDC initialization.");
            return;
        }

        cdcListener = new PostgresCDCListener.Builder()
                .jdbcUrl(jdbcUrl)
                .credentials(username, password)
                .slotName(slotName)
                .publicationName(publicationName)
                .createSlotIfNotExists(createSlotIfNotExists)
                .createPublicationIfNotExists(createPublicationIfNotExists)
                .eventHandler(cdcService::handleCDCEvent)
                .build();

        try {
            cdcListener.start();
            cdcService.markRunning();
            log.info("PostgreSQL CDC listener started and wired to CDCService");
        } catch (Exception e) {
            log.error("Failed to start PostgreSQL CDC listener", e);
        }
    }

    @PreDestroy
    public void stop() {
        if (cdcListener != null) {
            cdcListener.stop();
        }
    }
}
