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
package io.github.excalibase.watcher.mysql;

import io.github.excalibase.watcher.CDCService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

/**
 * Spring service that bootstraps {@link MysqlBinlogListener} and wires it to
 * {@link CDCService}.
 *
 * <p>Required configuration:</p>
 * <pre>{@code
 * spring.datasource.url=jdbc:mysql://localhost:3306/mydb
 * spring.datasource.username=user
 * spring.datasource.password=secret
 *
 * app.cdc.enabled=true
 *
 * # Optional
 * app.cdc.mysql.tables=users,orders   # empty = watch all tables
 * }</pre>
 *
 * <p>MySQL server must have binlog enabled:</p>
 * <pre>{@code
 * log_bin          = ON
 * binlog_format    = ROW
 * binlog_row_image = FULL
 * server_id        = 1
 * }</pre>
 */
@Service
public class MysqlBinlogStartup {

    private static final Logger log = LoggerFactory.getLogger(MysqlBinlogStartup.class);

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

    @Value("${app.cdc.mysql.tables:}")
    private String tablesConfig;

    private MysqlBinlogListener binlogListener;

    @PostConstruct
    public void start() {
        if (!cdcEnabled) {
            log.info("CDC is disabled. Skipping MySQL binlog listener initialisation.");
            return;
        }

        List<String> tables = tablesConfig.isBlank()
                ? List.of()
                : Arrays.stream(tablesConfig.split(","))
                        .map(String::trim)
                        .filter(s -> !s.isBlank())
                        .toList();

        binlogListener = new MysqlBinlogListener.Builder()
                .jdbcUrl(jdbcUrl)
                .credentials(username, password)
                .tables(tables)
                .eventHandler(cdcService::handleCDCEvent)
                .build();

        try {
            binlogListener.start();
            cdcService.markRunning();
            log.info("MySQL binlog listener started and wired to CDCService");
        } catch (Exception e) {
            log.error("Failed to start MySQL binlog listener", e);
        }
    }

    @PreDestroy
    public void stop() {
        if (binlogListener != null) {
            binlogListener.stop();
        }
    }
}
