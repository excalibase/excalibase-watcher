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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

class CDCServiceTest {

    private CDCService cdcService;

    @BeforeEach
    void setUp() {
        cdcService = new CDCService();
    }

    @AfterEach
    void tearDown() {
        cdcService.shutdown();
    }

    @Test
    void shouldCreateSeparateEventStreamsForDifferentTables() {
        Flux<CDCEvent> customerStream = cdcService.getTableEventStream("customer");
        Flux<CDCEvent> orderStream = cdcService.getTableEventStream("orders");

        assertThat(customerStream).isNotNull();
        assertThat(orderStream).isNotNull();
        assertThat(customerStream).isNotSameAs(orderStream);
    }

    @Test
    void shouldReturnNonNullStreamForSameTable() {
        Flux<CDCEvent> stream1 = cdcService.getTableEventStream("customer");
        Flux<CDCEvent> stream2 = cdcService.getTableEventStream("customer");

        assertThat(stream1).isNotNull();
        assertThat(stream2).isNotNull();
    }

    @Test
    void shouldReturnNotRunningWhenListenerNotStarted() {
        assertThat(cdcService.isRunning()).isFalse();
    }

    @Test
    void shouldReturnRunningAfterMarkRunning() {
        cdcService.markRunning();
        assertThat(cdcService.isRunning()).isTrue();
    }

    @Test
    void shouldRouteEventsToCorrectTableStreams() throws InterruptedException {
        Flux<CDCEvent> customerStream = cdcService.getTableEventStream("customer");
        Flux<CDCEvent> orderStream = cdcService.getTableEventStream("orders");

        List<CDCEvent> customerEvents = new ArrayList<>();
        List<CDCEvent> orderEvents = new ArrayList<>();

        customerStream.subscribe(customerEvents::add);
        orderStream.subscribe(orderEvents::add);

        CDCEvent customerEvent = new CDCEvent(
                CDCEvent.Type.INSERT, "public", "customer",
                "{\"customer_id\": 1, \"name\": \"Test Customer\"}", "INSERT", null);

        CDCEvent orderEvent = new CDCEvent(
                CDCEvent.Type.UPDATE, "public", "orders",
                "{\"order_id\": 100, \"status\": \"shipped\"}", "UPDATE", null);

        cdcService.handleCDCEvent(customerEvent);
        cdcService.handleCDCEvent(orderEvent);

        Thread.sleep(100);

        assertThat(customerEvents).hasSize(1);
        assertThat(customerEvents.get(0).getTable()).isEqualTo("customer");
        assertThat(customerEvents.get(0).getType()).isEqualTo(CDCEvent.Type.INSERT);

        assertThat(orderEvents).hasSize(1);
        assertThat(orderEvents.get(0).getTable()).isEqualTo("orders");
        assertThat(orderEvents.get(0).getType()).isEqualTo(CDCEvent.Type.UPDATE);
    }

    @Test
    void shouldHandleBeginAndCommitEventsWithoutErrors() {
        CDCEvent beginEvent = new CDCEvent(CDCEvent.Type.BEGIN, null, null, null, "BEGIN", null);
        CDCEvent commitEvent = new CDCEvent(CDCEvent.Type.COMMIT, null, null, null, "COMMIT", null);

        assertThatNoException().isThrownBy(() -> {
            cdcService.handleCDCEvent(beginEvent);
            cdcService.handleCDCEvent(commitEvent);
        });
    }

    @Test
    void shouldStartWithZeroActiveSubscriptions() {
        assertThat(cdcService.getActiveSubscriptionCount()).isZero();
    }

    @Test
    void shouldTrackActiveSubscriptionCount() {
        List<Disposable> subscriptions = new ArrayList<>();
        subscriptions.add(cdcService.getTableEventStream("table1").subscribe());
        subscriptions.add(cdcService.getTableEventStream("table2").subscribe());
        subscriptions.add(cdcService.getTableEventStream("table3").subscribe());

        assertThat(cdcService.getActiveSubscriptionCount()).isEqualTo(3);

        subscriptions.add(cdcService.getTableEventStream("table1").subscribe());
        subscriptions.add(cdcService.getTableEventStream("table2").subscribe());

        assertThat(cdcService.getActiveSubscriptionCount()).isEqualTo(5);

        subscriptions.forEach(Disposable::dispose);
    }

    @Test
    void shouldTrackPerTableSubscriptionCount() {
        List<Disposable> subscriptions = new ArrayList<>();
        subscriptions.add(cdcService.getTableEventStream("customer").subscribe());
        subscriptions.add(cdcService.getTableEventStream("customer").subscribe());

        assertThat(cdcService.getActiveSubscriptionCount("customer")).isEqualTo(2);
        assertThat(cdcService.getActiveSubscriptionCount("orders")).isZero();

        subscriptions.forEach(Disposable::dispose);
    }

    @Test
    void shouldHandleConcurrentTableStreamRequestsSafely() throws InterruptedException {
        List<String> tables = List.of("table1", "table2", "table3", "table4", "table5");
        List<Flux<CDCEvent>> streams = Collections.synchronizedList(new ArrayList<>());
        List<Disposable> subscriptions = Collections.synchronizedList(new ArrayList<>());

        List<Thread> threads = new ArrayList<>();
        for (String tableName : tables) {
            Thread t = new Thread(() -> {
                for (int i = 0; i < 10; i++) {
                    Flux<CDCEvent> stream = cdcService.getTableEventStream(tableName);
                    streams.add(stream);
                    subscriptions.add(stream.subscribe());
                }
            });
            threads.add(t);
            t.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        assertThat(streams).hasSize(50);
        assertThat(subscriptions).hasSize(50);
        assertThat(cdcService.getActiveSubscriptionCount()).isEqualTo(50);

        subscriptions.forEach(Disposable::dispose);
    }
}
