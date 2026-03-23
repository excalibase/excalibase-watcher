package io.github.excalibase.watcher.nats;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.excalibase.watcher.CDCEvent;
import io.github.excalibase.watcher.CDCService;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for {@link NatsEventPublisher} using a real NATS container.
 * <p>
 * Tests the full CDC-to-NATS pipeline without Spring context:
 * {@code CDCService.handleCDCEvent()} → {@code NatsEventPublisher} → NATS JetStream.
 * </p>
 * <p>Requires Docker to be running.</p>
 */
@Testcontainers
class NatsEventPublisherIntegrationTest {

    @Container
    static GenericContainer<?> nats = new GenericContainer<>("nats:2.10")
            .withCommand("-js")
            .withExposedPorts(4222);

    private CDCService cdcService;
    private NatsEventPublisher natsPublisher;
    private Connection consumerConnection;

    @BeforeEach
    void setUp() throws Exception {
        cdcService = new CDCService();

        NatsProperties props = new NatsProperties();
        props.setUrl("nats://" + nats.getHost() + ":" + nats.getMappedPort(4222));

        natsPublisher = new NatsEventPublisher(cdcService, props, new ObjectMapper(), null);
        natsPublisher.start();

        consumerConnection = Nats.connect(props.getUrl());

        // Allow stream creation to settle
        Thread.sleep(200);
    }

    @AfterEach
    void tearDown() throws Exception {
        natsPublisher.stop();
        if (consumerConnection != null) {
            consumerConnection.close();
        }
        cdcService.shutdown();
    }

    // Subscribe to a subject, receiving only messages published after this call.
    private JetStreamSubscription subscribeNew(String subject) throws Exception {
        PushSubscribeOptions opts = PushSubscribeOptions.builder()
                .configuration(ConsumerConfiguration.builder()
                        .deliverPolicy(DeliverPolicy.New)
                        .build())
                .build();
        return consumerConnection.jetStream().subscribe(subject, opts);
    }

    @Test
    void shouldPublishInsertEventToNats() throws Exception {
        JetStreamSubscription sub = subscribeNew("cdc.public.orders");

        CDCEvent event = new CDCEvent(
                CDCEvent.Type.INSERT, "public", "orders",
                "{\"id\": 1, \"product\": \"widget\"}", "INSERT", "0/1000");

        cdcService.handleCDCEvent(event);

        Message msg = sub.nextMessage(5000);

        assertThat(msg).isNotNull();
        String payload = new String(msg.getData());
        assertThat(payload).contains("INSERT");
        assertThat(payload).contains("orders");
        assertThat(payload).contains("public");
    }

    @Test
    void shouldPublishUpdateEventToNats() throws Exception {
        JetStreamSubscription sub = subscribeNew("cdc.public.users");

        CDCEvent event = new CDCEvent(
                CDCEvent.Type.UPDATE, "public", "users",
                "{\"old\":{\"name\":\"Alice\"}, \"new\":{\"name\":\"Alicia\"}}", "UPDATE", "0/2000");

        cdcService.handleCDCEvent(event);

        Message msg = sub.nextMessage(5000);

        assertThat(msg).isNotNull();
        String payload = new String(msg.getData());
        assertThat(payload).contains("UPDATE");
        assertThat(payload).contains("users");
    }

    @Test
    void shouldPublishDeleteEventToNats() throws Exception {
        JetStreamSubscription sub = subscribeNew("cdc.public.products");

        CDCEvent event = new CDCEvent(
                CDCEvent.Type.DELETE, "public", "products",
                "{\"id\": 42}", "DELETE", "0/3000");

        cdcService.handleCDCEvent(event);

        Message msg = sub.nextMessage(5000);

        assertThat(msg).isNotNull();
        String payload = new String(msg.getData());
        assertThat(payload).contains("DELETE");
        assertThat(payload).contains("products");
    }

    @Test
    void shouldNotPublishBeginOrCommitEvents() throws Exception {
        JetStreamSubscription sub = subscribeNew("cdc.>");

        // BEGIN and COMMIT have null table — NatsEventPublisher filters them
        CDCEvent beginEvent = new CDCEvent(CDCEvent.Type.BEGIN, null, null, null, "BEGIN", "0/100");
        CDCEvent commitEvent = new CDCEvent(CDCEvent.Type.COMMIT, null, null, null, "COMMIT", "0/200");

        cdcService.handleCDCEvent(beginEvent);
        cdcService.handleCDCEvent(commitEvent);

        // Wait briefly — no message should arrive
        Message msg = sub.nextMessage(500);
        assertThat(msg).isNull();
    }

    @Test
    void shouldPublishToCorrectSubjectPerTable() throws Exception {
        JetStreamSubscription ordersOnly = subscribeNew("cdc.public.orders");
        JetStreamSubscription usersOnly = subscribeNew("cdc.public.users");

        CDCEvent ordersEvent = new CDCEvent(
                CDCEvent.Type.INSERT, "public", "orders",
                "{\"id\": 1}", "INSERT", "0/1000");
        cdcService.handleCDCEvent(ordersEvent);

        Message ordersMsg = ordersOnly.nextMessage(5000);
        Message usersMsg = usersOnly.nextMessage(500);

        assertThat(ordersMsg).isNotNull();
        assertThat(usersMsg).isNull();

        String payload = new String(ordersMsg.getData());
        assertThat(payload).contains("orders");
    }
}
