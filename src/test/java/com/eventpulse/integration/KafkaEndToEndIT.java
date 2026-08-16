package com.eventpulse.integration;

import com.eventpulse.config.ReloadableValidationRulesProvider;
import com.eventpulse.config.YamlValidationConfigLoader;
import com.eventpulse.dlq.DeadLetterPublisher;
import com.eventpulse.dlq.DeadLetterQueueSettings;
import com.eventpulse.error.ErrorCode;
import com.eventpulse.kafka.KafkaConsumerSettings;
import com.eventpulse.kafka.KafkaProducerSettings;
import com.eventpulse.kafka.KafkaRequestConsumer;
import com.eventpulse.kafka.KafkaRequestProducer;
import com.eventpulse.kafka.KafkaSecuritySettings;
import com.eventpulse.metrics.EventPulseMetrics;
import com.eventpulse.parser.RequestParser;
import com.eventpulse.processor.ProcessingStatus;
import com.eventpulse.processor.RequestProcessor;
import com.eventpulse.threading.RequestProcessingExecutor;
import com.eventpulse.threading.ThreadPoolSettings;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end tests that exercise the real Kafka client wire protocol - the production
 * {@link KafkaRequestProducer}/{@link KafkaRequestConsumer}/{@link DeadLetterPublisher}
 * constructors, talking to a real, ephemeral broker started by Testcontainers - rather than the
 * {@code MockConsumer}/{@code MockProducer} test doubles used everywhere else in this suite.
 *
 * <p>The Mock-based unit tests (see {@code KafkaRequestConsumerTest}) are fast and deterministic,
 * but a {@code MockProducer} never actually serializes a record onto the wire and a
 * {@code MockConsumer.rebalance(...)} never drives a real consumer group rebalance or commit.
 * These tests close that gap for the two outcomes that matter most in production: a request that
 * is successfully processed, and one that is dead-lettered - including reading the dead letter
 * envelope back off a real topic with an independent consumer, which confirms the JSON payload
 * {@link DeadLetterPublisher} writes is actually the payload a downstream consumer would read.
 *
 * <p>Requires a working Docker (or Docker-compatible) daemon. {@code disabledWithoutDocker = true}
 * means these tests are skipped - not failed - when no daemon is available, so `mvn verify` still
 * succeeds on a machine without Docker. Maven Failsafe (see pom.xml) is what actually runs
 * {@code *IT.java} classes; Surefire's default include patterns only match {@code *Test.java}, so
 * these never run as part of the fast `mvn test` unit-test loop.
 */
@Testcontainers(disabledWithoutDocker = true)
class KafkaEndToEndIT {

    // Static and shared across every test method in this class, so the one-time broker startup
    // cost is paid once per test run rather than once per test method.
    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:3.8.0");

    @TempDir
    Path tempDir;

    @Test
    @Timeout(60)
    void validRequestIsProcessedAndNeverReachesTheDeadLetterTopic() throws Exception {
        String topic = "it-requests-success";
        String dlqTopic = "it-requests-success-dlq";
        createTopics(topic, dlqTopic);

        // Produce first and close the producer (which flushes) before starting the consumer, so
        // the record is guaranteed to already be on the broker rather than racing the consumer.
        try (KafkaRequestProducer producer = new KafkaRequestProducer(
                new KafkaProducerSettings(KAFKA.getBootstrapServers(), topic, "it-producer"),
                plaintextSecurity())) {
            producer.send("LOGIN|user=alice");
        }

        RequestProcessor processor = new RequestProcessor(
                new RequestParser(),
                new ReloadableValidationRulesProvider(writeLoginRules(), new YamlValidationConfigLoader()));
        EventPulseMetrics metrics = EventPulseMetrics.inMemory();

        try (RequestProcessingExecutor executor = new RequestProcessingExecutor(new ThreadPoolSettings(1, 1, 10));
             DeadLetterPublisher deadLetterPublisher = new DeadLetterPublisher(
                     new DeadLetterQueueSettings(KAFKA.getBootstrapServers(), dlqTopic, "it-dlq-producer"),
                     metrics, plaintextSecurity());
             KafkaRequestConsumer consumer = new KafkaRequestConsumer(
                     new KafkaConsumerSettings(KAFKA.getBootstrapServers(), topic, "it-success-group", 200, 3),
                     executor, metrics, deadLetterPublisher, plaintextSecurity())) {

            Thread consumerThread = new Thread(() -> consumer.run(processor));
            consumerThread.setDaemon(true);
            consumerThread.start();

            awaitTrue(() -> metrics.requestCount(ProcessingStatus.SUCCESS) >= 1.0, 30_000);

            consumer.close();
            consumerThread.join(10_000);
        }

        assertEquals(1.0, metrics.requestCount(ProcessingStatus.SUCCESS));
        assertEquals(0.0, metrics.deadLetterCount(ErrorCode.VALIDATION_ERROR));
        assertTrue(collectFromTopic(dlqTopic, 1, 5_000).isEmpty(),
                "a successfully processed request should never reach the dead letter topic");
    }

    @Test
    @Timeout(60)
    void invalidRequestIsPublishedToTheRealDeadLetterTopic() throws Exception {
        String topic = "it-requests-invalid";
        String dlqTopic = "it-requests-invalid-dlq";
        createTopics(topic, dlqTopic);

        try (KafkaRequestProducer producer = new KafkaRequestProducer(
                new KafkaProducerSettings(KAFKA.getBootstrapServers(), topic, "it-producer"),
                plaintextSecurity())) {
            // "user" is present but blank, which fails the LOGIN rule's minLength: 1 below - a
            // VALIDATION_ERROR, which is deterministic and so is dead-lettered immediately rather
            // than retried (see KafkaRequestConsumer#processAndRecord).
            producer.send("LOGIN|user=");
        }

        RequestProcessor processor = new RequestProcessor(
                new RequestParser(),
                new ReloadableValidationRulesProvider(writeLoginRules(), new YamlValidationConfigLoader()));
        EventPulseMetrics metrics = EventPulseMetrics.inMemory();
        List<ConsumerRecord<String, String>> deadLettered;

        try (RequestProcessingExecutor executor = new RequestProcessingExecutor(new ThreadPoolSettings(1, 1, 10));
             DeadLetterPublisher deadLetterPublisher = new DeadLetterPublisher(
                     new DeadLetterQueueSettings(KAFKA.getBootstrapServers(), dlqTopic, "it-dlq-producer"),
                     metrics, plaintextSecurity());
             KafkaRequestConsumer consumer = new KafkaRequestConsumer(
                     new KafkaConsumerSettings(KAFKA.getBootstrapServers(), topic, "it-invalid-group", 200, 3),
                     executor, metrics, deadLetterPublisher, plaintextSecurity())) {

            Thread consumerThread = new Thread(() -> consumer.run(processor));
            consumerThread.setDaemon(true);
            consumerThread.start();

            deadLettered = collectFromTopic(dlqTopic, 1, 30_000);

            consumer.close();
            consumerThread.join(10_000);
        }

        assertEquals(1, deadLettered.size());
        JsonNode envelope = new ObjectMapper().readTree(deadLettered.get(0).value());
        assertEquals(ErrorCode.VALIDATION_ERROR.code(), envelope.get("errorCode").asText());
        assertEquals(1, envelope.get("attempts").asInt());
        assertEquals("LOGIN|user=", envelope.get("rawRequest").asText());
        assertEquals(1.0, metrics.deadLetterCount(ErrorCode.VALIDATION_ERROR));
    }

    private static KafkaSecuritySettings plaintextSecurity() {
        return new KafkaSecuritySettings("PLAINTEXT", "", "", "", "", "", "", "");
    }

    private Path writeLoginRules() throws Exception {
        Path rulesPath = tempDir.resolve("rules.yml");
        Files.writeString(rulesPath, """
                requestTypes:
                  LOGIN:
                    user:
                      required: true
                      minLength: 1
                """);
        return rulesPath;
    }

    private static void createTopics(String... topics) throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (Admin admin = Admin.create(adminProps)) {
            List<NewTopic> newTopics = List.of(topics).stream()
                    .map(topic -> new NewTopic(topic, 1, (short) 1))
                    .toList();
            admin.createTopics(newTopics).all().get(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Polls {@code topic} with a plain, real {@link KafkaConsumer} - not the class under test -
     * until either {@code maxRecords} records are collected or {@code timeoutMillis} elapses,
     * returning whatever was collected. A short timeout with no records collected is how a test
     * asserts a topic received nothing, without hanging for the full timeout on every run.
     */
    private static List<ConsumerRecord<String, String>> collectFromTopic(
            String topic, int maxRecords, long timeoutMillis) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-verifier-" + topic);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        List<ConsumerRecord<String, String>> collected = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + timeoutMillis;
            while (collected.size() < maxRecords && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                records.forEach(collected::add);
            }
        }
        return collected;
    }

    private static void awaitTrue(BooleanSupplier condition, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("condition was not met within " + timeoutMillis + "ms");
            }
            Thread.sleep(50);
        }
    }
}
