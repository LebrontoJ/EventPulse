package com.eventpulse.kafka;

import com.eventpulse.config.ReloadableValidationRulesProvider;
import com.eventpulse.config.YamlValidationConfigLoader;
import com.eventpulse.dlq.DeadLetterPublisher;
import com.eventpulse.error.ErrorCode;
import com.eventpulse.metrics.EventPulseMetrics;
import com.eventpulse.parser.RequestParser;
import com.eventpulse.processor.ProcessingResult;
import com.eventpulse.processor.ProcessingStatus;
import com.eventpulse.processor.RequestProcessor;
import com.eventpulse.threading.RequestProcessingExecutor;
import com.eventpulse.threading.ThreadPoolSettings;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class KafkaRequestConsumerTest {
    private static final String TOPIC = "requests";
    private static final String DLQ_TOPIC = "requests-dlq";

    @TempDir
    Path tempDir;

    @Test
    @Timeout(10)
    void processesBatchCommitsOffsetsAndSendsFailuresToDeadLetterQueue() throws Exception {
        TopicPartition partition = new TopicPartition(TOPIC, 0);
        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        AtomicReference<Map<TopicPartition, OffsetAndMetadata>> committedRef = new AtomicReference<>();

        // MockConsumer runs one scheduled task per poll() call; this one sets up the assignment
        // and enqueues records so they are visible to the KafkaRequestConsumer's very first poll.
        // It also queues a second task, which runs on the *next* poll() call - by then the first
        // batch's commitSync() has already completed, so we read the committed offset from inside
        // that callback (i.e. from the consumer thread itself, before the consumer is ever closed)
        // instead of racing the background thread or querying the mock after it has been closed.
        mockConsumer.schedulePollTask(() -> {
            mockConsumer.rebalance(List.of(partition));
            mockConsumer.updateBeginningOffsets(Map.of(partition, 0L));
            mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "k1", "LOGIN|user=alice"));
            mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1L, "k2", "INVALID_FORMAT_STRING"));
            mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 2L, "k3", "LOGIN|user="));
            mockConsumer.schedulePollTask(() ->
                    committedRef.set(mockConsumer.committed(Set.of(partition), Duration.ofSeconds(1))));
        });

        RecordingRequestProcessor processor = new RecordingRequestProcessor(
                new RequestParser(),
                new ReloadableValidationRulesProvider(writeRules(), new YamlValidationConfigLoader())
        );
        KafkaConsumerSettings settings = new KafkaConsumerSettings("localhost:9092", TOPIC, "test-group", 50, 3);
        EventPulseMetrics metrics = EventPulseMetrics.inMemory();
        MockProducer<String, String> dlqProducer =
                new MockProducer<>(true, new StringSerializer(), new StringSerializer());

        // Single worker thread: keeps processing order deterministic (matching record/offset order)
        // so the assertion below on processed() can compare against an exact expected list.
        try (RequestProcessingExecutor executor = new RequestProcessingExecutor(new ThreadPoolSettings(1, 1, 10));
             DeadLetterPublisher deadLetterPublisher = new DeadLetterPublisher(dlqProducer, DLQ_TOPIC, metrics);
             KafkaRequestConsumer consumer =
                     new KafkaRequestConsumer(mockConsumer, settings, executor, metrics, deadLetterPublisher)) {

            Thread consumerThread = new Thread(() -> consumer.run(processor));
            consumerThread.setDaemon(true);
            consumerThread.start();

            awaitTrue(() -> processor.processed().size() >= 3 && committedRef.get() != null, 5000);

            consumer.close();
            consumerThread.join(5000);
            assertFalse(consumerThread.isAlive(), "consumer loop should stop after close()");
        }

        assertEquals(
                List.of("LOGIN|user=alice", "INVALID_FORMAT_STRING", "LOGIN|user="),
                processor.processed()
        );

        assertEquals(3L, committedRef.get().get(partition).offset(),
                "the malformed and invalid records should still advance the committed offset");

        assertEquals(1.0, metrics.requestCount(ProcessingStatus.SUCCESS));
        assertEquals(1.0, metrics.requestCount(ProcessingStatus.PARSING_ERROR));
        assertEquals(1.0, metrics.requestCount(ProcessingStatus.VALIDATION_ERROR));
        assertEquals(0.0, metrics.requestCount(ProcessingStatus.RUNTIME_ERROR));
        // One commit for the batch of 3, plus the unconditional final commit issued on shutdown.
        assertEquals(2.0, metrics.kafkaCommitCount());
        assertEquals(0.0, metrics.kafkaCommitFailureCount());

        // Parsing/validation errors are deterministic, so they are sent straight to the DLQ with a
        // single attempt rather than being retried.
        assertEquals(1.0, metrics.deadLetterCount(ErrorCode.PARSE_ERROR));
        assertEquals(1.0, metrics.deadLetterCount(ErrorCode.VALIDATION_ERROR));
        assertEquals(0.0, metrics.processingRetryCount());
        assertEquals(2, dlqProducer.history().size());
    }

    @Test
    @Timeout(10)
    void retriesRuntimeErrorsAndSucceedsWithinMaxAttempts() throws Exception {
        TopicPartition partition = new TopicPartition(TOPIC, 0);
        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        mockConsumer.schedulePollTask(() -> {
            mockConsumer.rebalance(List.of(partition));
            mockConsumer.updateBeginningOffsets(Map.of(partition, 0L));
            mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "k1", "irrelevant"));
        });

        FlakyRequestProcessor processor = new FlakyRequestProcessor(2);
        KafkaConsumerSettings settings = new KafkaConsumerSettings("localhost:9092", TOPIC, "test-group", 50, 3);
        EventPulseMetrics metrics = EventPulseMetrics.inMemory();
        MockProducer<String, String> dlqProducer =
                new MockProducer<>(true, new StringSerializer(), new StringSerializer());

        try (RequestProcessingExecutor executor = new RequestProcessingExecutor(new ThreadPoolSettings(1, 1, 10));
             DeadLetterPublisher deadLetterPublisher = new DeadLetterPublisher(dlqProducer, DLQ_TOPIC, metrics);
             KafkaRequestConsumer consumer =
                     new KafkaRequestConsumer(mockConsumer, settings, executor, metrics, deadLetterPublisher)) {

            Thread consumerThread = new Thread(() -> consumer.run(processor));
            consumerThread.setDaemon(true);
            consumerThread.start();

            awaitTrue(() -> processor.callCount() >= 3, 5000);
            consumer.close();
            consumerThread.join(5000);
        }

        assertEquals(3, processor.callCount(), "2 failed attempts + 1 successful attempt");
        assertEquals(1.0, metrics.requestCount(ProcessingStatus.SUCCESS));
        assertEquals(0.0, metrics.requestCount(ProcessingStatus.RUNTIME_ERROR),
                "only the final outcome is recorded, not each retried attempt");
        assertEquals(2.0, metrics.processingRetryCount());
        assertEquals(0, dlqProducer.history().size(), "a message that eventually succeeds is never dead-lettered");
    }

    @Test
    @Timeout(10)
    void exhaustsRetriesThenPublishesToDeadLetterQueue() throws Exception {
        TopicPartition partition = new TopicPartition(TOPIC, 0);
        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        mockConsumer.schedulePollTask(() -> {
            mockConsumer.rebalance(List.of(partition));
            mockConsumer.updateBeginningOffsets(Map.of(partition, 0L));
            mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0L, "k1", "irrelevant"));
        });

        FlakyRequestProcessor processor = new FlakyRequestProcessor(Integer.MAX_VALUE);
        KafkaConsumerSettings settings = new KafkaConsumerSettings("localhost:9092", TOPIC, "test-group", 50, 3);
        EventPulseMetrics metrics = EventPulseMetrics.inMemory();
        MockProducer<String, String> dlqProducer =
                new MockProducer<>(true, new StringSerializer(), new StringSerializer());

        try (RequestProcessingExecutor executor = new RequestProcessingExecutor(new ThreadPoolSettings(1, 1, 10));
             DeadLetterPublisher deadLetterPublisher = new DeadLetterPublisher(dlqProducer, DLQ_TOPIC, metrics);
             KafkaRequestConsumer consumer =
                     new KafkaRequestConsumer(mockConsumer, settings, executor, metrics, deadLetterPublisher)) {

            Thread consumerThread = new Thread(() -> consumer.run(processor));
            consumerThread.setDaemon(true);
            consumerThread.start();

            awaitTrue(() -> dlqProducer.history().size() >= 1, 5000);
            consumer.close();
            consumerThread.join(5000);
        }

        assertEquals(3, processor.callCount(), "gives up after settings.maxProcessingAttempts()");
        assertEquals(1.0, metrics.requestCount(ProcessingStatus.RUNTIME_ERROR));
        assertEquals(2.0, metrics.processingRetryCount());
        assertEquals(1.0, metrics.deadLetterCount(ErrorCode.RUNTIME_ERROR));
        assertEquals(1, dlqProducer.history().size());

        JsonNode envelope = new ObjectMapper().readTree(dlqProducer.history().get(0).value());
        assertEquals(3, envelope.get("attempts").asInt());
        assertEquals(ErrorCode.RUNTIME_ERROR.code(), envelope.get("errorCode").asText());
    }

    @Test
    void rebalanceListenerCommitsBeforeRevokeAndRecordsBothEvents() throws Exception {
        // MockConsumer.rebalance(...) does not itself invoke a registered ConsumerRebalanceListener,
        // so the listener is driven directly here rather than through consumer.run().
        TopicPartition partition = new TopicPartition(TOPIC, 0);
        MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        mockConsumer.subscribe(List.of(TOPIC));
        mockConsumer.rebalance(List.of(partition));
        mockConsumer.updateBeginningOffsets(Map.of(partition, 0L));
        mockConsumer.seek(partition, 5L);

        EventPulseMetrics metrics = EventPulseMetrics.inMemory();
        MockProducer<String, String> dlqProducer =
                new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        KafkaConsumerSettings settings = new KafkaConsumerSettings("localhost:9092", TOPIC, "test-group", 50, 3);

        try (RequestProcessingExecutor executor = new RequestProcessingExecutor(new ThreadPoolSettings(1, 1, 10));
             DeadLetterPublisher deadLetterPublisher = new DeadLetterPublisher(dlqProducer, DLQ_TOPIC, metrics);
             KafkaRequestConsumer consumer =
                     new KafkaRequestConsumer(mockConsumer, settings, executor, metrics, deadLetterPublisher)) {

            KafkaRequestConsumer.RebalanceListener listener = consumer.new RebalanceListener();
            listener.onPartitionsRevoked(List.of(partition));
            listener.onPartitionsAssigned(List.of(partition));

            assertEquals(1.0, metrics.rebalanceEventCount("revoked"));
            assertEquals(1.0, metrics.rebalanceEventCount("assigned"));
            assertEquals(5L, mockConsumer.committed(Set.of(partition), Duration.ofSeconds(1)).get(partition).offset(),
                    "offsets should be committed synchronously before partitions are given up");
        }
    }

    private Path writeRules() throws Exception {
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

    private static void awaitTrue(BooleanSupplier condition, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("condition was not met within " + timeoutMillis + "ms");
            }
            Thread.sleep(20);
        }
    }

    /**
     * Wraps the real processing pipeline (real parser + real validation rules) while recording
     * every raw request handed to it, so the test can assert on side effects instead of reaching
     * into the MockConsumer concurrently from another thread.
     */
    private static final class RecordingRequestProcessor extends RequestProcessor {
        private final List<String> processed = new CopyOnWriteArrayList<>();

        RecordingRequestProcessor(RequestParser parser, ReloadableValidationRulesProvider rulesProvider) {
            super(parser, rulesProvider);
        }

        @Override
        public ProcessingResult process(String rawRequest) {
            ProcessingResult result = super.process(rawRequest);
            processed.add(rawRequest);
            return result;
        }

        List<String> processed() {
            return processed;
        }
    }

    /**
     * Stub processor that returns {@link ProcessingStatus#RUNTIME_ERROR} for the first
     * {@code failuresBeforeSuccess} calls and {@link ProcessingStatus#SUCCESS} afterwards, so
     * retry behavior can be tested deterministically without a real parser/validator.
     */
    private static final class FlakyRequestProcessor extends RequestProcessor {
        private final int failuresBeforeSuccess;
        private final AtomicInteger callCount = new AtomicInteger();

        FlakyRequestProcessor(int failuresBeforeSuccess) {
            super(null, null);
            this.failuresBeforeSuccess = failuresBeforeSuccess;
        }

        @Override
        public ProcessingResult process(String rawRequest) {
            int call = callCount.incrementAndGet();
            if (call <= failuresBeforeSuccess) {
                return ProcessingResult.failure(ProcessingStatus.RUNTIME_ERROR, "simulated transient failure");
            }
            return ProcessingResult.success();
        }

        int callCount() {
            return callCount.get();
        }
    }
}
