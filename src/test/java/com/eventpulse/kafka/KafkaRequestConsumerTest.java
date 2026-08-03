package com.eventpulse.kafka;

import com.eventpulse.config.ReloadableValidationRulesProvider;
import com.eventpulse.config.YamlValidationConfigLoader;
import com.eventpulse.parser.RequestParser;
import com.eventpulse.processor.ProcessingResult;
import com.eventpulse.processor.RequestProcessor;
import com.eventpulse.threading.RequestProcessingExecutor;
import com.eventpulse.threading.ThreadPoolSettings;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class KafkaRequestConsumerTest {
    private static final String TOPIC = "requests";

    @TempDir
    Path tempDir;

    @Test
    @Timeout(10)
    void processesBatchCommitsOffsetsAndSurvivesBadRecordsUntilClosed() throws Exception {
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
        KafkaConsumerSettings settings = new KafkaConsumerSettings("localhost:9092", TOPIC, "test-group", 50);

        // Single worker thread: keeps processing order deterministic (matching record/offset order)
        // so the assertion below on processed() can compare against an exact expected list.
        try (RequestProcessingExecutor executor = new RequestProcessingExecutor(new ThreadPoolSettings(1, 1, 10));
             KafkaRequestConsumer consumer = new KafkaRequestConsumer(mockConsumer, settings, executor)) {

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
}
