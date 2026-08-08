package com.eventpulse.kafka;

import com.eventpulse.dlq.DeadLetterPublishException;
import com.eventpulse.dlq.DeadLetterPublisher;
import com.eventpulse.error.ErrorCode;
import com.eventpulse.metrics.EventPulseMetrics;
import com.eventpulse.processor.ProcessingResult;
import com.eventpulse.processor.ProcessingStatus;
import com.eventpulse.processor.RequestProcessor;
import com.eventpulse.threading.RequestProcessingExecutor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaRequestConsumer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(KafkaRequestConsumer.class);

    private final Consumer<String, String> consumer;
    private final RequestProcessingExecutor executor;
    private final KafkaConsumerSettings settings;
    private final EventPulseMetrics metrics;
    private final DeadLetterPublisher deadLetterPublisher;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public KafkaRequestConsumer(KafkaConsumerSettings settings,
                                 RequestProcessingExecutor executor,
                                 EventPulseMetrics metrics,
                                 DeadLetterPublisher deadLetterPublisher) {
        this(new KafkaConsumer<>(properties(settings)), settings, executor, metrics, deadLetterPublisher);
    }

    // Package-private constructor accepting the Consumer interface (rather than the concrete
    // KafkaConsumer) so tests can inject org.apache.kafka.clients.consumer.MockConsumer.
    KafkaRequestConsumer(Consumer<String, String> consumer,
                         KafkaConsumerSettings settings,
                         RequestProcessingExecutor executor,
                         EventPulseMetrics metrics,
                         DeadLetterPublisher deadLetterPublisher) {
        this.consumer = consumer;
        this.settings = settings;
        this.executor = executor;
        this.metrics = metrics;
        this.deadLetterPublisher = deadLetterPublisher;
    }

    public void run(RequestProcessor processor) {
        consumer.subscribe(List.of(settings.topic()), new RebalanceListener());
        log.info("Kafka consumer started topic={} groupId={} maxProcessingAttempts={}",
                settings.topic(), settings.groupId(), settings.maxProcessingAttempts());

        try {
            while (running.get()) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(settings.pollTimeoutMillis()));
                    List<Future<?>> futures = new ArrayList<>(records.count());
                    for (ConsumerRecord<String, String> record : records) {
                        futures.add(executor.submit(() -> processAndRecord(processor, record)));
                    }
                    waitForBatch(futures);
                    if (!futures.isEmpty()) {
                        commit();
                    }
                } catch (WakeupException exception) {
                    if (running.get()) {
                        log.warn("Kafka consumer wakeup while running", exception);
                    }
                } catch (RuntimeException exception) {
                    log.error("Kafka consumer loop error; continuing", exception);
                }
            }
        } finally {
            try {
                commit();
            } catch (RuntimeException exception) {
                log.warn("Final Kafka commit failed", exception);
            }
            consumer.close();
            log.info("Kafka consumer stopped");
        }
    }

    /**
     * Processes a single record, retrying {@link ProcessingStatus#RUNTIME_ERROR} results up to
     * {@link KafkaConsumerSettings#maxProcessingAttempts()} times since those may be transient.
     * Parsing/validation errors are deterministic - retrying them would just reproduce the same
     * failure - so they are sent straight to the dead letter queue. Only the final outcome is
     * recorded in {@code eventpulse_requests_total}; earlier failed attempts are counted
     * separately via {@code eventpulse_processing_retries_total}.
     */
    private void processAndRecord(RequestProcessor processor, ConsumerRecord<String, String> record) {
        String rawRequest = record.value();
        int maxAttempts = settings.maxProcessingAttempts();
        int attempts = 0;
        ProcessingResult result;
        long start = System.nanoTime();
        do {
            attempts++;
            result = processor.process(rawRequest);
        } while (result.status() == ProcessingStatus.RUNTIME_ERROR && attempts < maxAttempts);
        metrics.recordProcessed(result.status(), System.nanoTime() - start);

        if (attempts > 1) {
            metrics.recordProcessingRetries(attempts - 1);
        }
        if (result.status() != ProcessingStatus.SUCCESS) {
            sendToDeadLetterQueue(record, result, attempts);
        }
    }

    private void sendToDeadLetterQueue(ConsumerRecord<String, String> record, ProcessingResult result, int attempts) {
        ErrorCode errorCode = errorCodeFor(result.status());
        try {
            deadLetterPublisher.publish(record, errorCode, result.message(), attempts);
        } catch (DeadLetterPublishException exception) {
            log.error("Could not publish message to dead letter queue key={}", record.key(), exception);
        }
    }

    private static ErrorCode errorCodeFor(ProcessingStatus status) {
        return switch (status) {
            case PARSING_ERROR -> ErrorCode.PARSE_ERROR;
            case VALIDATION_ERROR -> ErrorCode.VALIDATION_ERROR;
            case RUNTIME_ERROR -> ErrorCode.RUNTIME_ERROR;
            case SUCCESS -> throw new IllegalStateException("SUCCESS results are never dead-lettered");
        };
    }

    private void commit() {
        try {
            consumer.commitSync();
            metrics.recordKafkaCommit();
        } catch (RuntimeException exception) {
            metrics.recordKafkaCommitFailure();
            throw exception;
        }
    }

    private void waitForBatch(List<Future<?>> futures) {
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                running.set(false);
                return;
            } catch (Exception exception) {
                log.error("Request processing task failed unexpectedly; continuing", exception);
            }
        }
    }

    @Override
    public void close() {
        running.set(false);
        consumer.wakeup();
    }

    /**
     * Commits the current offsets before partitions are revoked - the standard Kafka pattern for
     * avoiding reprocessing of already-completed work once a partition moves to another consumer
     * in the group - and records both halves of a rebalance as metrics for observability.
     *
     * <p>Package-private (not private) and a non-static inner class so tests can instantiate it
     * directly via {@code consumer.new RebalanceListener()} and drive its callbacks: Kafka's
     * {@code MockConsumer.rebalance(...)} does not itself invoke a registered rebalance listener.
     */
    final class RebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.info("Partitions revoked: {}", partitions);
            try {
                commit();
            } catch (RuntimeException exception) {
                log.warn("Commit during partition revocation failed", exception);
            }
            metrics.recordRebalanceEvent("revoked");
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.info("Partitions assigned: {}", partitions);
            metrics.recordRebalanceEvent("assigned");
        }
    }

    private static Properties properties(KafkaConsumerSettings settings) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, settings.groupId());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }
}
