package com.eventpulse.kafka;

import com.eventpulse.processor.RequestProcessor;
import com.eventpulse.threading.RequestProcessingExecutor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaRequestConsumer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(KafkaRequestConsumer.class);

    private final KafkaConsumer<String, String> consumer;
    private final RequestProcessingExecutor executor;
    private final KafkaConsumerSettings settings;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public KafkaRequestConsumer(KafkaConsumerSettings settings, RequestProcessingExecutor executor) {
        this(new KafkaConsumer<>(properties(settings)), settings, executor);
    }

    KafkaRequestConsumer(KafkaConsumer<String, String> consumer,
                         KafkaConsumerSettings settings,
                         RequestProcessingExecutor executor) {
        this.consumer = consumer;
        this.settings = settings;
        this.executor = executor;
    }

    public void run(RequestProcessor processor) {
        consumer.subscribe(List.of(settings.topic()));
        log.info("Kafka consumer started topic={} groupId={}", settings.topic(), settings.groupId());

        try {
            while (running.get()) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(settings.pollTimeoutMillis()));
                    List<Future<?>> futures = new ArrayList<>(records.count());
                    for (ConsumerRecord<String, String> record : records) {
                        futures.add(executor.submit(() -> processor.process(record.value())));
                    }
                    waitForBatch(futures);
                    if (!futures.isEmpty()) {
                        consumer.commitSync();
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
                consumer.commitSync();
            } catch (RuntimeException exception) {
                log.warn("Final Kafka commit failed", exception);
            }
            consumer.close();
            log.info("Kafka consumer stopped");
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
