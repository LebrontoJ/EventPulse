package com.eventpulse.app;

import com.eventpulse.config.ApplicationConfig;
import com.eventpulse.config.ReloadableValidationRulesProvider;
import com.eventpulse.config.ValidationConfigLoaderFactory;
import com.eventpulse.dlq.DeadLetterPublisher;
import com.eventpulse.dlq.DeadLetterQueueSettings;
import com.eventpulse.kafka.KafkaConsumerSettings;
import com.eventpulse.kafka.KafkaRequestConsumer;
import com.eventpulse.metrics.EventPulseMetrics;
import com.eventpulse.parser.RequestParser;
import com.eventpulse.processor.RequestProcessor;
import com.eventpulse.threading.RequestProcessingExecutor;
import com.eventpulse.threading.ThreadPoolSettings;
import com.eventpulse.validation.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public final class EventPulseApplication {
    private static final Logger log = LoggerFactory.getLogger(EventPulseApplication.class);

    private EventPulseApplication() {
    }

    public static void main(String[] args) throws ConfigurationException, IOException {
        ApplicationConfig config = ApplicationConfig.load();

        Path validationRulesPath = validationRulesPath(config);
        ReloadableValidationRulesProvider rulesProvider = new ReloadableValidationRulesProvider(
                validationRulesPath,
                ValidationConfigLoaderFactory.forPath(validationRulesPath)
        );

        ThreadPoolSettings threadPoolSettings = new ThreadPoolSettings(
                config.getInt("threadpool.core.size", 4),
                config.getInt("threadpool.max.size", 8),
                config.getInt("threadpool.queue.capacity", 1000)
        );
        KafkaConsumerSettings kafkaSettings = new KafkaConsumerSettings(
                config.get("kafka.bootstrap.servers", "localhost:9092"),
                config.get("kafka.topic", "requests"),
                config.get("kafka.group.id", "eventpulse-v1"),
                config.getInt("kafka.poll.timeout.ms", 1000),
                config.getInt("kafka.processing.max.attempts", 3)
        );
        DeadLetterQueueSettings dlqSettings = new DeadLetterQueueSettings(
                config.get("kafka.bootstrap.servers", "localhost:9092"),
                config.get("kafka.dlq.topic", "requests-dlq"),
                config.get("kafka.dlq.producer.client.id", "eventpulse-dlq-producer")
        );
        boolean metricsEnabled = Boolean.parseBoolean(config.get("metrics.enabled", "true"));
        int metricsPort = config.getInt("metrics.port", 9404);

        RequestProcessor processor = new RequestProcessor(new RequestParser(), rulesProvider);
        try (EventPulseMetrics metrics = EventPulseMetrics.start(metricsEnabled, metricsPort);
             DeadLetterPublisher deadLetterPublisher = new DeadLetterPublisher(dlqSettings, metrics);
             RequestProcessingExecutor executor = new RequestProcessingExecutor(threadPoolSettings);
             KafkaRequestConsumer consumer = new KafkaRequestConsumer(kafkaSettings, executor, metrics, deadLetterPublisher)) {
            metrics.bindThreadPool(executor::activeThreadCount, executor::queuedTaskCount, executor::completedTaskCount);
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));
            log.info("EventPulse V1 starting with rules={} dlqTopic={} maxProcessingAttempts={}",
                    validationRulesPath, dlqSettings.topic(), kafkaSettings.maxProcessingAttempts());
            consumer.run(processor);
        }
    }

    private static Path validationRulesPath(ApplicationConfig config) throws IOException {
        String configuredPath = config.get("validation.rules.path", "");
        if (!configuredPath.isBlank()) {
            return Path.of(configuredPath);
        }
        return extractDefaultValidationRules();
    }

    // Path.of(resource.toURI()) fails with FileSystemNotFoundException when the resource lives
    // inside a jar (e.g. the shaded jar used in Docker) rather than on the plain filesystem
    // (e.g. target/classes when running via `mvn exec:java`). Copying the bundled resource to a
    // temp file gives every downstream loader (which reads via Files.newInputStream(Path)) a real
    // filesystem path to work with, regardless of how the app is packaged/run.
    private static Path extractDefaultValidationRules() throws IOException {
        try (InputStream resourceStream = EventPulseApplication.class.getResourceAsStream("/validation-rules.yml")) {
            if (resourceStream == null) {
                throw new IllegalStateException("Default validation-rules.yml resource is missing");
            }
            Path tempFile = Files.createTempFile("eventpulse-validation-rules", ".yml");
            tempFile.toFile().deleteOnExit();
            Files.copy(resourceStream, tempFile, StandardCopyOption.REPLACE_EXISTING);
            return tempFile;
        }
    }
}
