package com.eventpulse.app;

import com.eventpulse.config.ApplicationConfig;
import com.eventpulse.config.ReloadableValidationRulesProvider;
import com.eventpulse.config.ValidationConfigLoaderFactory;
import com.eventpulse.kafka.KafkaConsumerSettings;
import com.eventpulse.kafka.KafkaRequestConsumer;
import com.eventpulse.parser.RequestParser;
import com.eventpulse.processor.RequestProcessor;
import com.eventpulse.threading.RequestProcessingExecutor;
import com.eventpulse.threading.ThreadPoolSettings;
import com.eventpulse.validation.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Objects;

public final class EventPulseApplication {
    private static final Logger log = LoggerFactory.getLogger(EventPulseApplication.class);

    private EventPulseApplication() {
    }

    public static void main(String[] args) throws ConfigurationException, URISyntaxException {
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
                config.getInt("kafka.poll.timeout.ms", 1000)
        );

        RequestProcessor processor = new RequestProcessor(new RequestParser(), rulesProvider);
        try (RequestProcessingExecutor executor = new RequestProcessingExecutor(threadPoolSettings);
             KafkaRequestConsumer consumer = new KafkaRequestConsumer(kafkaSettings, executor)) {
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));
            log.info("EventPulse V1 starting with rules={}", validationRulesPath);
            consumer.run(processor);
        }
    }

    private static Path validationRulesPath(ApplicationConfig config) throws URISyntaxException {
        String configuredPath = config.get("validation.rules.path", "");
        if (!configuredPath.isBlank()) {
            return Path.of(configuredPath);
        }
        URL resource = Objects.requireNonNull(
                EventPulseApplication.class.getResource("/validation-rules.yml"),
                "Default validation-rules.yml resource is missing"
        );
        return Path.of(resource.toURI());
    }
}
