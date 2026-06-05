package com.eventpulse.app;

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
import java.util.Optional;

public final class EventPulseApplication {
    private static final Logger log = LoggerFactory.getLogger(EventPulseApplication.class);

    private EventPulseApplication() {
    }

    public static void main(String[] args) throws ConfigurationException, URISyntaxException {
        Path validationRulesPath = validationRulesPath();
        ReloadableValidationRulesProvider rulesProvider = new ReloadableValidationRulesProvider(
                validationRulesPath,
                ValidationConfigLoaderFactory.forPath(validationRulesPath)
        );

        ThreadPoolSettings threadPoolSettings = new ThreadPoolSettings(
                intProperty("threadpool.core.size", 4),
                intProperty("threadpool.max.size", 8),
                intProperty("threadpool.queue.capacity", 1000)
        );
        KafkaConsumerSettings kafkaSettings = new KafkaConsumerSettings(
                property("kafka.bootstrap.servers", "localhost:9092"),
                property("kafka.topic", "requests"),
                property("kafka.group.id", "eventpulse-v1"),
                intProperty("kafka.poll.timeout.ms", 1000)
        );

        RequestProcessor processor = new RequestProcessor(new RequestParser(), rulesProvider);
        try (RequestProcessingExecutor executor = new RequestProcessingExecutor(threadPoolSettings);
             KafkaRequestConsumer consumer = new KafkaRequestConsumer(kafkaSettings, executor)) {
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));
            log.info("EventPulse V1 starting with rules={}", validationRulesPath);
            consumer.run(processor);
        }
    }

    private static Path validationRulesPath() throws URISyntaxException {
        String configuredPath = System.getProperty("validation.rules.path");
        if (configuredPath != null && !configuredPath.isBlank()) {
            return Path.of(configuredPath);
        }
        URL resource = Objects.requireNonNull(
                EventPulseApplication.class.getResource("/validation-rules.yml"),
                "Default validation-rules.yml resource is missing"
        );
        return Path.of(resource.toURI());
    }

    private static String property(String name, String defaultValue) {
        return Optional.ofNullable(System.getProperty(name)).orElse(defaultValue);
    }

    private static int intProperty(String name, int defaultValue) {
        return Integer.parseInt(property(name, Integer.toString(defaultValue)));
    }
}
