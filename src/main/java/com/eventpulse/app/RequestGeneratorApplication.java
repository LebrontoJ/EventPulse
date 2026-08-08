package com.eventpulse.app;

import com.eventpulse.config.ApplicationConfig;
import com.eventpulse.generator.RequestGeneratorSettings;
import com.eventpulse.generator.SimulatedRequestGenerator;
import com.eventpulse.kafka.KafkaProducerSettings;
import com.eventpulse.kafka.KafkaRequestProducer;
import com.eventpulse.metrics.EventPulseMetrics;
import com.eventpulse.validation.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public final class RequestGeneratorApplication {
    private static final Logger log = LoggerFactory.getLogger(RequestGeneratorApplication.class);

    private RequestGeneratorApplication() {
    }

    public static void main(String[] args) throws InterruptedException, ConfigurationException, IOException {
        ApplicationConfig config = ApplicationConfig.load();

        KafkaProducerSettings producerSettings = new KafkaProducerSettings(
                config.get("kafka.bootstrap.servers", "localhost:9092"),
                config.get("kafka.topic", "requests"),
                config.get("kafka.producer.client.id", "eventpulse-generator")
        );
        RequestGeneratorSettings generatorSettings = new RequestGeneratorSettings(
                config.getLong("generator.interval.ms", 1000),
                config.getInt("generator.invalid.rate.percent", 5),
                config.getLong("generator.max.requests", 0)
        );
        boolean metricsEnabled = Boolean.parseBoolean(config.get("metrics.enabled", "true"));
        int metricsPort = config.getInt("generator.metrics.port", 9405);

        AtomicBoolean running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

        SimulatedRequestGenerator generator = new SimulatedRequestGenerator(generatorSettings.invalidRequestRatePercent());
        try (EventPulseMetrics metrics = EventPulseMetrics.start(metricsEnabled, metricsPort);
             KafkaRequestProducer producer = new KafkaRequestProducer(producerSettings)) {
            long sent = 0;
            log.info("Request generator started topic={} intervalMs={} invalidRatePercent={} maxRequests={}",
                    producerSettings.topic(),
                    generatorSettings.intervalMillis(),
                    generatorSettings.invalidRequestRatePercent(),
                    generatorSettings.maxRequests());
            while (running.get() && shouldContinue(generatorSettings.maxRequests(), sent)) {
                producer.send(generator.nextRequest());
                metrics.recordGeneratedRequest();
                sent++;
                Thread.sleep(generatorSettings.intervalMillis());
            }
            log.info("Request generator stopped after sending {} requests", sent);
        }
    }

    private static boolean shouldContinue(long maxRequests, long sent) {
        return maxRequests == 0 || sent < maxRequests;
    }
}
