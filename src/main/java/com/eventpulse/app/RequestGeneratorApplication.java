package com.eventpulse.app;

import com.eventpulse.generator.RequestGeneratorSettings;
import com.eventpulse.generator.SimulatedRequestGenerator;
import com.eventpulse.kafka.KafkaProducerSettings;
import com.eventpulse.kafka.KafkaRequestProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public final class RequestGeneratorApplication {
    private static final Logger log = LoggerFactory.getLogger(RequestGeneratorApplication.class);

    private RequestGeneratorApplication() {
    }

    public static void main(String[] args) throws InterruptedException {
        KafkaProducerSettings producerSettings = new KafkaProducerSettings(
                property("kafka.bootstrap.servers", "localhost:9092"),
                property("kafka.topic", "requests"),
                property("kafka.producer.client.id", "eventpulse-generator")
        );
        RequestGeneratorSettings generatorSettings = new RequestGeneratorSettings(
                longProperty("generator.interval.ms", 1000),
                intProperty("generator.invalid.rate.percent", 5),
                longProperty("generator.max.requests", 0)
        );

        AtomicBoolean running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

        SimulatedRequestGenerator generator = new SimulatedRequestGenerator(generatorSettings.invalidRequestRatePercent());
        try (KafkaRequestProducer producer = new KafkaRequestProducer(producerSettings)) {
            long sent = 0;
            log.info("Request generator started topic={} intervalMs={} invalidRatePercent={} maxRequests={}",
                    producerSettings.topic(),
                    generatorSettings.intervalMillis(),
                    generatorSettings.invalidRequestRatePercent(),
                    generatorSettings.maxRequests());
            while (running.get() && shouldContinue(generatorSettings.maxRequests(), sent)) {
                producer.send(generator.nextRequest());
                sent++;
                Thread.sleep(generatorSettings.intervalMillis());
            }
            log.info("Request generator stopped after sending {} requests", sent);
        }
    }

    private static boolean shouldContinue(long maxRequests, long sent) {
        return maxRequests == 0 || sent < maxRequests;
    }

    private static String property(String name, String defaultValue) {
        return Optional.ofNullable(System.getProperty(name)).orElse(defaultValue);
    }

    private static int intProperty(String name, int defaultValue) {
        return Integer.parseInt(property(name, Integer.toString(defaultValue)));
    }

    private static long longProperty(String name, long defaultValue) {
        return Long.parseLong(property(name, Long.toString(defaultValue)));
    }
}
