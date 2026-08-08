package com.eventpulse.kafka;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaProducerSettingsTest {
    @Test
    void storesValidSettings() {
        KafkaProducerSettings settings = new KafkaProducerSettings("localhost:9092", "requests", "generator-1");

        assertEquals("localhost:9092", settings.bootstrapServers());
        assertEquals("requests", settings.topic());
        assertEquals("generator-1", settings.clientId());
    }

    @Test
    void rejectsBlankBootstrapServers() {
        assertThrows(IllegalArgumentException.class,
                () -> new KafkaProducerSettings(" ", "requests", "generator-1"));
    }

    @Test
    void rejectsBlankTopic() {
        assertThrows(IllegalArgumentException.class,
                () -> new KafkaProducerSettings("localhost:9092", " ", "generator-1"));
    }

    @Test
    void rejectsBlankClientId() {
        assertThrows(IllegalArgumentException.class,
                () -> new KafkaProducerSettings("localhost:9092", "requests", " "));
    }
}
