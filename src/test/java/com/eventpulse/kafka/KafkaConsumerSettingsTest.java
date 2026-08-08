package com.eventpulse.kafka;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaConsumerSettingsTest {
    @Test
    void storesValidSettings() {
        KafkaConsumerSettings settings = new KafkaConsumerSettings("localhost:9092", "requests", "group-1", 1000, 3);

        assertEquals("localhost:9092", settings.bootstrapServers());
        assertEquals("requests", settings.topic());
        assertEquals("group-1", settings.groupId());
        assertEquals(1000, settings.pollTimeoutMillis());
        assertEquals(3, settings.maxProcessingAttempts());
    }

    @Test
    void rejectsBlankBootstrapServers() {
        assertThrows(IllegalArgumentException.class,
                () -> new KafkaConsumerSettings(" ", "requests", "group-1", 1000, 3));
    }

    @Test
    void rejectsNullBootstrapServers() {
        assertThrows(IllegalArgumentException.class,
                () -> new KafkaConsumerSettings(null, "requests", "group-1", 1000, 3));
    }

    @Test
    void rejectsBlankTopic() {
        assertThrows(IllegalArgumentException.class,
                () -> new KafkaConsumerSettings("localhost:9092", " ", "group-1", 1000, 3));
    }

    @Test
    void rejectsBlankGroupId() {
        assertThrows(IllegalArgumentException.class,
                () -> new KafkaConsumerSettings("localhost:9092", "requests", " ", 1000, 3));
    }

    @Test
    void rejectsNonPositivePollTimeout() {
        assertThrows(IllegalArgumentException.class,
                () -> new KafkaConsumerSettings("localhost:9092", "requests", "group-1", 0, 3));
    }

    @Test
    void rejectsNonPositiveMaxProcessingAttempts() {
        assertThrows(IllegalArgumentException.class,
                () -> new KafkaConsumerSettings("localhost:9092", "requests", "group-1", 1000, 0));
    }
}
