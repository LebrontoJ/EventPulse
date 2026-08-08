package com.eventpulse.kafka;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaRequestProducerTest {
    private static final String TOPIC = "requests";

    @Test
    void sendsRecordsToTheConfiguredTopic() {
        MockProducer<String, String> mockProducer =
                new MockProducer<>(true, new StringSerializer(), new StringSerializer());

        try (KafkaRequestProducer producer = new KafkaRequestProducer(mockProducer, TOPIC)) {
            producer.send("LOGIN|user=alice");
            producer.send("MESSAGE|from=bob|to=taylor|content=hello");
        }

        assertEquals(2, mockProducer.history().size());
        ProducerRecord<String, String> first = mockProducer.history().get(0);
        assertEquals(TOPIC, first.topic());
        assertEquals("LOGIN|user=alice", first.value());
        assertTrue(mockProducer.closed());
    }

    @Test
    void doesNotThrowWhenTheProducerReportsASendFailure() {
        MockProducer<String, String> mockProducer =
                new MockProducer<>(false, new StringSerializer(), new StringSerializer());

        KafkaRequestProducer producer = new KafkaRequestProducer(mockProducer, TOPIC);
        producer.send("LOGIN|user=alice");

        boolean completed = mockProducer.errorNext(new RuntimeException("simulated broker failure"));

        assertTrue(completed, "the pending send should have been completed with an error");
        producer.close();
        assertTrue(mockProducer.closed());
    }
}
