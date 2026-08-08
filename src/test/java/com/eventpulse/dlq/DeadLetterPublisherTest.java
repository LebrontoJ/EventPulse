package com.eventpulse.dlq;

import com.eventpulse.error.ErrorCode;
import com.eventpulse.metrics.EventPulseMetrics;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DeadLetterPublisherTest {
    private static final String TOPIC = "requests";
    private static final String DLQ_TOPIC = "requests-dlq";

    @Test
    void publishesEnvelopeWithFailureContextToTheConfiguredTopic() throws Exception {
        MockProducer<String, String> mockProducer =
                new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        EventPulseMetrics metrics = EventPulseMetrics.inMemory();
        ConsumerRecord<String, String> record =
                new ConsumerRecord<>(TOPIC, 0, 7L, "k1", "LOGIN|user=");

        try (DeadLetterPublisher publisher = new DeadLetterPublisher(mockProducer, DLQ_TOPIC, metrics)) {
            publisher.publish(record, ErrorCode.VALIDATION_ERROR, "user must not be blank", 1);
        }

        assertEquals(1, mockProducer.history().size());
        ProducerRecord<String, String> published = mockProducer.history().get(0);
        assertEquals(DLQ_TOPIC, published.topic());
        assertEquals("k1", published.key());

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode envelope = objectMapper.readTree(published.value());
        assertEquals(TOPIC, envelope.get("originalTopic").asText());
        assertEquals(0, envelope.get("originalPartition").asInt());
        assertEquals(7L, envelope.get("originalOffset").asLong());
        assertEquals("LOGIN|user=", envelope.get("rawRequest").asText());
        assertEquals(ErrorCode.VALIDATION_ERROR.code(), envelope.get("errorCode").asText());
        assertEquals("user must not be blank", envelope.get("errorMessage").asText());
        assertEquals(1, envelope.get("attempts").asInt());

        assertEquals(1.0, metrics.deadLetterCount(ErrorCode.VALIDATION_ERROR));
        assertEquals(0.0, metrics.deadLetterPublishFailureCount());
        assertTrue(mockProducer.closed());
    }

    @Test
    void recordsAPublishFailureWhenTheProducerReportsASendFailure() {
        MockProducer<String, String> mockProducer =
                new MockProducer<>(false, new StringSerializer(), new StringSerializer());
        EventPulseMetrics metrics = EventPulseMetrics.inMemory();
        ConsumerRecord<String, String> record =
                new ConsumerRecord<>(TOPIC, 0, 3L, "k2", "boom");

        DeadLetterPublisher publisher = new DeadLetterPublisher(mockProducer, DLQ_TOPIC, metrics);
        publisher.publish(record, ErrorCode.RUNTIME_ERROR, "unexpected failure", 3);

        boolean completed = mockProducer.errorNext(new RuntimeException("simulated broker failure"));

        assertTrue(completed, "the pending send should have been completed with an error");
        assertEquals(1.0, metrics.deadLetterCount(ErrorCode.RUNTIME_ERROR),
                "the message is still counted as dead-lettered even though publishing failed");
        assertEquals(1.0, metrics.deadLetterPublishFailureCount());
        publisher.close();
    }
}
