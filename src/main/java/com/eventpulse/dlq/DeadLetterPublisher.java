package com.eventpulse.dlq;

import com.eventpulse.error.ErrorCode;
import com.eventpulse.metrics.EventPulseMetrics;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;

/**
 * Publishes messages that permanently failed processing - parsing/validation errors (which are
 * deterministic and go straight here) or runtime errors that exhausted their retry attempts - to
 * a dead letter queue topic, wrapped in a {@link DeadLetterEnvelope} JSON payload.
 */
public class DeadLetterPublisher implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(DeadLetterPublisher.class);

    private final Producer<String, String> producer;
    private final String topic;
    private final EventPulseMetrics metrics;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public DeadLetterPublisher(DeadLetterQueueSettings settings, EventPulseMetrics metrics) {
        this(new KafkaProducer<>(properties(settings)), settings.topic(), metrics);
    }

    // Public constructor accepting the Producer interface (rather than the concrete KafkaProducer)
    // so tests - including from other packages, e.g. KafkaRequestConsumerTest - can inject
    // org.apache.kafka.clients.producer.MockProducer.
    public DeadLetterPublisher(Producer<String, String> producer, String topic, EventPulseMetrics metrics) {
        this.producer = producer;
        this.topic = topic;
        this.metrics = metrics;
    }

    /**
     * Serializes the original record plus failure context into a {@link DeadLetterEnvelope} and
     * publishes it to the DLQ topic. Never throws for an async publish failure (that is logged and
     * recorded as a metric in the send callback); only throws {@link DeadLetterPublishException} if
     * the envelope itself could not be serialized.
     */
    public void publish(ConsumerRecord<String, String> record, ErrorCode errorCode, String errorMessage, int attempts) {
        metrics.recordDeadLetterQueued(errorCode);

        DeadLetterEnvelope envelope = new DeadLetterEnvelope(
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value(),
                errorCode.code(),
                errorMessage,
                attempts,
                Instant.now().toString()
        );

        String payload;
        try {
            payload = objectMapper.writeValueAsString(envelope);
        } catch (JsonProcessingException exception) {
            metrics.recordDeadLetterPublishFailure();
            throw new DeadLetterPublishException(
                    "Failed to serialize dead letter envelope for key=" + record.key(), exception);
        }

        producer.send(new ProducerRecord<>(topic, record.key(), payload), (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to publish dead letter key={} to topic={}", record.key(), topic, exception);
                metrics.recordDeadLetterPublishFailure();
            } else {
                log.info("Published dead letter key={} errorCode={} attempts={} to topic={} partition={} offset={}",
                        record.key(), errorCode.code(), attempts, metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

    private static Properties properties(DeadLetterQueueSettings settings) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, settings.clientId());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        return properties;
    }
}
