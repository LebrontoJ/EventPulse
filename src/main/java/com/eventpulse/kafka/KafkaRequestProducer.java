package com.eventpulse.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaRequestProducer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(KafkaRequestProducer.class);

    private final Producer<String, String> producer;
    private final String topic;

    public KafkaRequestProducer(KafkaProducerSettings settings) {
        this(new KafkaProducer<>(properties(settings)), settings.topic());
    }

    // Package-private constructor accepting the Producer interface (rather than the concrete
    // KafkaProducer) so tests can inject org.apache.kafka.clients.producer.MockProducer.
    KafkaRequestProducer(Producer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    public void send(String request) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, request);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to produce request='{}'", request, exception);
                return;
            }
            log.info("Produced request topic={} partition={} offset={} value={}",
                    metadata.topic(), metadata.partition(), metadata.offset(), request);
        });
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

    private static Properties properties(KafkaProducerSettings settings) {
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
