package com.eventpulse.kafka;

public record KafkaConsumerSettings(
        String bootstrapServers,
        String topic,
        String groupId,
        int pollTimeoutMillis,
        int maxProcessingAttempts
) {
    public KafkaConsumerSettings {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("bootstrapServers must not be blank");
        }
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("groupId must not be blank");
        }
        if (pollTimeoutMillis < 1) {
            throw new IllegalArgumentException("pollTimeoutMillis must be >= 1");
        }
        if (maxProcessingAttempts < 1) {
            throw new IllegalArgumentException("maxProcessingAttempts must be >= 1");
        }
    }
}
