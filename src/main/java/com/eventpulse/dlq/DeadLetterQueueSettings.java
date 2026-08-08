package com.eventpulse.dlq;

public record DeadLetterQueueSettings(
        String bootstrapServers,
        String topic,
        String clientId
) {
    public DeadLetterQueueSettings {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("bootstrapServers must not be blank");
        }
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        if (clientId == null || clientId.isBlank()) {
            throw new IllegalArgumentException("clientId must not be blank");
        }
    }
}
