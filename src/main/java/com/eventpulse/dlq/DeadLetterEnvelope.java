package com.eventpulse.dlq;

/**
 * JSON payload published to the dead letter queue topic for a message that permanently failed
 * processing, carrying enough context to inspect or manually replay it later.
 */
public record DeadLetterEnvelope(
        String originalTopic,
        int originalPartition,
        long originalOffset,
        String key,
        String rawRequest,
        String errorCode,
        String errorMessage,
        int attempts,
        String failedAt
) {
}
