package com.eventpulse.error;

/**
 * Stable, precise identifiers for the different ways processing a request or operating the Kafka
 * pipeline can fail. Each code carries whether the underlying failure is worth retrying:
 * parsing/validation errors are deterministic (the same input will always fail the same way), so
 * retrying them wastes work, while runtime errors may be transient and are retried before a
 * message is sent to the dead letter queue.
 */
public enum ErrorCode {
    PARSE_ERROR("EP-1000", "Request could not be parsed", false),
    VALIDATION_ERROR("EP-2000", "Request failed validation rules", false),
    RUNTIME_ERROR("EP-3000", "Unexpected error while processing the request", true),
    CONFIGURATION_ERROR("EP-4000", "Application configuration could not be loaded", false),
    DEAD_LETTER_PUBLISH_ERROR("EP-5000", "Failed to publish a message to the dead letter queue", false);

    private final String code;
    private final String description;
    private final boolean retryable;

    ErrorCode(String code, String description, boolean retryable) {
        this.code = code;
        this.description = description;
        this.retryable = retryable;
    }

    public String code() {
        return code;
    }

    public String description() {
        return description;
    }

    /** Whether a failure with this code may succeed if attempted again, without any input change. */
    public boolean retryable() {
        return retryable;
    }
}
