package com.eventpulse.dlq;

import com.eventpulse.error.ErrorCode;
import com.eventpulse.error.HasErrorCode;

/** Thrown when a message could not even be serialized/routed to the dead letter queue. */
public class DeadLetterPublishException extends RuntimeException implements HasErrorCode {
    public DeadLetterPublishException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public ErrorCode errorCode() {
        return ErrorCode.DEAD_LETTER_PUBLISH_ERROR;
    }
}
