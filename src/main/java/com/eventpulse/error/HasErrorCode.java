package com.eventpulse.error;

/**
 * Implemented by exceptions that can identify their failure with a precise {@link ErrorCode},
 * instead of callers having to infer the failure type from the exception's class or message.
 */
public interface HasErrorCode {
    ErrorCode errorCode();
}
