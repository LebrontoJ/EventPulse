package com.eventpulse.validation;

import com.eventpulse.error.ErrorCode;
import com.eventpulse.error.HasErrorCode;

public class ConfigurationException extends Exception implements HasErrorCode {
    public ConfigurationException(String message) {
        super(message);
    }

    public ConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public ErrorCode errorCode() {
        return ErrorCode.CONFIGURATION_ERROR;
    }
}
