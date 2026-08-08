package com.eventpulse.validation;

import com.eventpulse.error.ErrorCode;
import com.eventpulse.error.HasErrorCode;

import java.util.List;

public class ValidationException extends Exception implements HasErrorCode {
    private final List<String> violations;

    public ValidationException(List<String> violations) {
        super(String.join("; ", violations));
        this.violations = List.copyOf(violations);
    }

    public List<String> violations() {
        return violations;
    }

    @Override
    public ErrorCode errorCode() {
        return ErrorCode.VALIDATION_ERROR;
    }
}
