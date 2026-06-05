package com.eventpulse.validation;

import java.util.List;

public class ValidationException extends Exception {
    private final List<String> violations;

    public ValidationException(List<String> violations) {
        super(String.join("; ", violations));
        this.violations = List.copyOf(violations);
    }

    public List<String> violations() {
        return violations;
    }
}
