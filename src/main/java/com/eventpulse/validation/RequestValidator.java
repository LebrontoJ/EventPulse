package com.eventpulse.validation;

import com.eventpulse.parser.ParsedRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RequestValidator {
    private final ValidationRules rules;

    public RequestValidator(ValidationRules rules) {
        this.rules = rules;
    }

    public void validate(ParsedRequest request) throws ValidationException {
        List<String> violations = new ArrayList<>();
        if (!rules.hasType(request.type())) {
            throw new ValidationException(List.of("Unsupported request type: " + request.type()));
        }

        Map<String, FieldRule> fieldRules = rules.rulesFor(request.type());
        for (Map.Entry<String, FieldRule> entry : fieldRules.entrySet()) {
            String fieldName = entry.getKey();
            FieldRule rule = entry.getValue();
            String value = request.fields().get(fieldName);

            if (rule.required() && (value == null || value.isBlank())) {
                violations.add(fieldName + " is required");
                continue;
            }
            if (value == null || value.isBlank()) {
                continue;
            }

            validateLength(fieldName, value, rule, violations);
            validateRange(fieldName, value, rule, violations);
        }

        if (!violations.isEmpty()) {
            throw new ValidationException(violations);
        }
    }

    private void validateLength(String fieldName, String value, FieldRule rule, List<String> violations) {
        if (rule.minLength() != null && value.length() < rule.minLength()) {
            violations.add(fieldName + " length must be >= " + rule.minLength());
        }
        if (rule.maxLength() != null && value.length() > rule.maxLength()) {
            violations.add(fieldName + " length must be <= " + rule.maxLength());
        }
    }

    private void validateRange(String fieldName, String value, FieldRule rule, List<String> violations) {
        if (rule.min() == null && rule.max() == null) {
            return;
        }

        long numericValue;
        try {
            numericValue = Long.parseLong(value);
        } catch (NumberFormatException exception) {
            violations.add(fieldName + " must be numeric");
            return;
        }

        if (rule.min() != null && numericValue < rule.min()) {
            violations.add(fieldName + " must be >= " + rule.min());
        }
        if (rule.max() != null && numericValue > rule.max()) {
            violations.add(fieldName + " must be <= " + rule.max());
        }
    }
}
