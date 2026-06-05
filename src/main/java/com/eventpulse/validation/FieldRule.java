package com.eventpulse.validation;

public record FieldRule(
        boolean required,
        Long min,
        Long max,
        Integer minLength,
        Integer maxLength
) {
}
