package com.eventpulse.validation;

import com.eventpulse.parser.ParsedRequest;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RequestValidatorTest {
    private final RequestValidator validator = new RequestValidator(new ValidationRules(Map.of(
            "LOGIN", Map.of(
                    "user", new FieldRule(true, null, null, 1, 64)
            ),
            "UPLOAD", Map.of(
                    "size", new FieldRule(true, 1L, 1000L, null, null)
            )
    )));

    @Test
    void acceptsValidRequest() {
        ParsedRequest request = new ParsedRequest("LOGIN", Map.of("user", "alice"), "LOGIN|user=alice");

        assertDoesNotThrow(() -> validator.validate(request));
    }

    @Test
    void rejectsUnsupportedType() {
        ParsedRequest request = new ParsedRequest("PAYMENT", Map.of("amount", "1"), "PAYMENT|amount=1");

        assertThrows(ValidationException.class, () -> validator.validate(request));
    }

    @Test
    void rejectsMissingRequiredField() {
        ParsedRequest request = new ParsedRequest("LOGIN", Map.of("user", ""), "LOGIN|user=");

        assertThrows(ValidationException.class, () -> validator.validate(request));
    }

    @Test
    void rejectsNumericRangeViolation() {
        ParsedRequest request = new ParsedRequest("UPLOAD", Map.of("size", "2048"), "UPLOAD|size=2048");

        assertThrows(ValidationException.class, () -> validator.validate(request));
    }
}
