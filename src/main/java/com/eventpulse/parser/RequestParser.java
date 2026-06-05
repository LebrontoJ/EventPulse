package com.eventpulse.parser;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class RequestParser {
    private static final Pattern TYPE_PATTERN = Pattern.compile("[A-Z][A-Z0-9_]*");
    private static final Pattern KEY_PATTERN = Pattern.compile("[A-Za-z][A-Za-z0-9_]*");

    public ParsedRequest parse(String raw) throws RequestParseException {
        if (raw == null || raw.isBlank()) {
            throw new RequestParseException("Request must not be blank");
        }

        String[] parts = raw.split("\\|", -1);
        if (parts.length < 2) {
            throw new RequestParseException("Request must contain a type and at least one field");
        }

        String type = parts[0].trim();
        if (!TYPE_PATTERN.matcher(type).matches()) {
            throw new RequestParseException("Invalid request type: " + type);
        }

        Map<String, String> fields = new LinkedHashMap<>();
        for (int i = 1; i < parts.length; i++) {
            String segment = parts[i];
            int separator = segment.indexOf('=');
            if (separator <= 0) {
                throw new RequestParseException("Invalid field segment: " + segment);
            }

            String key = segment.substring(0, separator).trim();
            String value = segment.substring(separator + 1).trim();
            if (!KEY_PATTERN.matcher(key).matches()) {
                throw new RequestParseException("Invalid field key: " + key);
            }
            if (fields.containsKey(key)) {
                throw new RequestParseException("Duplicate field: " + key);
            }

            fields.put(key, value);
        }

        return new ParsedRequest(type, fields, raw);
    }
}
