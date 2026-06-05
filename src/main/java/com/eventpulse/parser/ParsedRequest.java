package com.eventpulse.parser;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class ParsedRequest {
    private final String type;
    private final Map<String, String> fields;
    private final String raw;

    public ParsedRequest(String type, Map<String, String> fields, String raw) {
        this.type = Objects.requireNonNull(type, "type");
        this.fields = Collections.unmodifiableMap(new LinkedHashMap<>(fields));
        this.raw = Objects.requireNonNull(raw, "raw");
    }

    public String type() {
        return type;
    }

    public Map<String, String> fields() {
        return fields;
    }

    public String raw() {
        return raw;
    }
}
