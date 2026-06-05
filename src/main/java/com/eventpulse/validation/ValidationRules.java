package com.eventpulse.validation;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class ValidationRules {
    private final Map<String, Map<String, FieldRule>> requestTypes;

    public ValidationRules(Map<String, Map<String, FieldRule>> requestTypes) {
        Objects.requireNonNull(requestTypes, "requestTypes");
        Map<String, Map<String, FieldRule>> copy = new LinkedHashMap<>();
        requestTypes.forEach((type, fields) ->
                copy.put(type, Collections.unmodifiableMap(new LinkedHashMap<>(fields))));
        this.requestTypes = Collections.unmodifiableMap(copy);
    }

    public Map<String, Map<String, FieldRule>> requestTypes() {
        return requestTypes;
    }

    public boolean hasType(String type) {
        return requestTypes.containsKey(type);
    }

    public Map<String, FieldRule> rulesFor(String type) {
        return requestTypes.getOrDefault(type, Map.of());
    }
}
