package com.eventpulse.config;

import com.eventpulse.validation.ConfigurationException;
import com.eventpulse.validation.FieldRule;
import com.eventpulse.validation.ValidationRules;

import java.util.LinkedHashMap;
import java.util.Map;

final class ValidationConfigParser {
    private ValidationConfigParser() {
    }

    static ValidationRules parse(Object loaded) throws ConfigurationException {
        if (!(loaded instanceof Map<?, ?> root)) {
            throw new ConfigurationException("Validation rules must be an object");
        }

        Object requestTypesNode = root.get("requestTypes");
        if (!(requestTypesNode instanceof Map<?, ?> requestTypes) || requestTypes.isEmpty()) {
            throw new ConfigurationException("Validation rules must define requestTypes");
        }

        Map<String, Map<String, FieldRule>> parsedTypes = new LinkedHashMap<>();
        for (Map.Entry<?, ?> typeEntry : requestTypes.entrySet()) {
            String type = requireName(typeEntry.getKey(), "request type");
            if (!(typeEntry.getValue() instanceof Map<?, ?> fieldsNode)) {
                throw new ConfigurationException("Request type " + type + " must define fields");
            }

            Map<String, FieldRule> parsedFields = new LinkedHashMap<>();
            for (Map.Entry<?, ?> fieldEntry : fieldsNode.entrySet()) {
                String fieldName = requireName(fieldEntry.getKey(), "field name");
                if (!(fieldEntry.getValue() instanceof Map<?, ?> ruleNode)) {
                    throw new ConfigurationException("Field " + type + "." + fieldName + " must define rules");
                }
                parsedFields.put(fieldName, parseFieldRule(type, fieldName, ruleNode));
            }
            parsedTypes.put(type, parsedFields);
        }

        return new ValidationRules(parsedTypes);
    }

    private static FieldRule parseFieldRule(String type, String fieldName, Map<?, ?> ruleNode) throws ConfigurationException {
        Object requiredNode = ruleNode.containsKey("required") ? ruleNode.get("required") : Boolean.FALSE;
        boolean required = booleanValue(requiredNode, type, fieldName, "required");
        Long min = longValue(ruleNode.get("min"), type, fieldName, "min");
        Long max = longValue(ruleNode.get("max"), type, fieldName, "max");
        Integer minLength = integerValue(ruleNode.get("minLength"), type, fieldName, "minLength");
        Integer maxLength = integerValue(ruleNode.get("maxLength"), type, fieldName, "maxLength");

        if (min != null && max != null && min > max) {
            throw new ConfigurationException(type + "." + fieldName + " min must be <= max");
        }
        if (minLength != null && maxLength != null && minLength > maxLength) {
            throw new ConfigurationException(type + "." + fieldName + " minLength must be <= maxLength");
        }

        return new FieldRule(required, min, max, minLength, maxLength);
    }

    private static String requireName(Object value, String label) throws ConfigurationException {
        if (!(value instanceof String name) || name.isBlank()) {
            throw new ConfigurationException("Invalid " + label);
        }
        return name;
    }

    private static boolean booleanValue(Object value, String type, String fieldName, String property) throws ConfigurationException {
        if (value instanceof Boolean booleanValue) {
            return booleanValue;
        }
        throw new ConfigurationException(type + "." + fieldName + "." + property + " must be a boolean");
    }

    private static Long longValue(Object value, String type, String fieldName, String property) throws ConfigurationException {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        throw new ConfigurationException(type + "." + fieldName + "." + property + " must be numeric");
    }

    private static Integer integerValue(Object value, String type, String fieldName, String property) throws ConfigurationException {
        Long longValue = longValue(value, type, fieldName, property);
        if (longValue == null) {
            return null;
        }
        if (longValue < 0 || longValue > Integer.MAX_VALUE) {
            throw new ConfigurationException(type + "." + fieldName + "." + property + " is out of range");
        }
        return longValue.intValue();
    }
}
