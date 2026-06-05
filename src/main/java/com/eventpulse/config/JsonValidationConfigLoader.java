package com.eventpulse.config;

import com.eventpulse.validation.ConfigurationException;
import com.eventpulse.validation.ValidationRules;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class JsonValidationConfigLoader implements ValidationConfigLoader {
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public ValidationRules load(Path path) throws ConfigurationException {
        try (InputStream inputStream = Files.newInputStream(path)) {
            return load(inputStream);
        } catch (IOException exception) {
            throw new ConfigurationException("Unable to read validation rules: " + path, exception);
        }
    }

    public ValidationRules load(InputStream inputStream) throws ConfigurationException {
        try {
            Map<String, Object> loaded = objectMapper.readValue(inputStream, MAP_TYPE);
            return ValidationConfigParser.parse(loaded);
        } catch (ConfigurationException exception) {
            throw exception;
        } catch (RuntimeException | IOException exception) {
            throw new ConfigurationException("Invalid JSON validation rules", exception);
        }
    }
}
