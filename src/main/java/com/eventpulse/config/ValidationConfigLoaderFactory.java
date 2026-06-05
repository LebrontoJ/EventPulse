package com.eventpulse.config;

import com.eventpulse.validation.ConfigurationException;
import com.eventpulse.validation.ValidationRules;

import java.nio.file.Path;

public final class ValidationConfigLoaderFactory {
    private ValidationConfigLoaderFactory() {
    }

    public static ValidationConfigLoader forPath(Path path) throws ConfigurationException {
        String fileName = path.getFileName().toString().toLowerCase();
        if (fileName.endsWith(".json")) {
            return new JsonValidationConfigLoader();
        }
        if (fileName.endsWith(".yml") || fileName.endsWith(".yaml")) {
            return new YamlValidationConfigLoader();
        }
        throw new ConfigurationException("Unsupported validation rules format: " + path);
    }
}
