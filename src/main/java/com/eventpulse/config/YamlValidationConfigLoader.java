package com.eventpulse.config;

import com.eventpulse.validation.ConfigurationException;
import com.eventpulse.validation.ValidationRules;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class YamlValidationConfigLoader implements ValidationConfigLoader {
    @Override
    public ValidationRules load(Path path) throws ConfigurationException {
        try (InputStream inputStream = Files.newInputStream(path)) {
            return load(inputStream);
        } catch (IOException exception) {
            throw new ConfigurationException("Unable to read validation rules: " + path, exception);
        }
    }

    public ValidationRules load(InputStream inputStream) throws ConfigurationException {
        Object loaded;
        try {
            loaded = new Yaml().load(inputStream);
        } catch (RuntimeException exception) {
            throw new ConfigurationException("Invalid YAML validation rules", exception);
        }

        return ValidationConfigParser.parse(loaded);
    }
}
