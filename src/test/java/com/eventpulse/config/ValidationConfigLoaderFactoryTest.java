package com.eventpulse.config;

import com.eventpulse.validation.ConfigurationException;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ValidationConfigLoaderFactoryTest {
    @Test
    void selectsJsonLoaderForJsonExtension() throws Exception {
        ValidationConfigLoader loader = ValidationConfigLoaderFactory.forPath(Path.of("rules.json"));

        assertInstanceOf(JsonValidationConfigLoader.class, loader);
    }

    @Test
    void selectsYamlLoaderForYmlExtension() throws Exception {
        ValidationConfigLoader loader = ValidationConfigLoaderFactory.forPath(Path.of("rules.yml"));

        assertInstanceOf(YamlValidationConfigLoader.class, loader);
    }

    @Test
    void selectsYamlLoaderForYamlExtension() throws Exception {
        ValidationConfigLoader loader = ValidationConfigLoaderFactory.forPath(Path.of("rules.yaml"));

        assertInstanceOf(YamlValidationConfigLoader.class, loader);
    }

    @Test
    void extensionMatchingIsCaseInsensitive() throws Exception {
        ValidationConfigLoader loader = ValidationConfigLoaderFactory.forPath(Path.of("rules.JSON"));

        assertInstanceOf(JsonValidationConfigLoader.class, loader);
    }

    @Test
    void rejectsUnsupportedExtension() {
        assertThrows(ConfigurationException.class,
                () -> ValidationConfigLoaderFactory.forPath(Path.of("rules.txt")));
    }
}
