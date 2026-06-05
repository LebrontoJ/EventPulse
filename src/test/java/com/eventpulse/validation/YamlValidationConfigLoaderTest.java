package com.eventpulse.validation;

import com.eventpulse.config.YamlValidationConfigLoader;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class YamlValidationConfigLoaderTest {
    private final YamlValidationConfigLoader loader = new YamlValidationConfigLoader();

    @Test
    void loadsYamlRules() throws Exception {
        String yaml = """
                requestTypes:
                  LOGIN:
                    user:
                      required: true
                      minLength: 1
                      maxLength: 64
                """;

        ValidationRules rules = loader.load(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)));

        assertEquals(1, rules.requestTypes().size());
        assertEquals(64, rules.rulesFor("LOGIN").get("user").maxLength());
    }

    @Test
    void rejectsInvalidSchema() {
        String yaml = """
                requestTypes:
                  LOGIN:
                    user:
                      min: 10
                      max: 1
                """;

        assertThrows(ConfigurationException.class,
                () -> loader.load(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8))));
    }
}
