package com.eventpulse.validation;

import com.eventpulse.config.JsonValidationConfigLoader;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JsonValidationConfigLoaderTest {
    private final JsonValidationConfigLoader loader = new JsonValidationConfigLoader();

    @Test
    void loadsJsonRules() throws Exception {
        String json = """
                {
                  "requestTypes": {
                    "MESSAGE": {
                      "content": {
                        "required": true,
                        "minLength": 1,
                        "maxLength": 2048
                      }
                    }
                  }
                }
                """;

        ValidationRules rules = loader.load(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));

        assertEquals(2048, rules.rulesFor("MESSAGE").get("content").maxLength());
    }

    @Test
    void rejectsInvalidJson() {
        String json = """
                {
                  "requestTypes": {
                    "UPLOAD": {
                      "size": {
                        "min": 100,
                        "max": 1
                      }
                    }
                  }
                }
                """;

        assertThrows(ConfigurationException.class,
                () -> loader.load(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))));
    }
}
