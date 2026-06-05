package com.eventpulse.processor;

import com.eventpulse.config.ReloadableValidationRulesProvider;
import com.eventpulse.config.YamlValidationConfigLoader;
import com.eventpulse.parser.RequestParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RequestProcessorTest {
    @TempDir
    Path tempDir;

    @Test
    void classifiesSuccessParsingAndValidationFailures() throws Exception {
        Path rulesPath = tempDir.resolve("rules.yml");
        Files.writeString(rulesPath, """
                requestTypes:
                  LOGIN:
                    user:
                      required: true
                      minLength: 1
                  MESSAGE:
                    from:
                      required: true
                      minLength: 1
                    to:
                      required: true
                      minLength: 1
                    content:
                      required: true
                      minLength: 1
                """);
        RequestProcessor processor = new RequestProcessor(
                new RequestParser(),
                new ReloadableValidationRulesProvider(rulesPath, new YamlValidationConfigLoader())
        );

        assertEquals(ProcessingStatus.SUCCESS, processor.process("LOGIN|user=alice").status());
        assertEquals(ProcessingStatus.SUCCESS, processor.process("MESSAGE|from=bob|to=taylor|content=hello").status());
        assertEquals(ProcessingStatus.PARSING_ERROR, processor.process("LOGIN|user").status());
        assertEquals(ProcessingStatus.VALIDATION_ERROR, processor.process("LOGIN|user=").status());
        assertEquals(ProcessingStatus.VALIDATION_ERROR, processor.process("MESSAGE|from=bob|to=taylor").status());
    }
}
