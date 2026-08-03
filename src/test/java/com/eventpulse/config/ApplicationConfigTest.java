package com.eventpulse.config;

import com.eventpulse.validation.ConfigurationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ApplicationConfigTest {
    @TempDir
    Path tempDir;

    private final Set<String> systemPropertiesToClear = new HashSet<>();

    @AfterEach
    void clearSystemProperties() {
        for (String key : systemPropertiesToClear) {
            System.clearProperty(key);
        }
        systemPropertiesToClear.clear();
    }

    @Test
    void fallsBackToBundledDefaultsWhenNoFileOrOverrideIsConfigured() throws Exception {
        ApplicationConfig config = ApplicationConfig.load(null, null);

        assertEquals("localhost:9092", config.get("kafka.bootstrap.servers", null));
        assertEquals("requests", config.get("kafka.topic", null));
        assertEquals(4, config.getInt("threadpool.core.size", -1));
        assertEquals(1000L, config.getLong("generator.interval.ms", -1));
    }

    @Test
    void externalFileOverridesBundledDefaultsForConfiguredKeysOnly() throws Exception {
        Path externalFile = writeProperties("""
                kafka.bootstrap.servers=broker.internal:9999
                threadpool.core.size=6
                """);

        ApplicationConfig config = ApplicationConfig.load(externalFile.toString(), null);

        assertEquals("broker.internal:9999", config.get("kafka.bootstrap.servers", null));
        assertEquals(6, config.getInt("threadpool.core.size", -1));
        // Untouched keys still fall back to the bundled defaults.
        assertEquals("requests", config.get("kafka.topic", null));
        assertEquals(8, config.getInt("threadpool.max.size", -1));
    }

    @Test
    void environmentVariableIsUsedWhenSystemPropertyPathIsNotSet() throws Exception {
        Path externalFile = writeProperties("kafka.topic=from-env-file\n");

        ApplicationConfig config = ApplicationConfig.load(null, externalFile.toString());

        assertEquals("from-env-file", config.get("kafka.topic", null));
    }

    @Test
    void systemPropertyPathTakesPrecedenceOverEnvironmentVariablePath() throws Exception {
        Path systemPropertyFile = writeProperties("kafka.topic=from-system-property-file\n", "sysprop.properties");
        Path envFile = writeProperties("kafka.topic=from-env-file\n", "env.properties");

        ApplicationConfig config = ApplicationConfig.load(systemPropertyFile.toString(), envFile.toString());

        assertEquals("from-system-property-file", config.get("kafka.topic", null));
    }

    @Test
    void systemPropertyOverridesExternalFileAndBundledDefaults() throws Exception {
        Path externalFile = writeProperties("kafka.bootstrap.servers=broker.internal:9999\n");
        setSystemProperty("kafka.bootstrap.servers", "override.from.jvm:1234");

        ApplicationConfig config = ApplicationConfig.load(externalFile.toString(), null);

        assertEquals("override.from.jvm:1234", config.get("kafka.bootstrap.servers", null));
    }

    @Test
    void systemPropertyOverridesBundledDefaultsWhenNoExternalFileIsConfigured() throws Exception {
        setSystemProperty("threadpool.max.size", "16");

        ApplicationConfig config = ApplicationConfig.load(null, null);

        assertEquals(16, config.getInt("threadpool.max.size", -1));
    }

    @Test
    void missingExternalFileThrowsConfigurationException() {
        Path missing = tempDir.resolve("does-not-exist.properties");

        assertThrows(ConfigurationException.class, () -> ApplicationConfig.load(missing.toString(), null));
    }

    @Test
    void nonNumericOverrideThrowsConfigurationExceptionOnRead() throws Exception {
        Path externalFile = writeProperties("threadpool.core.size=not-a-number\n");

        ApplicationConfig config = ApplicationConfig.load(externalFile.toString(), null);

        assertThrows(ConfigurationException.class, () -> config.getInt("threadpool.core.size", -1));
    }

    private Path writeProperties(String content) throws IOException {
        return writeProperties(content, "external.properties");
    }

    private Path writeProperties(String content, String fileName) throws IOException {
        Path file = tempDir.resolve(fileName);
        Files.writeString(file, content);
        return file;
    }

    private void setSystemProperty(String key, String value) {
        System.setProperty(key, value);
        systemPropertiesToClear.add(key);
    }
}
