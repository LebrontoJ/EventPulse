package com.eventpulse.config;

import com.eventpulse.validation.ConfigurationException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Layered runtime configuration for EventPulse's application entry points.
 *
 * <p>Values are resolved, from lowest to highest precedence:
 * <ol>
 *     <li>the bundled {@code application.properties} classpath resource</li>
 *     <li>an external properties file, if one is configured via the
 *         {@value #CONFIG_FILE_SYSTEM_PROPERTY} system property or the
 *         {@value #CONFIG_FILE_ENV_VARIABLE} environment variable</li>
 *     <li>individual {@code -D} system properties matching a known key</li>
 * </ol>
 *
 * <p>Only keys already present in the bundled defaults file can be overridden by an
 * external file or a system property; this keeps the set of recognized settings in one
 * place and documented in {@code application.properties}.
 */
public final class ApplicationConfig {
    public static final String CONFIG_FILE_SYSTEM_PROPERTY = "config.file";
    public static final String CONFIG_FILE_ENV_VARIABLE = "EVENTPULSE_CONFIG_FILE";

    private static final String DEFAULT_CLASSPATH_RESOURCE = "/application.properties";

    private final Properties properties;

    private ApplicationConfig(Properties properties) {
        this.properties = properties;
    }

    public static ApplicationConfig load() throws ConfigurationException {
        return load(
                System.getProperty(CONFIG_FILE_SYSTEM_PROPERTY),
                System.getenv(CONFIG_FILE_ENV_VARIABLE)
        );
    }

    static ApplicationConfig load(String externalConfigFileSystemProperty,
                                   String externalConfigFileEnvVariable) throws ConfigurationException {
        Properties merged = new Properties();
        loadClasspathDefaults(merged);

        String externalPath = firstNonBlank(externalConfigFileSystemProperty, externalConfigFileEnvVariable);
        if (externalPath != null) {
            loadExternalFile(merged, Path.of(externalPath));
        }

        applySystemPropertyOverrides(merged);
        return new ApplicationConfig(merged);
    }

    private static void loadClasspathDefaults(Properties target) throws ConfigurationException {
        try (InputStream in = ApplicationConfig.class.getResourceAsStream(DEFAULT_CLASSPATH_RESOURCE)) {
            if (in == null) {
                throw new ConfigurationException("Default application.properties resource is missing");
            }
            target.load(in);
        } catch (IOException exception) {
            throw new ConfigurationException("Failed to read default application.properties resource", exception);
        }
    }

    private static void loadExternalFile(Properties target, Path path) throws ConfigurationException {
        if (!Files.isRegularFile(path)) {
            throw new ConfigurationException("Configured config file does not exist: " + path);
        }
        try (InputStream in = Files.newInputStream(path)) {
            Properties external = new Properties();
            external.load(in);
            target.putAll(external);
        } catch (IOException exception) {
            throw new ConfigurationException("Failed to read config file: " + path, exception);
        }
    }

    private static void applySystemPropertyOverrides(Properties target) {
        for (String key : target.stringPropertyNames()) {
            String override = System.getProperty(key);
            if (override != null) {
                target.setProperty(key, override);
            }
        }
    }

    private static String firstNonBlank(String a, String b) {
        if (a != null && !a.isBlank()) {
            return a;
        }
        if (b != null && !b.isBlank()) {
            return b;
        }
        return null;
    }

    public String get(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public int getInt(String key, int defaultValue) throws ConfigurationException {
        String value = get(key, Integer.toString(defaultValue));
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException exception) {
            throw new ConfigurationException(key + " must be an integer, was: " + value, exception);
        }
    }

    public long getLong(String key, long defaultValue) throws ConfigurationException {
        String value = get(key, Long.toString(defaultValue));
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException exception) {
            throw new ConfigurationException(key + " must be a number, was: " + value, exception);
        }
    }
}
