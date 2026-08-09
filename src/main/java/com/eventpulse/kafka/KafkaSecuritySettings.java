package com.eventpulse.kafka;

import com.eventpulse.config.ApplicationConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Properties;

/**
 * Kafka client security configuration - SASL and/or SSL - shared by the consumer, producer, and
 * dead letter queue producer, since they all connect to the same cluster the same way.
 *
 * <p>Every field defaults to blank except {@code protocol}, which defaults to {@code PLAINTEXT} -
 * unchanged, zero-config behavior for local development against an unauthenticated broker. Only
 * non-blank fields are applied to a client's {@link Properties}, so setting e.g. just
 * {@code kafka.security.protocol=SASL_SSL} plus the SASL keys, without any SSL keystore keys,
 * works exactly as it would passing those same properties to the Kafka client directly.
 */
public record KafkaSecuritySettings(
        String protocol,
        String saslMechanism,
        String saslJaasConfig,
        String sslTruststoreLocation,
        String sslTruststorePassword,
        String sslKeystoreLocation,
        String sslKeystorePassword,
        String sslKeyPassword
) {
    public static KafkaSecuritySettings fromConfig(ApplicationConfig config) {
        return new KafkaSecuritySettings(
                config.get("kafka.security.protocol", "PLAINTEXT"),
                config.get("kafka.sasl.mechanism", ""),
                config.get("kafka.sasl.jaas.config", ""),
                config.get("kafka.ssl.truststore.location", ""),
                config.get("kafka.ssl.truststore.password", ""),
                config.get("kafka.ssl.keystore.location", ""),
                config.get("kafka.ssl.keystore.password", ""),
                config.get("kafka.ssl.key.password", "")
        );
    }

    /** Only sets properties for non-blank fields, so PLAINTEXT-with-nothing-else is a no-op. */
    public void applyTo(Properties properties) {
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol);
        putIfNotBlank(properties, SaslConfigs.SASL_MECHANISM, saslMechanism);
        putIfNotBlank(properties, SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
        putIfNotBlank(properties, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslTruststoreLocation);
        putIfNotBlank(properties, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslTruststorePassword);
        putIfNotBlank(properties, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslKeystoreLocation);
        putIfNotBlank(properties, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslKeystorePassword);
        putIfNotBlank(properties, SslConfigs.SSL_KEY_PASSWORD_CONFIG, sslKeyPassword);
    }

    private static void putIfNotBlank(Properties properties, String key, String value) {
        if (value != null && !value.isBlank()) {
            properties.put(key, value);
        }
    }

    /**
     * Redacts every secret (the JAAS config embeds the SASL password; the three password fields
     * are self-explanatory) so this is safe to include in a log line or exception message without
     * leaking credentials.
     */
    @Override
    public String toString() {
        return "KafkaSecuritySettings[protocol=" + protocol
                + ", saslMechanism=" + saslMechanism
                + ", saslJaasConfig=" + redact(saslJaasConfig)
                + ", sslTruststoreLocation=" + sslTruststoreLocation
                + ", sslTruststorePassword=" + redact(sslTruststorePassword)
                + ", sslKeystoreLocation=" + sslKeystoreLocation
                + ", sslKeystorePassword=" + redact(sslKeystorePassword)
                + ", sslKeyPassword=" + redact(sslKeyPassword)
                + "]";
    }

    private static String redact(String value) {
        return value == null || value.isBlank() ? "(unset)" : "****";
    }
}
