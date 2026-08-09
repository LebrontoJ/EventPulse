package com.eventpulse.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaSecuritySettingsTest {
    @Test
    void plaintextWithNoOtherFieldsOnlySetsTheProtocol() {
        KafkaSecuritySettings security = new KafkaSecuritySettings("PLAINTEXT", "", "", "", "", "", "", "");
        Properties properties = new Properties();

        security.applyTo(properties);

        assertEquals("PLAINTEXT", properties.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertNull(properties.getProperty(SaslConfigs.SASL_MECHANISM));
        assertNull(properties.getProperty(SaslConfigs.SASL_JAAS_CONFIG));
        assertNull(properties.getProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
        assertNull(properties.getProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
        assertNull(properties.getProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
        assertNull(properties.getProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
        assertNull(properties.getProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG));
    }

    @Test
    void nonBlankFieldsAreAllApplied() {
        KafkaSecuritySettings security = new KafkaSecuritySettings(
                "SASL_SSL",
                "PLAIN",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"u\" password=\"p\";",
                "/certs/truststore.jks",
                "trust-secret",
                "/certs/keystore.jks",
                "keystore-secret",
                "key-secret"
        );
        Properties properties = new Properties();

        security.applyTo(properties);

        assertEquals("SASL_SSL", properties.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertEquals("PLAIN", properties.getProperty(SaslConfigs.SASL_MECHANISM));
        assertTrue(properties.getProperty(SaslConfigs.SASL_JAAS_CONFIG).contains("PlainLoginModule"));
        assertEquals("/certs/truststore.jks", properties.getProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
        assertEquals("trust-secret", properties.getProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
        assertEquals("/certs/keystore.jks", properties.getProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
        assertEquals("keystore-secret", properties.getProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
        assertEquals("key-secret", properties.getProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG));
    }

    @Test
    void toStringRedactsSecretsButKeepsNonSensitiveFieldsReadable() {
        KafkaSecuritySettings security = new KafkaSecuritySettings(
                "SASL_SSL",
                "PLAIN",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"u\" password=\"p\";",
                "/certs/truststore.jks",
                "trust-secret",
                "/certs/keystore.jks",
                "keystore-secret",
                "key-secret"
        );

        String rendered = security.toString();

        assertTrue(rendered.contains("SASL_SSL"), "protocol should be visible");
        assertTrue(rendered.contains("PLAIN"), "sasl mechanism should be visible");
        assertTrue(rendered.contains("/certs/truststore.jks"), "non-secret paths should be visible");
        assertTrue(rendered.contains("/certs/keystore.jks"), "non-secret paths should be visible");
        assertFalse(rendered.contains("trust-secret"));
        assertFalse(rendered.contains("keystore-secret"));
        assertFalse(rendered.contains("key-secret"));
        assertFalse(rendered.contains("password=\"p\""), "the JAAS config's embedded password must not leak");
    }

    @Test
    void toStringMarksUnsetSecretsDistinctlyFromRedactedOnes() {
        KafkaSecuritySettings security = new KafkaSecuritySettings("PLAINTEXT", "", "", "", "", "", "", "");

        String rendered = security.toString();

        assertTrue(rendered.contains("(unset)"));
    }
}
