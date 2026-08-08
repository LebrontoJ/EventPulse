package com.eventpulse.generator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RequestGeneratorSettingsTest {
    @Test
    void storesValidSettings() {
        RequestGeneratorSettings settings = new RequestGeneratorSettings(500, 10, 100);

        assertEquals(500, settings.intervalMillis());
        assertEquals(10, settings.invalidRequestRatePercent());
        assertEquals(100, settings.maxRequests());
    }

    @Test
    void allowsUnboundedMaxRequestsOfZero() {
        RequestGeneratorSettings settings = new RequestGeneratorSettings(500, 10, 0);

        assertEquals(0, settings.maxRequests());
    }

    @Test
    void rejectsNonPositiveInterval() {
        assertThrows(IllegalArgumentException.class, () -> new RequestGeneratorSettings(0, 10, 0));
    }

    @Test
    void rejectsNegativeInvalidRate() {
        assertThrows(IllegalArgumentException.class, () -> new RequestGeneratorSettings(500, -1, 0));
    }

    @Test
    void rejectsInvalidRateAboveOneHundred() {
        assertThrows(IllegalArgumentException.class, () -> new RequestGeneratorSettings(500, 101, 0));
    }

    @Test
    void rejectsNegativeMaxRequests() {
        assertThrows(IllegalArgumentException.class, () -> new RequestGeneratorSettings(500, 10, -1));
    }
}
