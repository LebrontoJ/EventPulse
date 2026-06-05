package com.eventpulse.generator;

public record RequestGeneratorSettings(
        long intervalMillis,
        int invalidRequestRatePercent,
        long maxRequests
) {
    public RequestGeneratorSettings {
        if (intervalMillis < 1) {
            throw new IllegalArgumentException("intervalMillis must be >= 1");
        }
        if (invalidRequestRatePercent < 0 || invalidRequestRatePercent > 100) {
            throw new IllegalArgumentException("invalidRequestRatePercent must be between 0 and 100");
        }
        if (maxRequests < 0) {
            throw new IllegalArgumentException("maxRequests must be >= 0");
        }
    }
}
