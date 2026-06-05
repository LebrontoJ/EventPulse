package com.eventpulse.threading;

public record ThreadPoolSettings(int corePoolSize, int maximumPoolSize, int queueCapacity) {
    public ThreadPoolSettings {
        if (corePoolSize < 1) {
            throw new IllegalArgumentException("corePoolSize must be >= 1");
        }
        if (maximumPoolSize < corePoolSize) {
            throw new IllegalArgumentException("maximumPoolSize must be >= corePoolSize");
        }
        if (queueCapacity < 1) {
            throw new IllegalArgumentException("queueCapacity must be >= 1");
        }
    }
}
