package com.eventpulse.threading;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ThreadPoolSettingsTest {
    @Test
    void storesValidSettings() {
        ThreadPoolSettings settings = new ThreadPoolSettings(4, 8, 1000);

        assertEquals(4, settings.corePoolSize());
        assertEquals(8, settings.maximumPoolSize());
        assertEquals(1000, settings.queueCapacity());
    }

    @Test
    void rejectsCorePoolSizeBelowOne() {
        assertThrows(IllegalArgumentException.class, () -> new ThreadPoolSettings(0, 8, 1000));
    }

    @Test
    void rejectsMaximumPoolSizeBelowCorePoolSize() {
        assertThrows(IllegalArgumentException.class, () -> new ThreadPoolSettings(4, 3, 1000));
    }

    @Test
    void allowsMaximumPoolSizeEqualToCorePoolSize() {
        ThreadPoolSettings settings = new ThreadPoolSettings(4, 4, 1000);

        assertEquals(4, settings.maximumPoolSize());
    }

    @Test
    void rejectsQueueCapacityBelowOne() {
        assertThrows(IllegalArgumentException.class, () -> new ThreadPoolSettings(4, 8, 0));
    }
}
