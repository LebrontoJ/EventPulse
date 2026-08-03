package com.eventpulse.threading;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class RequestProcessingExecutorTest {
    @Test
    @Timeout(5)
    void executesSubmittedTasksAndTracksCompletedCount() throws Exception {
        try (RequestProcessingExecutor executor = new RequestProcessingExecutor(new ThreadPoolSettings(2, 2, 10))) {
            CountDownLatch latch = new CountDownLatch(3);
            for (int i = 0; i < 3; i++) {
                executor.submit(latch::countDown);
            }

            assertTrue(latch.await(2, TimeUnit.SECONDS), "all submitted tasks should run");
            awaitTrue(() -> executor.completedTaskCount() == 3);
            assertEquals(3, executor.completedTaskCount());
        }
    }

    @Test
    @Timeout(5)
    void queuedTaskCountReflectsPendingWorkWhenPoolIsSaturated() throws Exception {
        try (RequestProcessingExecutor executor = new RequestProcessingExecutor(new ThreadPoolSettings(1, 1, 10))) {
            CountDownLatch taskStarted = new CountDownLatch(1);
            CountDownLatch releaseTask = new CountDownLatch(1);

            executor.submit(() -> {
                taskStarted.countDown();
                await(releaseTask);
            });
            assertTrue(taskStarted.await(2, TimeUnit.SECONDS), "the blocking task should start immediately");
            awaitTrue(() -> executor.activeThreadCount() == 1);
            assertEquals(1, executor.activeThreadCount());

            executor.submit(() -> {
            });
            executor.submit(() -> {
            });
            awaitTrue(() -> executor.queuedTaskCount() == 2);
            assertEquals(2, executor.queuedTaskCount());

            releaseTask.countDown();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(2, TimeUnit.SECONDS)) {
                fail("latch was not released in time");
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            fail("interrupted while waiting for latch");
        }
    }

    private static void awaitTrue(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 2000;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                return;
            }
            Thread.sleep(20);
        }
    }
}
