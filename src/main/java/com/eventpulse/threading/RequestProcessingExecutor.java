package com.eventpulse.threading;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class RequestProcessingExecutor implements AutoCloseable {
    private final ThreadPoolExecutor executor;

    public RequestProcessingExecutor(ThreadPoolSettings settings) {
        this.executor = new ThreadPoolExecutor(
                settings.corePoolSize(),
                settings.maximumPoolSize(),
                30,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(settings.queueCapacity()),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    public Future<?> submit(Runnable task) {
        return executor.submit(task);
    }

    public int activeThreadCount() {
        return executor.getActiveCount();
    }

    public int queuedTaskCount() {
        return executor.getQueue().size();
    }

    public long completedTaskCount() {
        return executor.getCompletedTaskCount();
    }

    @Override
    public void close() {
        executor.shutdown();
    }
}
