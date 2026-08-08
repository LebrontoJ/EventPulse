package com.eventpulse.metrics;

import com.eventpulse.error.ErrorCode;
import com.eventpulse.processor.ProcessingStatus;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EventPulseMetricsTest {
    @Test
    void recordsProcessedRequestsByStatusIndependently() {
        EventPulseMetrics metrics = EventPulseMetrics.inMemory();

        metrics.recordProcessed(ProcessingStatus.SUCCESS, 1_000_000);
        metrics.recordProcessed(ProcessingStatus.SUCCESS, 2_000_000);
        metrics.recordProcessed(ProcessingStatus.PARSING_ERROR, 500_000);

        assertEquals(2.0, metrics.requestCount(ProcessingStatus.SUCCESS));
        assertEquals(1.0, metrics.requestCount(ProcessingStatus.PARSING_ERROR));
        assertEquals(0.0, metrics.requestCount(ProcessingStatus.VALIDATION_ERROR));
        assertEquals(0.0, metrics.requestCount(ProcessingStatus.RUNTIME_ERROR));
    }

    @Test
    void recordsKafkaCommitsAndFailuresIndependently() {
        EventPulseMetrics metrics = EventPulseMetrics.inMemory();

        metrics.recordKafkaCommit();
        metrics.recordKafkaCommit();
        metrics.recordKafkaCommitFailure();

        assertEquals(2.0, metrics.kafkaCommitCount());
        assertEquals(1.0, metrics.kafkaCommitFailureCount());
    }

    @Test
    void recordsGeneratedRequests() {
        EventPulseMetrics metrics = EventPulseMetrics.inMemory();

        metrics.recordGeneratedRequest();
        metrics.recordGeneratedRequest();
        metrics.recordGeneratedRequest();

        assertEquals(3.0, metrics.generatedRequestCount());
    }

    @Test
    void threadPoolGaugesReadTheSuppliedValuesAtScrapeTime() {
        EventPulseMetrics metrics = EventPulseMetrics.inMemory();
        AtomicInteger active = new AtomicInteger(2);
        AtomicInteger queued = new AtomicInteger(5);
        AtomicLong completed = new AtomicLong(42);

        metrics.bindThreadPool(active::get, queued::get, completed::get);

        assertEquals(2.0, gaugeValue(metrics.registry(), "eventpulse_threadpool_active_threads"));
        assertEquals(5.0, gaugeValue(metrics.registry(), "eventpulse_threadpool_queued_tasks"));
        assertEquals(42.0, gaugeValue(metrics.registry(), "eventpulse_threadpool_completed_tasks"));

        // Gauges are callback-based: each scrape re-reads the live supplier value, it is not a
        // value that was snapshotted once at bind() time.
        active.set(9);
        assertEquals(9.0, gaugeValue(metrics.registry(), "eventpulse_threadpool_active_threads"));
    }

    @Test
    void recordsDeadLetterQueuedByErrorCodeIndependently() {
        EventPulseMetrics metrics = EventPulseMetrics.inMemory();

        metrics.recordDeadLetterQueued(ErrorCode.PARSE_ERROR);
        metrics.recordDeadLetterQueued(ErrorCode.PARSE_ERROR);
        metrics.recordDeadLetterQueued(ErrorCode.VALIDATION_ERROR);

        assertEquals(2.0, metrics.deadLetterCount(ErrorCode.PARSE_ERROR));
        assertEquals(1.0, metrics.deadLetterCount(ErrorCode.VALIDATION_ERROR));
        assertEquals(0.0, metrics.deadLetterCount(ErrorCode.RUNTIME_ERROR));
    }

    @Test
    void recordsDeadLetterPublishFailures() {
        EventPulseMetrics metrics = EventPulseMetrics.inMemory();

        metrics.recordDeadLetterPublishFailure();

        assertEquals(1.0, metrics.deadLetterPublishFailureCount());
    }

    @Test
    void recordsProcessingRetries() {
        EventPulseMetrics metrics = EventPulseMetrics.inMemory();

        metrics.recordProcessingRetries(2);
        metrics.recordProcessingRetries(1);

        assertEquals(3.0, metrics.processingRetryCount());
    }

    @Test
    void recordsRebalanceEventsByTypeIndependently() {
        EventPulseMetrics metrics = EventPulseMetrics.inMemory();

        metrics.recordRebalanceEvent("revoked");
        metrics.recordRebalanceEvent("assigned");
        metrics.recordRebalanceEvent("assigned");

        assertEquals(1.0, metrics.rebalanceEventCount("revoked"));
        assertEquals(2.0, metrics.rebalanceEventCount("assigned"));
    }

    @Test
    void closeIsSafeToCallRepeatedlyWithoutAnHttpServer() {
        EventPulseMetrics metrics = EventPulseMetrics.inMemory();

        metrics.close();
        metrics.close();
    }

    private static double gaugeValue(PrometheusRegistry registry, String name) {
        return registry.scrape().stream()
                .filter(snapshot -> snapshot.getMetadata().getName().equals(name))
                .findFirst()
                .map(snapshot -> (GaugeSnapshot) snapshot)
                .map(snapshot -> snapshot.getDataPoints().get(0).getValue())
                .orElseThrow(() -> new AssertionError("gauge not registered: " + name));
    }
}
