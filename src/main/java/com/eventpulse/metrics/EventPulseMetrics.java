package com.eventpulse.metrics;

import com.eventpulse.processor.ProcessingStatus;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.GaugeWithCallback;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Locale;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

/**
 * Collects EventPulse's Prometheus metrics and, when enabled, exposes them over HTTP at
 * {@code /metrics}.
 *
 * <p>Metrics exposed:
 * <ul>
 *     <li>{@code eventpulse_requests_total{status}} - requests processed, by outcome</li>
 *     <li>{@code eventpulse_request_processing_duration_seconds} - parse+validate latency</li>
 *     <li>{@code eventpulse_kafka_commits_total} / {@code eventpulse_kafka_commit_failures_total}</li>
 *     <li>{@code eventpulse_generator_requests_sent_total} - requests sent by the generator</li>
 *     <li>{@code eventpulse_threadpool_active_threads} / {@code _queued_tasks} / {@code _completed_tasks}</li>
 *     <li>standard {@code jvm_*} / {@code process_*} metrics via {@link JvmMetrics}</li>
 * </ul>
 */
public final class EventPulseMetrics implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(EventPulseMetrics.class);

    private final PrometheusRegistry registry;
    private final HTTPServer httpServer;
    private final Counter requestsTotal;
    private final Histogram processingDurationSeconds;
    private final Counter kafkaCommitsTotal;
    private final Counter kafkaCommitFailuresTotal;
    private final Counter generatorRequestsTotal;

    private EventPulseMetrics(PrometheusRegistry registry, HTTPServer httpServer) {
        this.registry = registry;
        this.httpServer = httpServer;
        this.requestsTotal = Counter.builder()
                .name("eventpulse_requests_total")
                .help("Total number of requests processed, by outcome")
                .labelNames("status")
                .register(registry);
        this.processingDurationSeconds = Histogram.builder()
                .name("eventpulse_request_processing_duration_seconds")
                .help("Time spent parsing and validating a single request")
                .unit(Unit.SECONDS)
                .register(registry);
        this.kafkaCommitsTotal = Counter.builder()
                .name("eventpulse_kafka_commits_total")
                .help("Total number of successful Kafka consumer offset commits")
                .register(registry);
        this.kafkaCommitFailuresTotal = Counter.builder()
                .name("eventpulse_kafka_commit_failures_total")
                .help("Total number of failed Kafka consumer offset commits")
                .register(registry);
        this.generatorRequestsTotal = Counter.builder()
                .name("eventpulse_generator_requests_sent_total")
                .help("Total number of requests sent by the request generator")
                .register(registry);
    }

    /**
     * Creates metrics backed by a fresh registry and, if {@code enabled}, starts an HTTP server
     * exposing them at {@code http://<host>:<port>/metrics}.
     */
    public static EventPulseMetrics start(boolean enabled, int port) throws IOException {
        PrometheusRegistry registry = new PrometheusRegistry();
        HTTPServer httpServer = null;
        if (enabled) {
            JvmMetrics.builder().register(registry);
            httpServer = HTTPServer.builder()
                    .port(port)
                    .registry(registry)
                    .buildAndStart();
            log.info("Prometheus metrics available at http://localhost:{}/metrics", httpServer.getPort());
        } else {
            log.info("Prometheus metrics HTTP server disabled (metrics.enabled=false)");
        }
        return new EventPulseMetrics(registry, httpServer);
    }

    /**
     * Creates metrics backed by an isolated in-memory registry with no HTTP server.
     * Intended for tests, so each test can assert on its own registry without port conflicts
     * or interference from the global {@code PrometheusRegistry.defaultRegistry}.
     */
    public static EventPulseMetrics inMemory() {
        return new EventPulseMetrics(new PrometheusRegistry(), null);
    }

    public PrometheusRegistry registry() {
        return registry;
    }

    public void recordProcessed(ProcessingStatus status, long durationNanos) {
        requestsTotal.labelValues(status.name().toLowerCase(Locale.ROOT)).inc();
        processingDurationSeconds.observe(Unit.nanosToSeconds(durationNanos));
    }

    public void recordKafkaCommit() {
        kafkaCommitsTotal.inc();
    }

    public void recordKafkaCommitFailure() {
        kafkaCommitFailuresTotal.inc();
    }

    public void recordGeneratedRequest() {
        generatorRequestsTotal.inc();
    }

    /** Current value of {@code eventpulse_requests_total} for the given status. Mainly for tests. */
    public double requestCount(ProcessingStatus status) {
        return requestsTotal.labelValues(status.name().toLowerCase(Locale.ROOT)).get();
    }

    /** Current value of {@code eventpulse_kafka_commits_total}. Mainly for tests. */
    public double kafkaCommitCount() {
        return kafkaCommitsTotal.get();
    }

    /** Current value of {@code eventpulse_kafka_commit_failures_total}. Mainly for tests. */
    public double kafkaCommitFailureCount() {
        return kafkaCommitFailuresTotal.get();
    }

    /** Current value of {@code eventpulse_generator_requests_sent_total}. Mainly for tests. */
    public double generatedRequestCount() {
        return generatorRequestsTotal.get();
    }

    /**
     * Registers gauges that read the given thread pool's live active/queued/completed counts
     * at scrape time. Call at most once per {@code EventPulseMetrics} instance (registering the
     * same gauge name twice on one registry throws {@link IllegalArgumentException}).
     */
    public void bindThreadPool(IntSupplier activeThreads, IntSupplier queuedTasks, LongSupplier completedTasks) {
        GaugeWithCallback.builder()
                .name("eventpulse_threadpool_active_threads")
                .help("Number of thread pool threads actively executing a task")
                .callback(callback -> callback.call(activeThreads.getAsInt()))
                .register(registry);
        GaugeWithCallback.builder()
                .name("eventpulse_threadpool_queued_tasks")
                .help("Number of tasks waiting in the thread pool queue")
                .callback(callback -> callback.call(queuedTasks.getAsInt()))
                .register(registry);
        GaugeWithCallback.builder()
                .name("eventpulse_threadpool_completed_tasks")
                .help("Total number of tasks completed by the thread pool")
                .callback(callback -> callback.call(completedTasks.getAsLong()))
                .register(registry);
    }

    @Override
    public void close() {
        if (httpServer != null) {
            httpServer.close();
        }
    }
}
