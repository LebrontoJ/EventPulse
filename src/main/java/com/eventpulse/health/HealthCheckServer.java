package com.eventpulse.health;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.function.BooleanSupplier;

/**
 * Lightweight HTTP endpoints for liveness/readiness probes (e.g. Kubernetes), built on the JDK's
 * built-in {@code com.sun.net.httpserver.HttpServer} so no extra dependency is needed.
 *
 * <p>{@code /health/live} always returns 200 while the process is up - it answers "is this JVM
 * still responsive", not "is it doing useful work". {@code /health/ready} returns 200 only while
 * the supplied readiness check reports true (e.g. the Kafka consumer currently holds an assigned
 * partition), and 503 otherwise - that's the signal a load balancer/orchestrator should use to
 * decide whether to route traffic to this instance or restart it.
 */
public class HealthCheckServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(HealthCheckServer.class);

    private final HttpServer httpServer;

    private HealthCheckServer(HttpServer httpServer) {
        this.httpServer = httpServer;
    }

    public static HealthCheckServer start(boolean enabled, int port, BooleanSupplier readinessCheck) throws IOException {
        if (!enabled) {
            log.info("Health check HTTP server disabled (health.enabled=false)");
            return new HealthCheckServer(null);
        }
        HttpServer httpServer = HttpServer.create(new InetSocketAddress(port), 0);
        httpServer.createContext("/health/live", exchange -> respond(exchange, 200, "OK"));
        httpServer.createContext("/health/ready", exchange -> {
            boolean ready = readinessCheck.getAsBoolean();
            respond(exchange, ready ? 200 : 503, ready ? "READY" : "NOT_READY");
        });
        httpServer.setExecutor(null);
        httpServer.start();
        log.info("Health check endpoints available at http://localhost:{}/health/live and /health/ready",
                httpServer.getAddress().getPort());
        return new HealthCheckServer(httpServer);
    }

    /** The bound port, mainly useful in tests that start the server on an OS-assigned port (0). */
    public int port() {
        return httpServer == null ? -1 : httpServer.getAddress().getPort();
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream responseBody = exchange.getResponseBody()) {
            responseBody.write(bytes);
        }
    }

    @Override
    public void close() {
        if (httpServer != null) {
            httpServer.stop(0);
        }
    }
}
