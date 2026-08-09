package com.eventpulse.health;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HealthCheckServerTest {
    // Port 0 asks the OS for any free port, so tests never collide with each other or anything
    // else already listening on the host; the actual bound port is read back via server.port().
    private static final int ANY_FREE_PORT = 0;

    private final HttpClient client = HttpClient.newHttpClient();

    @Test
    void liveEndpointAlwaysReturns200RegardlessOfReadiness() throws Exception {
        try (HealthCheckServer server = HealthCheckServer.start(true, ANY_FREE_PORT, () -> false)) {
            HttpResponse<String> response = get(server, "/health/live");

            assertEquals(200, response.statusCode());
            assertEquals("OK", response.body());
        }
    }

    @Test
    void readyEndpointReflectsTheSuppliedReadinessCheckLive() throws Exception {
        AtomicBoolean ready = new AtomicBoolean(false);

        try (HealthCheckServer server = HealthCheckServer.start(true, ANY_FREE_PORT, ready::get)) {
            HttpResponse<String> notReady = get(server, "/health/ready");
            assertEquals(503, notReady.statusCode());
            assertEquals("NOT_READY", notReady.body());

            ready.set(true);

            HttpResponse<String> nowReady = get(server, "/health/ready");
            assertEquals(200, nowReady.statusCode());
            assertEquals("READY", nowReady.body());
        }
    }

    @Test
    void disabledServerNeverBindsAPort() throws Exception {
        HealthCheckServer server = HealthCheckServer.start(false, ANY_FREE_PORT, () -> true);

        assertEquals(-1, server.port());

        server.close();
        server.close();
    }

    private HttpResponse<String> get(HealthCheckServer server, String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + path))
                .GET()
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
