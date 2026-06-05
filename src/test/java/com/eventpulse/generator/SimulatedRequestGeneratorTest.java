package com.eventpulse.generator;

import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SimulatedRequestGeneratorTest {
    @Test
    void generatesMessageRequestsWithContent() {
        SimulatedRequestGenerator generator = new SimulatedRequestGenerator(0);

        boolean foundMessageWithContent = IntStream.range(0, 100)
                .mapToObj(index -> generator.nextRequest())
                .filter(request -> request.startsWith("MESSAGE|"))
                .anyMatch(request -> request.contains("|content="));

        assertTrue(foundMessageWithContent);
    }

    @Test
    void canGenerateOnlyInvalidRequests() {
        SimulatedRequestGenerator generator = new SimulatedRequestGenerator(100);

        String request = generator.nextRequest();

        assertFalse(request.equals("MESSAGE|from=bob|to=taylor|content=hello"));
    }
}
