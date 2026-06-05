package com.eventpulse.generator;

import java.util.List;
import java.util.Random;

public class SimulatedRequestGenerator {
    private static final List<String> USERS = List.of("alice", "bob", "taylor", "morgan", "jordan");
    private static final List<String> KEYWORDS = List.of("kafka", "observability", "threadpool", "validation", "backend");
    private static final List<String> CONTENT = List.of(
            "hello",
            "can you review this event",
            "request pipeline looks healthy",
            "retry later",
            "message content for validation"
    );
    private static final List<String> INVALID_REQUESTS = List.of(
            "LOGIN|user=",
            "MESSAGE|from=bob|to=taylor",
            "UPLOAD|filename=test.pdf|size=-1",
            "PAYMENT|amount=-100",
            "UNKNOWN|abc",
            "INVALID_FORMAT_STRING"
    );

    private final Random random;
    private final int invalidRequestRatePercent;

    public SimulatedRequestGenerator(int invalidRequestRatePercent) {
        this(invalidRequestRatePercent, new Random());
    }

    SimulatedRequestGenerator(int invalidRequestRatePercent, Random random) {
        if (invalidRequestRatePercent < 0 || invalidRequestRatePercent > 100) {
            throw new IllegalArgumentException("invalidRequestRatePercent must be between 0 and 100");
        }
        this.invalidRequestRatePercent = invalidRequestRatePercent;
        this.random = random;
    }

    public String nextRequest() {
        if (random.nextInt(100) < invalidRequestRatePercent) {
            return pick(INVALID_REQUESTS);
        }

        return switch (random.nextInt(5)) {
            case 0 -> "SIGNUP|user=" + user() + "|ip=" + ip();
            case 1 -> "LOGIN|user=" + user() + "|ip=" + ip();
            case 2 -> "MESSAGE|from=" + user() + "|to=" + user() + "|content=" + content();
            case 3 -> "QUERY|keyword=" + pick(KEYWORDS);
            default -> "UPLOAD|filename=" + fileName() + "|size=" + uploadSize();
        };
    }

    private String user() {
        return pick(USERS);
    }

    private String ip() {
        return "192.168.1." + (random.nextInt(254) + 1);
    }

    private String content() {
        return pick(CONTENT);
    }

    private String fileName() {
        return "file-" + (random.nextInt(1000) + 1) + ".pdf";
    }

    private int uploadSize() {
        return random.nextInt(10_000_000) + 1;
    }

    private String pick(List<String> values) {
        return values.get(random.nextInt(values.size()));
    }
}
