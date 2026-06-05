package com.eventpulse.processor;

public record ProcessingResult(ProcessingStatus status, String message) {
    public static ProcessingResult success() {
        return new ProcessingResult(ProcessingStatus.SUCCESS, "ok");
    }

    public static ProcessingResult failure(ProcessingStatus status, String message) {
        return new ProcessingResult(status, message);
    }
}
