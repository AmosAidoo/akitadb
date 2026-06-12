package com.akita;

public record EngineOptions(int bufferFrameCount) {
    private static final int DEFAULT_BUFFER_FRAMES = 64;

    public EngineOptions {
        if (bufferFrameCount <= 0) {
            throw new IllegalArgumentException("bufferFrameCount must be positive");
        }
    }

    public static EngineOptions defaults() {
        return new EngineOptions(DEFAULT_BUFFER_FRAMES);
    }
}
