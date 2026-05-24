package com.akita.query.execution;

import com.akita.buffer.BufferPoolManager;

public record ExecutionContext(
        BufferPoolManager bufferPoolManager
) {}
