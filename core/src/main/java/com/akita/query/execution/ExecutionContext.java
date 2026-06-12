package com.akita.query.execution;

import com.akita.query.storage.TableAccess;
import com.akita.buffer.BufferPoolManager;
import com.akita.query.storage.HeapTableAccess;

public record ExecutionContext(
        TableAccess tableAccess
) {
    public ExecutionContext(BufferPoolManager bufferPoolManager) {
        this(new HeapTableAccess(bufferPoolManager));
    }
}
