package com.akita.buffer;

import com.akita.storage.Storage;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * FCFSDiskScheduler is a First-Come, First-Serve Disk Scheduler
 */
public class FCFSDiskScheduler implements DiskScheduler {
    private final ExecutorService executor;
    private final Storage storage;

    private FCFSDiskScheduler(ExecutorService executor, Storage storage) {
        this.executor = executor;
        this.storage = storage;
    }

    public static FCFSDiskScheduler create(ExecutorService executor, Storage storage) {
        return new FCFSDiskScheduler(executor, storage);
    }

    @Override
    public Future<ByteBuffer> schedulePageRead(PageId pageId) {
        return executor.submit(ReadRequest.create(pageId, storage));
    }

    @Override
    public Future<?> schedulePageWrite(PageId pageId, ByteBuffer buffer) {
        return executor.submit(WriteRequest.create(pageId, buffer, storage));
    }

    @Override
    public Future<?> schedulePageAllocate(PageId pageId) {
        return executor.submit(AllocateRequest.create(pageId, storage));
    }
}
