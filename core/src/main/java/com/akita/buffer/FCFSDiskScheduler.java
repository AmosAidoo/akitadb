package com.akita.buffer;

import com.akita.storage.BlockManager;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * FCFSDiskScheduler is a First-Come, First-Serve Disk Scheduler
 */
public class FCFSDiskScheduler implements DiskScheduler {
    private final ExecutorService executor;
    private final BlockManager blockManager;

    private FCFSDiskScheduler(ExecutorService executor, BlockManager blockManager) {
        this.executor = executor;
        this.blockManager = blockManager;
    }

    public static FCFSDiskScheduler create(ExecutorService executor, BlockManager blockManager) {
        return new FCFSDiskScheduler(executor, blockManager);
    }

    @Override
    public Future<ByteBuffer> schedulePageRead(PageId pageId) {
        return executor.submit(ReadRequest.create(pageId, blockManager));
    }

    @Override
    public Future<?> schedulePageWrite(PageId pageId, ByteBuffer buffer) {
        return executor.submit(WriteRequest.create(pageId, buffer, blockManager));
    }

    @Override
    public Future<?> schedulePageAllocate(PageId pageId) {
        return executor.submit(AllocateRequest.create(pageId, blockManager));
    }
}
