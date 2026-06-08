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
    private final DiskSchedulerMetrics metrics;

    private FCFSDiskScheduler(ExecutorService executor, BlockManager blockManager, DiskSchedulerMetrics metrics) {
        this.executor = executor;
        this.blockManager = blockManager;
        this.metrics = metrics;
    }

    public static FCFSDiskScheduler create(ExecutorService executor, BlockManager blockManager) {
        return create(executor, blockManager, new DiskSchedulerMetrics());
    }

    public static FCFSDiskScheduler create(ExecutorService executor, BlockManager blockManager, DiskSchedulerMetrics metrics) {
        return new FCFSDiskScheduler(executor, blockManager, metrics);
    }

    public DiskSchedulerMetrics metrics() {
        return metrics;
    }

    @Override
    public Future<ByteBuffer> schedulePageRead(PageId pageId) {
        metrics.recordReadRequest();
        return executor.submit(ReadRequest.create(pageId, blockManager));
    }

    @Override
    public Future<?> schedulePageWrite(PageId pageId, ByteBuffer buffer) {
        metrics.recordWriteRequest();
        return executor.submit(WriteRequest.create(pageId, buffer, blockManager));
    }

    @Override
    public Future<?> schedulePageAllocate(PageId pageId) {
        metrics.recordAllocateRequest();
        return executor.submit(AllocateRequest.create(pageId, blockManager));
    }
}
