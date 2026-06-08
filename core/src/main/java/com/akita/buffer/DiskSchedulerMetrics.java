package com.akita.buffer;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Raw counters for disk work scheduled by the buffer pool.
 */
public class DiskSchedulerMetrics {
    private final AtomicLong readRequests = new AtomicLong();
    private final AtomicLong writeRequests = new AtomicLong();
    private final AtomicLong allocateRequests = new AtomicLong();

    public void recordReadRequest() {
        readRequests.incrementAndGet();
    }

    public void recordWriteRequest() {
        writeRequests.incrementAndGet();
    }

    public void recordAllocateRequest() {
        allocateRequests.incrementAndGet();
    }

    public Snapshot snapshot() {
        return new Snapshot(
                readRequests.get(),
                writeRequests.get(),
                allocateRequests.get()
        );
    }

    public record Snapshot(
            long readRequests,
            long writeRequests,
            long allocateRequests
    ) {
        public String toLine() {
            return "[akita.metrics] " +
                    "disk.read.requests=" + readRequests +
                    " disk.write.requests=" + writeRequests +
                    " disk.allocate.requests=" + allocateRequests;
        }
    }
}
