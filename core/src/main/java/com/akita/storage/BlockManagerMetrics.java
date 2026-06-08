package com.akita.storage;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Raw counters for physical block-manager operations.
 */
public class BlockManagerMetrics {
    private final AtomicLong readBlockRequests = new AtomicLong();
    private final AtomicLong writeBlockRequests = new AtomicLong();
    private final AtomicLong allocateBlockRequests = new AtomicLong();
    private final AtomicLong blocksZeroFilled = new AtomicLong();

    public void recordReadBlockRequest() {
        readBlockRequests.incrementAndGet();
    }

    public void recordWriteBlockRequest() {
        writeBlockRequests.incrementAndGet();
    }

    public void recordAllocateBlockRequest() {
        allocateBlockRequests.incrementAndGet();
    }

    public void recordBlockZeroFilled() {
        blocksZeroFilled.incrementAndGet();
    }

    public Snapshot snapshot() {
        return new Snapshot(
                readBlockRequests.get(),
                writeBlockRequests.get(),
                allocateBlockRequests.get(),
                blocksZeroFilled.get()
        );
    }

    public record Snapshot(
            long readBlockRequests,
            long writeBlockRequests,
            long allocateBlockRequests,
            long blocksZeroFilled
    ) {
        public String toLine() {
            return "[akita.metrics] " +
                    "block.read.requests=" + readBlockRequests +
                    " block.write.requests=" + writeBlockRequests +
                    " block.allocate.requests=" + allocateBlockRequests +
                    " block.allocate.zero_filled=" + blocksZeroFilled;
        }
    }
}
