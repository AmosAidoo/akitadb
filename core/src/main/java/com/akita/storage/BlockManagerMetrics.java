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
    private final AtomicLong readBlockTotalNanos = new AtomicLong();
    private final AtomicLong readBlockMaxNanos = new AtomicLong();
    private final AtomicLong writeBlockTotalNanos = new AtomicLong();
    private final AtomicLong writeBlockMaxNanos = new AtomicLong();
    private final AtomicLong allocateBlockTotalNanos = new AtomicLong();
    private final AtomicLong allocateBlockMaxNanos = new AtomicLong();

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

    public void recordReadBlockDuration(long nanos) {
        recordDuration(readBlockTotalNanos, readBlockMaxNanos, nanos);
    }

    public void recordWriteBlockDuration(long nanos) {
        recordDuration(writeBlockTotalNanos, writeBlockMaxNanos, nanos);
    }

    public void recordAllocateBlockDuration(long nanos) {
        recordDuration(allocateBlockTotalNanos, allocateBlockMaxNanos, nanos);
    }

    public Snapshot snapshot() {
        return new Snapshot(
                readBlockRequests.get(),
                writeBlockRequests.get(),
                allocateBlockRequests.get(),
                blocksZeroFilled.get(),
                readBlockTotalNanos.get(),
                readBlockMaxNanos.get(),
                writeBlockTotalNanos.get(),
                writeBlockMaxNanos.get(),
                allocateBlockTotalNanos.get(),
                allocateBlockMaxNanos.get()
        );
    }

    private static void recordDuration(AtomicLong total, AtomicLong max, long nanos) {
        total.addAndGet(nanos);
        max.updateAndGet(current -> Math.max(current, nanos));
    }

    public record Snapshot(
            long readBlockRequests,
            long writeBlockRequests,
            long allocateBlockRequests,
            long blocksZeroFilled,
            long readBlockTotalNanos,
            long readBlockMaxNanos,
            long writeBlockTotalNanos,
            long writeBlockMaxNanos,
            long allocateBlockTotalNanos,
            long allocateBlockMaxNanos
    ) {
        public String toLine() {
            return "[akita.metrics] block\n" +
                    "  block.read.requests=" + readBlockRequests + "\n" +
                    "  block.write.requests=" + writeBlockRequests + "\n" +
                    "  block.allocate.requests=" + allocateBlockRequests + "\n" +
                    "  block.allocate.zero_filled=" + blocksZeroFilled + "\n" +
                    "  block.read.total_nanos=" + readBlockTotalNanos + "\n" +
                    "  block.read.max_nanos=" + readBlockMaxNanos + "\n" +
                    "  block.write.total_nanos=" + writeBlockTotalNanos + "\n" +
                    "  block.write.max_nanos=" + writeBlockMaxNanos + "\n" +
                    "  block.allocate.total_nanos=" + allocateBlockTotalNanos + "\n" +
                    "  block.allocate.max_nanos=" + allocateBlockMaxNanos;
        }
    }
}
