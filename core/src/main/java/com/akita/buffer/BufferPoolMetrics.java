package com.akita.buffer;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Raw counters for learning how the buffer pool behaves.
 */
public class BufferPoolMetrics {
    private final AtomicLong readPageRequests = new AtomicLong();
    private final AtomicLong readPageHits = new AtomicLong();
    private final AtomicLong readPageMisses = new AtomicLong();
    private final AtomicLong writePageRequests = new AtomicLong();
    private final AtomicLong writePageHits = new AtomicLong();
    private final AtomicLong writePageMisses = new AtomicLong();
    private final AtomicLong allocatePageRequests = new AtomicLong();
    private final AtomicLong frameEvictions = new AtomicLong();
    private final AtomicLong frameWaits = new AtomicLong();
    private final AtomicLong flushPageRequests = new AtomicLong();
    private final AtomicLong flushPageMisses = new AtomicLong();
    private final AtomicLong dirtyPageFlushes = new AtomicLong();
    private final AtomicLong acquireFrameTotalNanos = new AtomicLong();
    private final AtomicLong acquireFrameMaxNanos = new AtomicLong();
    private final AtomicLong prepareFrameForReuseTotalNanos = new AtomicLong();
    private final AtomicLong prepareFrameForReuseMaxNanos = new AtomicLong();
    private final AtomicLong flushSnapshotTotalNanos = new AtomicLong();
    private final AtomicLong flushSnapshotMaxNanos = new AtomicLong();

    public void recordReadPageRequest() {
        readPageRequests.incrementAndGet();
    }

    public void recordReadPageHit() {
        readPageHits.incrementAndGet();
    }

    public void recordReadPageMiss() {
        readPageMisses.incrementAndGet();
    }

    public void recordWritePageRequest() {
        writePageRequests.incrementAndGet();
    }

    public void recordWritePageHit() {
        writePageHits.incrementAndGet();
    }

    public void recordWritePageMiss() {
        writePageMisses.incrementAndGet();
    }

    public void recordAllocatePageRequest() {
        allocatePageRequests.incrementAndGet();
    }

    public void recordFrameEviction() {
        frameEvictions.incrementAndGet();
    }

    public void recordFrameWait() {
        frameWaits.incrementAndGet();
    }

    public void recordFlushPageRequest() {
        flushPageRequests.incrementAndGet();
    }

    public void recordFlushPageMiss() {
        flushPageMisses.incrementAndGet();
    }

    public void recordDirtyPageFlush() {
        dirtyPageFlushes.incrementAndGet();
    }

    public void recordAcquireFrameDuration(long nanos) {
        recordDuration(acquireFrameTotalNanos, acquireFrameMaxNanos, nanos);
    }

    public void recordPrepareFrameForReuseDuration(long nanos) {
        recordDuration(prepareFrameForReuseTotalNanos, prepareFrameForReuseMaxNanos, nanos);
    }

    public void recordFlushSnapshotDuration(long nanos) {
        recordDuration(flushSnapshotTotalNanos, flushSnapshotMaxNanos, nanos);
    }

    public Snapshot snapshot() {
        return new Snapshot(
                readPageRequests.get(),
                readPageHits.get(),
                readPageMisses.get(),
                writePageRequests.get(),
                writePageHits.get(),
                writePageMisses.get(),
                allocatePageRequests.get(),
                frameEvictions.get(),
                frameWaits.get(),
                flushPageRequests.get(),
                flushPageMisses.get(),
                dirtyPageFlushes.get(),
                acquireFrameTotalNanos.get(),
                acquireFrameMaxNanos.get(),
                prepareFrameForReuseTotalNanos.get(),
                prepareFrameForReuseMaxNanos.get(),
                flushSnapshotTotalNanos.get(),
                flushSnapshotMaxNanos.get()
        );
    }

    private static void recordDuration(AtomicLong total, AtomicLong max, long nanos) {
        total.addAndGet(nanos);
        max.updateAndGet(current -> Math.max(current, nanos));
    }

    public record Snapshot(
            long readPageRequests,
            long readPageHits,
            long readPageMisses,
            long writePageRequests,
            long writePageHits,
            long writePageMisses,
            long allocatePageRequests,
            long frameEvictions,
            long frameWaits,
            long flushPageRequests,
            long flushPageMisses,
            long dirtyPageFlushes,
            long acquireFrameTotalNanos,
            long acquireFrameMaxNanos,
            long prepareFrameForReuseTotalNanos,
            long prepareFrameForReuseMaxNanos,
            long flushSnapshotTotalNanos,
            long flushSnapshotMaxNanos
    ) {
        public String toLine() {
            return "[akita.metrics] buffer\n" +
                    "  buffer.read_page.requests=" + readPageRequests + "\n" +
                    "  buffer.read_page.hits=" + readPageHits + "\n" +
                    "  buffer.read_page.misses=" + readPageMisses + "\n" +
                    "  buffer.write_page.requests=" + writePageRequests + "\n" +
                    "  buffer.write_page.hits=" + writePageHits + "\n" +
                    "  buffer.write_page.misses=" + writePageMisses + "\n" +
                    "  buffer.allocate_page.requests=" + allocatePageRequests + "\n" +
                    "  buffer.frame.evictions=" + frameEvictions + "\n" +
                    "  buffer.frame.waits=" + frameWaits + "\n" +
                    "  buffer.flush_page.requests=" + flushPageRequests + "\n" +
                    "  buffer.flush_page.misses=" + flushPageMisses + "\n" +
                    "  buffer.flush_page.dirty_pages=" + dirtyPageFlushes + "\n" +
                    "  buffer.acquire_frame.total_nanos=" + acquireFrameTotalNanos + "\n" +
                    "  buffer.acquire_frame.max_nanos=" + acquireFrameMaxNanos + "\n" +
                    "  buffer.prepare_frame_for_reuse.total_nanos=" + prepareFrameForReuseTotalNanos + "\n" +
                    "  buffer.prepare_frame_for_reuse.max_nanos=" + prepareFrameForReuseMaxNanos + "\n" +
                    "  buffer.flush_snapshot.total_nanos=" + flushSnapshotTotalNanos + "\n" +
                    "  buffer.flush_snapshot.max_nanos=" + flushSnapshotMaxNanos;
        }
    }
}
