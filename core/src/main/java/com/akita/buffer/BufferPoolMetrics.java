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
                dirtyPageFlushes.get()
        );
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
            long dirtyPageFlushes
    ) {
        public String toLine() {
            return "[akita.metrics] " +
                    "buffer.read_page.requests=" + readPageRequests +
                    " buffer.read_page.hits=" + readPageHits +
                    " buffer.read_page.misses=" + readPageMisses +
                    " buffer.write_page.requests=" + writePageRequests +
                    " buffer.write_page.hits=" + writePageHits +
                    " buffer.write_page.misses=" + writePageMisses +
                    " buffer.allocate_page.requests=" + allocatePageRequests +
                    " buffer.frame.evictions=" + frameEvictions +
                    " buffer.frame.waits=" + frameWaits +
                    " buffer.flush_page.requests=" + flushPageRequests +
                    " buffer.flush_page.misses=" + flushPageMisses +
                    " buffer.flush_page.dirty_pages=" + dirtyPageFlushes;
        }
    }
}
