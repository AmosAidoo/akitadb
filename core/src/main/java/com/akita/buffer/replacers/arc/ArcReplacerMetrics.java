package com.akita.buffer.replacers.arc;

import java.util.concurrent.atomic.AtomicLong;

public class ArcReplacerMetrics {
    private final AtomicLong recordAccessRequests = new AtomicLong();
    private final AtomicLong hitsT1 = new AtomicLong();
    private final AtomicLong hitsT2 = new AtomicLong();
    private final AtomicLong ghostHitsB1 = new AtomicLong();
    private final AtomicLong ghostHitsB2 = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong evictRequests = new AtomicLong();
    private final AtomicLong evictionsFromT1 = new AtomicLong();
    private final AtomicLong evictionsFromT2 = new AtomicLong();
    private final AtomicLong evictMisses = new AtomicLong();
    private final AtomicLong metadataDropsFromB1 = new AtomicLong();
    private final AtomicLong metadataDropsFromB2 = new AtomicLong();
    private final AtomicLong metadataDropsFromT1 = new AtomicLong();

    void recordAccessRequest() {
        recordAccessRequests.incrementAndGet();
    }

    void recordHit(ArcLocation location) {
        switch (location) {
            case T1 -> hitsT1.incrementAndGet();
            case T2 -> hitsT2.incrementAndGet();
            case B1 -> ghostHitsB1.incrementAndGet();
            case B2 -> ghostHitsB2.incrementAndGet();
        }
    }

    void recordMiss() {
        misses.incrementAndGet();
    }

    void recordEvictRequest() {
        evictRequests.incrementAndGet();
    }

    void recordEviction(ArcLocation location) {
        switch (location) {
            case T1 -> evictionsFromT1.incrementAndGet();
            case T2 -> evictionsFromT2.incrementAndGet();
            case B1, B2 -> throw new IllegalArgumentException("Cannot evict resident frame from ghost list: " + location);
        }
    }

    void recordEvictMiss() {
        evictMisses.incrementAndGet();
    }

    void recordMetadataDrop(ArcLocation location) {
        switch (location) {
            case B1 -> metadataDropsFromB1.incrementAndGet();
            case B2 -> metadataDropsFromB2.incrementAndGet();
            case T1 -> metadataDropsFromT1.incrementAndGet();
            case T2 -> throw new IllegalArgumentException("Unexpected metadata drop from T2");
        }
    }

    public Snapshot snapshot(int t1Size, int t2Size, int b1Size, int b2Size, int t1Target, int evictableSize) {
        return new Snapshot(
                recordAccessRequests.get(),
                hitsT1.get(),
                hitsT2.get(),
                ghostHitsB1.get(),
                ghostHitsB2.get(),
                misses.get(),
                evictRequests.get(),
                evictionsFromT1.get(),
                evictionsFromT2.get(),
                evictMisses.get(),
                metadataDropsFromB1.get(),
                metadataDropsFromB2.get(),
                metadataDropsFromT1.get(),
                t1Size,
                t2Size,
                b1Size,
                b2Size,
                t1Target,
                evictableSize
        );
    }

    public record Snapshot(
            long recordAccessRequests,
            long hitsT1,
            long hitsT2,
            long ghostHitsB1,
            long ghostHitsB2,
            long misses,
            long evictRequests,
            long evictionsFromT1,
            long evictionsFromT2,
            long evictMisses,
            long metadataDropsFromB1,
            long metadataDropsFromB2,
            long metadataDropsFromT1,
            int t1Size,
            int t2Size,
            int b1Size,
            int b2Size,
            int t1Target,
            int evictableSize
    ) {
        public String toLine() {
            return "[akita.metrics] " +
                    "arc.record_access.requests=" + recordAccessRequests +
                    " arc.hits.t1=" + hitsT1 +
                    " arc.hits.t2=" + hitsT2 +
                    " arc.ghost_hits.b1=" + ghostHitsB1 +
                    " arc.ghost_hits.b2=" + ghostHitsB2 +
                    " arc.misses=" + misses +
                    " arc.evict.requests=" + evictRequests +
                    " arc.evictions.t1=" + evictionsFromT1 +
                    " arc.evictions.t2=" + evictionsFromT2 +
                    " arc.evict.misses=" + evictMisses +
                    " arc.metadata_drops.b1=" + metadataDropsFromB1 +
                    " arc.metadata_drops.b2=" + metadataDropsFromB2 +
                    " arc.metadata_drops.t1=" + metadataDropsFromT1 +
                    " arc.size.t1=" + t1Size +
                    " arc.size.t2=" + t2Size +
                    " arc.size.b1=" + b1Size +
                    " arc.size.b2=" + b2Size +
                    " arc.t1_target=" + t1Target +
                    " arc.evictable_size=" + evictableSize;
        }
    }
}
