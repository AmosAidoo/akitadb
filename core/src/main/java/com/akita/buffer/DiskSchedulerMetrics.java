package com.akita.buffer;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Raw counters for disk work scheduled by the buffer pool.
 */
public class DiskSchedulerMetrics {
    private final AtomicLong readRequests = new AtomicLong();
    private final AtomicLong writeRequests = new AtomicLong();
    private final AtomicLong allocateRequests = new AtomicLong();
    private final AtomicLong submittedRequests = new AtomicLong();
    private final AtomicLong completedRequests = new AtomicLong();
    private final AtomicLong queueDepth = new AtomicLong();
    private final AtomicLong maxQueueDepth = new AtomicLong();
    private final AtomicLong inFlightRequests = new AtomicLong();
    private final AtomicLong maxInFlightRequests = new AtomicLong();
    private final AtomicLong totalWaitNanos = new AtomicLong();
    private final AtomicLong maxWaitNanos = new AtomicLong();
    private final AtomicLong totalServiceNanos = new AtomicLong();
    private final AtomicLong maxServiceNanos = new AtomicLong();

    public void recordReadRequest() {
        readRequests.incrementAndGet();
    }

    public void recordWriteRequest() {
        writeRequests.incrementAndGet();
    }

    public void recordAllocateRequest() {
        allocateRequests.incrementAndGet();
    }

    public long recordSubmitted() {
        submittedRequests.incrementAndGet();
        long depth = queueDepth.incrementAndGet();
        recordMax(maxQueueDepth, depth);
        return System.nanoTime();
    }

    public long recordStarted(long submittedAtNanos) {
        decrement(queueDepth);
        long inFlight = inFlightRequests.incrementAndGet();
        recordMax(maxInFlightRequests, inFlight);
        long waitNanos = System.nanoTime() - submittedAtNanos;
        totalWaitNanos.addAndGet(waitNanos);
        recordMax(maxWaitNanos, waitNanos);
        return System.nanoTime();
    }

    public void recordCompleted(long startedAtNanos) {
        completedRequests.incrementAndGet();
        decrement(inFlightRequests);
        long serviceNanos = System.nanoTime() - startedAtNanos;
        totalServiceNanos.addAndGet(serviceNanos);
        recordMax(maxServiceNanos, serviceNanos);
    }

    public Snapshot snapshot() {
        return new Snapshot(
                readRequests.get(),
                writeRequests.get(),
                allocateRequests.get(),
                submittedRequests.get(),
                completedRequests.get(),
                queueDepth.get(),
                maxQueueDepth.get(),
                inFlightRequests.get(),
                maxInFlightRequests.get(),
                totalWaitNanos.get(),
                maxWaitNanos.get(),
                totalServiceNanos.get(),
                maxServiceNanos.get()
        );
    }

    private static void decrement(AtomicLong value) {
        value.updateAndGet(current -> Math.max(0, current - 1));
    }

    private static void recordMax(AtomicLong target, long candidate) {
        target.updateAndGet(current -> Math.max(current, candidate));
    }

    public record Snapshot(
            long readRequests,
            long writeRequests,
            long allocateRequests,
            long submittedRequests,
            long completedRequests,
            long queueDepth,
            long maxQueueDepth,
            long inFlightRequests,
            long maxInFlightRequests,
            long totalWaitNanos,
            long maxWaitNanos,
            long totalServiceNanos,
            long maxServiceNanos
    ) {
        public String toLine() {
            return "[akita.metrics] disk\n" +
                    "  disk.read.requests=" + readRequests + "\n" +
                    "  disk.write.requests=" + writeRequests + "\n" +
                    "  disk.allocate.requests=" + allocateRequests + "\n" +
                    "  disk.queue.submitted=" + submittedRequests + "\n" +
                    "  disk.queue.completed=" + completedRequests + "\n" +
                    "  disk.queue.depth=" + queueDepth + "\n" +
                    "  disk.queue.max_depth=" + maxQueueDepth + "\n" +
                    "  disk.queue.in_flight=" + inFlightRequests + "\n" +
                    "  disk.queue.max_in_flight=" + maxInFlightRequests + "\n" +
                    "  disk.request.wait.total_nanos=" + totalWaitNanos + "\n" +
                    "  disk.request.wait.max_nanos=" + maxWaitNanos + "\n" +
                    "  disk.request.service.total_nanos=" + totalServiceNanos + "\n" +
                    "  disk.request.service.max_nanos=" + maxServiceNanos;
        }
    }
}
