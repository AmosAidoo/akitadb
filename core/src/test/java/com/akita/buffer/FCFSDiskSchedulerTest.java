package com.akita.buffer;

import com.akita.storage.BlockManager;
import com.akita.storage.ContainerId;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

class FCFSDiskSchedulerTest {

    @Test
    void recordsQueueDepthAndInFlightRequests() throws Exception {
        BlockingReadBlockManager blockManager = new BlockingReadBlockManager();
        DiskSchedulerMetrics metrics = new DiskSchedulerMetrics();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        FCFSDiskScheduler scheduler = FCFSDiskScheduler.create(executor, blockManager, metrics);
        ContainerId containerId = containerId(1);

        Future<ByteBuffer> first = scheduler.schedulePageRead(new PageId(containerId, 0));
        assertThat(blockManager.firstReadStarted.await(1, TimeUnit.SECONDS)).isTrue();

        Future<ByteBuffer> second = scheduler.schedulePageRead(new PageId(containerId, 1));

        DiskSchedulerMetrics.Snapshot queued = metrics.snapshot();
        assertThat(queued.readRequests()).isEqualTo(2);
        assertThat(queued.submittedRequests()).isEqualTo(2);
        assertThat(queued.completedRequests()).isZero();
        assertThat(queued.queueDepth()).isEqualTo(1);
        assertThat(queued.maxQueueDepth()).isEqualTo(1);
        assertThat(queued.inFlightRequests()).isEqualTo(1);
        assertThat(queued.maxInFlightRequests()).isEqualTo(1);
        assertThat(queued.totalWaitNanos()).isGreaterThanOrEqualTo(0);

        blockManager.allowReads.countDown();
        first.get(1, TimeUnit.SECONDS);
        second.get(1, TimeUnit.SECONDS);
        executor.shutdown();
        assertThat(executor.awaitTermination(1, TimeUnit.SECONDS)).isTrue();

        DiskSchedulerMetrics.Snapshot completed = metrics.snapshot();
        assertThat(completed.completedRequests()).isEqualTo(2);
        assertThat(completed.queueDepth()).isZero();
        assertThat(completed.inFlightRequests()).isZero();
        assertThat(completed.totalServiceNanos()).isGreaterThan(0);
        assertThat(completed.maxServiceNanos()).isGreaterThan(0);
    }

    @Test
    void recordsCompletedRequestsWhenTaskFails() {
        FailingBlockManager blockManager = new FailingBlockManager();
        DiskSchedulerMetrics metrics = new DiskSchedulerMetrics();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        FCFSDiskScheduler scheduler = FCFSDiskScheduler.create(executor, blockManager, metrics);
        ContainerId containerId = containerId(1);

        assertTimeoutPreemptively(Duration.ofSeconds(1), () -> {
            Future<ByteBuffer> read = scheduler.schedulePageRead(new PageId(containerId, 0));
            try {
                read.get();
            } catch (Exception ignored) {
                // The metric must complete even when the underlying request fails.
            }
        });

        executor.shutdown();

        DiskSchedulerMetrics.Snapshot completed = metrics.snapshot();
        assertThat(completed.submittedRequests()).isEqualTo(1);
        assertThat(completed.completedRequests()).isEqualTo(1);
        assertThat(completed.queueDepth()).isZero();
        assertThat(completed.inFlightRequests()).isZero();
    }

    private static ContainerId containerId(long value) {
        return ContainerId.fromUUID(new UUID(0, value));
    }

    private static class BlockingReadBlockManager implements BlockManager {
        private final CountDownLatch firstReadStarted = new CountDownLatch(1);
        private final CountDownLatch allowReads = new CountDownLatch(1);

        @Override
        public void allocateBlock(ContainerId containerId, long blockNumber) {}

        @Override
        public void writeBlock(ContainerId containerId, long blockNumber, ByteBuffer buffer) {}

        @Override
        public void readBlock(ContainerId containerId, long blockNumber, ByteBuffer buffer) throws IOException {
            firstReadStarted.countDown();
            try {
                allowReads.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting to read", e);
            }
        }
    }

    private static class FailingBlockManager implements BlockManager {
        @Override
        public void allocateBlock(ContainerId containerId, long blockNumber) {}

        @Override
        public void writeBlock(ContainerId containerId, long blockNumber, ByteBuffer buffer) {}

        @Override
        public void readBlock(ContainerId containerId, long blockNumber, ByteBuffer buffer) throws IOException {
            throw new IOException("read failed");
        }
    }
}
