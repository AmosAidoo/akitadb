package com.akita.buffer;

import com.akita.buffer.guards.ReadPageGuard;
import com.akita.buffer.guards.WritePageGuard;
import com.akita.buffer.replacers.arc.ArcReplacer;
import com.akita.buffer.replacers.arc.ArcReplacerMetrics;
import com.akita.storage.*;
import com.akita.testing.AkitaExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

@ExtendWith(AkitaExtension.class)
class BufferPoolManagerTest {

    @Test
    void veryBasicTest(
            BufferPoolManager bpm,
            FileChannelContainerManager cm
    ) throws Exception {

        ContainerId containerId = cm.createContainer();
        PageId pageId = new PageId(containerId, 0);
        final String expected = "Hello world";

        // Check WritePageGuard for basic functionality
        try (WritePageGuard writePageGuard = bpm.writePage(pageId)) {
            writePageGuard.getData().put(expected.getBytes(StandardCharsets.UTF_8));

            byte[] dst = new byte[expected.length()];
            writePageGuard.getData().get(dst);
            assertThat(new String(dst, StandardCharsets.UTF_8)).isEqualTo(expected);
        }

        // Check ReadPageGuard for basic functionality
        try (ReadPageGuard readPageGuard = bpm.readPage(pageId)) {
            byte[] dst = new byte[expected.length()];
            readPageGuard.getData().get(dst);
            assertThat(new String(dst, StandardCharsets.UTF_8)).isEqualTo(expected);
        }

        // Check ReadPageGuard for basic functionality again
        try (ReadPageGuard readPageGuard = bpm.readPage(pageId)) {
            byte[] dst = new byte[expected.length()];
            readPageGuard.getData().get(dst);
            assertThat(new String(dst, StandardCharsets.UTF_8)).isEqualTo(expected);
        }

        assertThat(bpm.deletePage(pageId)).isTrue();
    }

    @Test
    void allocatePageReturnsZeroedWritablePage(
            BufferPoolManager bpm,
            FileChannelContainerManager cm
    ) throws Exception {
        ContainerId containerId = cm.createContainer();
        PageId pageId = new PageId(containerId, 3);

        try (WritePageGuard guard = bpm.allocatePage(pageId)) {
            assertThat(guard.getPageId()).isEqualTo(pageId);
            assertThat(guard.getData().getInt()).isZero();
            guard.getData().putInt(1234);
        }

        try (ReadPageGuard guard = bpm.readPage(pageId)) {
            assertThat(guard.getData().getInt()).isEqualTo(1234);
        }
    }

    @Test
    void recordsBasicBufferPoolMetrics(
            BufferPoolManager bpm,
            FileChannelContainerManager cm
    ) throws Exception {
        ContainerId containerId = cm.createContainer();
        PageId pageId = new PageId(containerId, 0);

        try (WritePageGuard guard = bpm.allocatePage(pageId)) {
            guard.getData().putInt(1234);
        }

        try (ReadPageGuard ignored = bpm.readPage(pageId)) {
            // First read after allocation is a buffer hit: the page is already resident.
        }

        try (WritePageGuard ignored = bpm.writePage(pageId)) {
            // Same idea for writePage: no disk read is needed when the page is resident.
        }

        bpm.flushPage(pageId);

        BufferPoolMetrics.Snapshot metrics = bpm.metrics().snapshot();
        assertThat(metrics.allocatePageRequests()).isEqualTo(1);
        assertThat(metrics.readPageRequests()).isEqualTo(1);
        assertThat(metrics.readPageHits()).isEqualTo(1);
        assertThat(metrics.readPageMisses()).isZero();
        assertThat(metrics.writePageRequests()).isEqualTo(1);
        assertThat(metrics.writePageHits()).isEqualTo(1);
        assertThat(metrics.writePageMisses()).isZero();
        assertThat(metrics.flushPageRequests()).isEqualTo(1);
        assertThat(metrics.dirtyPageFlushes()).isEqualTo(1);
    }

    @Test
    void evictingDirtyPageFlushesItBeforeReusingFrame(
            FileChannelBlockManager bm,
            FileChannelContainerManager cm
    ) throws Exception {
        InstrumentedBufferPool instrumented = bufferPoolManagerWithFrames(bm, 1);
        BufferPoolManager bpm = instrumented.bufferPoolManager();
        ContainerId containerId = cm.createContainer();
        PageId firstPage = new PageId(containerId, 0);
        PageId secondPage = new PageId(containerId, 1);

        try (WritePageGuard guard = bpm.allocatePage(firstPage)) {
            guard.getData().putInt(1234);
        }

        try (WritePageGuard ignored = bpm.allocatePage(secondPage)) {
            // Allocating another page with one frame forces the first dirty page out.
        }

        BufferPoolMetrics.Snapshot metrics = bpm.metrics().snapshot();
        assertThat(metrics.allocatePageRequests()).isEqualTo(2);
        assertThat(metrics.frameEvictions()).isEqualTo(1);
        assertThat(metrics.dirtyPageFlushes()).isEqualTo(1);

        DiskSchedulerMetrics.Snapshot diskMetrics = instrumented.diskMetrics().snapshot();
        assertThat(diskMetrics.allocateRequests()).isEqualTo(2);
        assertThat(diskMetrics.writeRequests()).isEqualTo(1);
        assertThat(diskMetrics.readRequests()).isZero();

        ByteBuffer persisted = ByteBuffer.allocate(BlockManager.BLOCK_SIZE);
        bm.readBlock(containerId, 0, persisted);
        persisted.clear();

        assertThat(persisted.getInt()).isEqualTo(1234);
    }

    @Test
    void flushAllPagesWritesDirtyPages(
            BufferPoolManager bpm,
            FileChannelBlockManager bm,
            FileChannelContainerManager cm
    ) throws Exception {
        ContainerId containerId = cm.createContainer();
        PageId pageId = new PageId(containerId, 0);

        try (WritePageGuard guard = bpm.allocatePage(pageId)) {
            guard.getData().putInt(5678);
        }

        bpm.flushAllPages();

        ByteBuffer persisted = ByteBuffer.allocate(BlockManager.BLOCK_SIZE);
        bm.readBlock(containerId, 0, persisted);
        persisted.clear();

        assertThat(persisted.getInt()).isEqualTo(5678);
    }

    @Test
    void recordsDiskReadWhenBufferPoolMisses(
            FileChannelBlockManager bm,
            FileChannelContainerManager cm
    ) throws Exception {
        InstrumentedBufferPool instrumented = bufferPoolManagerWithFrames(bm, 1);
        BufferPoolManager bpm = instrumented.bufferPoolManager();
        ContainerId containerId = cm.createContainer();
        PageId pageId = new PageId(containerId, 0);

        try (WritePageGuard guard = bpm.allocatePage(pageId)) {
            guard.getData().putInt(1234);
        }

        bpm.flushPage(pageId);
        assertThat(bpm.deletePage(pageId)).isTrue();

        try (ReadPageGuard guard = bpm.readPage(pageId)) {
            assertThat(guard.getData().getInt()).isEqualTo(1234);
        }

        BufferPoolMetrics.Snapshot bufferMetrics = bpm.metrics().snapshot();
        DiskSchedulerMetrics.Snapshot diskMetrics = instrumented.diskMetrics().snapshot();

        assertThat(bufferMetrics.readPageRequests()).isEqualTo(1);
        assertThat(bufferMetrics.readPageHits()).isZero();
        assertThat(bufferMetrics.readPageMisses()).isEqualTo(1);
        assertThat(diskMetrics.readRequests()).isEqualTo(1);
        assertThat(diskMetrics.writeRequests()).isEqualTo(1);
        assertThat(diskMetrics.allocateRequests()).isEqualTo(1);
    }

    @Test
    void sequentialWorkingSetLargerThanBufferPoolKeepsArcInvariants(
            FileChannelBlockManager bm,
            FileChannelContainerManager cm
    ) throws Exception {
        InstrumentedBufferPool instrumented = bufferPoolManagerWithFrames(bm, 8);
        BufferPoolManager bpm = instrumented.bufferPoolManager();
        ArcReplacer arc = instrumented.arcReplacer();
        ContainerId containerId = cm.createContainer();
        PageId[] pageIds = new PageId[16];

        for (int i = 0; i < pageIds.length; i++) {
            PageId pageId = new PageId(containerId, i);
            pageIds[i] = pageId;
            ByteBuffer page = ByteBuffer.allocate(BlockManager.BLOCK_SIZE);
            page.putInt(i);
            bm.writeBlock(containerId, i, page);
        }

        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
            for (int round = 0; round < 4; round++) {
                for (int i = 0; i < pageIds.length; i++) {
                    try (ReadPageGuard guard = bpm.readPage(pageIds[i])) {
                        assertThat(guard.getData().getInt()).isEqualTo(i);
                    }
                    assertArcInvariants(arc.metrics(), 8);
                }
            }
        });
    }

    private static void assertArcInvariants(ArcReplacerMetrics.Snapshot metrics, int capacity) {
        assertThat(metrics.t1Size() + metrics.t2Size()).isLessThanOrEqualTo(capacity);
        assertThat(metrics.t1Size() + metrics.b1Size()).isLessThanOrEqualTo(capacity);
        assertThat(metrics.t1Size() + metrics.t2Size() + metrics.b1Size() + metrics.b2Size()).isLessThanOrEqualTo(2 * capacity);
        assertThat(metrics.evictableSize()).isBetween(0, capacity);
    }

    private static InstrumentedBufferPool bufferPoolManagerWithFrames(FileChannelBlockManager bm, int frameCount) {
        Map<FrameId, Frame> frames = new HashMap<>();
        for (int i = 0; i < frameCount; i++) {
            FrameId id = new FrameId(i);
            frames.put(id, Frame.create(id));
        }

        DiskSchedulerMetrics diskMetrics = new DiskSchedulerMetrics();
        ArcReplacer arc = ArcReplacer.create(frameCount);
        BufferPoolManager bpm = BufferPoolManager.create(
                FCFSDiskScheduler.create(Executors.newSingleThreadExecutor(), bm, diskMetrics),
                arc,
                frames,
                new HashMap<>()
        );
        return new InstrumentedBufferPool(bpm, diskMetrics, arc);
    }

    private record InstrumentedBufferPool(
            BufferPoolManager bufferPoolManager,
            DiskSchedulerMetrics diskMetrics,
            ArcReplacer arcReplacer
    ) {}
}
