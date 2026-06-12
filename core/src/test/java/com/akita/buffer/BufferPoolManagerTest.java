package com.akita.buffer;

import com.akita.buffer.guards.ReadPageGuard;
import com.akita.buffer.guards.WritePageGuard;
import com.akita.buffer.replacers.arc.ArcReplacer;
import com.akita.storage.ContainerId;
import com.akita.storage.Storage;
import com.akita.testing.AkitaExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(AkitaExtension.class)
class BufferPoolManagerTest {

    @Test
    void veryBasicTest(
            BufferPoolManager bpm,
            Storage storage
    ) throws Exception {

        ContainerId containerId = storage.createContainer();
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
            Storage storage
    ) throws Exception {
        ContainerId containerId = storage.createContainer();
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
    void evictingDirtyPageFlushesItBeforeReusingFrame(
            Storage storage
    ) throws Exception {
        BufferPoolManager bpm = bufferPoolManagerWithFrames(storage, 1);
        ContainerId containerId = storage.createContainer();
        PageId firstPage = new PageId(containerId, 0);
        PageId secondPage = new PageId(containerId, 1);

        try (WritePageGuard guard = bpm.allocatePage(firstPage)) {
            guard.getData().putInt(1234);
        }

        try (WritePageGuard ignored = bpm.allocatePage(secondPage)) {
            // Allocating another page with one frame forces the first dirty page out.
        }

        ByteBuffer persisted = storage.read(firstPage);

        assertThat(persisted.getInt()).isEqualTo(1234);
    }

    @Test
    void flushAllPagesWritesDirtyPages(
            BufferPoolManager bpm,
            Storage storage
    ) throws Exception {
        ContainerId containerId = storage.createContainer();
        PageId pageId = new PageId(containerId, 0);

        try (WritePageGuard guard = bpm.allocatePage(pageId)) {
            guard.getData().putInt(5678);
        }

        bpm.flushAllPages();

        ByteBuffer persisted = storage.read(pageId);

        assertThat(persisted.getInt()).isEqualTo(5678);
    }

    private static BufferPoolManager bufferPoolManagerWithFrames(Storage storage, int frameCount) {
        Map<FrameId, Frame> frames = new HashMap<>();
        for (int i = 0; i < frameCount; i++) {
            FrameId id = new FrameId(i);
            frames.put(id, Frame.create(id));
        }

        return BufferPoolManager.create(
                FCFSDiskScheduler.create(Executors.newSingleThreadExecutor(), storage),
                ArcReplacer.create(frameCount),
                frames,
                new HashMap<>()
        );
    }
}
