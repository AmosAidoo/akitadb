package com.akita;

import com.akita.buffer.BufferPoolManager;
import com.akita.buffer.BufferPoolMetrics;
import com.akita.buffer.DiskSchedulerMetrics;
import com.akita.buffer.FCFSDiskScheduler;
import com.akita.buffer.Frame;
import com.akita.buffer.FrameId;
import com.akita.buffer.PageId;
import com.akita.buffer.guards.ReadPageGuard;
import com.akita.buffer.guards.WritePageGuard;
import com.akita.buffer.replacers.arc.ArcReplacer;
import com.akita.storage.BlockManagerMetrics;
import com.akita.storage.ContainerId;
import com.akita.storage.FileChannelBlockManager;
import com.akita.storage.FileChannelContainerManager;
import com.akita.storage.FileChannelVFS;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class StorageMetricsWorkload {
    public static void main(String[] args) throws Exception {
        sparseBlockAllocation();
        hotBufferRead();
        coldBufferReadAfterEviction();
        dirtyEvictionFlush();
    }

    private static void sparseBlockAllocation() throws Exception {
        try (WorkloadContext ctx = WorkloadContext.create(2)) {
            ContainerId containerId = ctx.containerManager.createContainer();

            ctx.blockManager.allocateBlock(containerId, 0);
            ctx.blockManager.allocateBlock(containerId, 9);

            printScenario(
                    "sparse block allocation",
                    "One allocation request for block 0, then one for block 9. The second request zero-fills blocks 1..9.",
                    ctx
            );
        }
    }

    private static void hotBufferRead() throws Exception {
        try (WorkloadContext ctx = WorkloadContext.create(2)) {
            ContainerId containerId = ctx.containerManager.createContainer();
            PageId pageId = new PageId(containerId, 0);

            try (WritePageGuard guard = ctx.bufferPoolManager.allocatePage(pageId)) {
                guard.getData().putInt(1234);
            }

            try (ReadPageGuard guard = ctx.bufferPoolManager.readPage(pageId)) {
                guard.getData().getInt();
            }

            try (ReadPageGuard guard = ctx.bufferPoolManager.readPage(pageId)) {
                guard.getData().getInt();
            }

            printScenario(
                    "hot buffer reads",
                    "Allocate page 0, then read it twice while it is still resident. Reads should be buffer hits and should not schedule disk reads.",
                    ctx
            );
        }
    }

    private static void coldBufferReadAfterEviction() throws Exception {
        try (WorkloadContext ctx = WorkloadContext.create(2)) {
            ContainerId containerId = ctx.containerManager.createContainer();
            PageId pageId = new PageId(containerId, 0);

            try (WritePageGuard guard = ctx.bufferPoolManager.allocatePage(pageId)) {
                guard.getData().putInt(1234);
            }

            ctx.bufferPoolManager.flushPage(pageId);
            ctx.bufferPoolManager.deletePage(pageId);

            try (ReadPageGuard guard = ctx.bufferPoolManager.readPage(pageId)) {
                guard.getData().getInt();
            }

            printScenario(
                    "cold buffer read after removal",
                    "Write page 0, flush it, remove it from the buffer pool, then read it again. The read should miss and schedule one disk read.",
                    ctx
            );
        }
    }

    private static void dirtyEvictionFlush() throws Exception {
        try (WorkloadContext ctx = WorkloadContext.create(1)) {
            ContainerId containerId = ctx.containerManager.createContainer();
            PageId firstPage = new PageId(containerId, 0);
            PageId secondPage = new PageId(containerId, 1);

            try (WritePageGuard guard = ctx.bufferPoolManager.allocatePage(firstPage)) {
                guard.getData().putInt(1234);
            }

            try (WritePageGuard guard = ctx.bufferPoolManager.allocatePage(secondPage)) {
                guard.getData().putInt(5678);
            }

            printScenario(
                    "dirty eviction flush",
                    "With one frame, allocating page 1 evicts dirty page 0. Eviction should schedule one disk write.",
                    ctx
            );
        }
    }

    private static void printScenario(String name, String expectation, WorkloadContext ctx) {
        BufferPoolMetrics.Snapshot buffer = ctx.bufferPoolManager.metrics().snapshot();
        DiskSchedulerMetrics.Snapshot disk = ctx.diskMetrics.snapshot();
        BlockManagerMetrics.Snapshot block = ctx.blockManager.metrics().snapshot();

        System.out.println();
        System.out.println("=== " + name + " ===");
        System.out.println("expectation: " + expectation);
        System.out.println(buffer.toLine());
        System.out.println(disk.toLine());
        System.out.println(block.toLine());
    }

    private static class WorkloadContext implements AutoCloseable {
        private final Path tempDir;
        private final FileChannelBlockManager blockManager;
        private final FileChannelContainerManager containerManager;
        private final ExecutorService executor;
        private final DiskSchedulerMetrics diskMetrics;
        private final BufferPoolManager bufferPoolManager;

        private WorkloadContext(
                Path tempDir,
                FileChannelBlockManager blockManager,
                FileChannelContainerManager containerManager,
                ExecutorService executor,
                DiskSchedulerMetrics diskMetrics,
                BufferPoolManager bufferPoolManager
        ) {
            this.tempDir = tempDir;
            this.blockManager = blockManager;
            this.containerManager = containerManager;
            this.executor = executor;
            this.diskMetrics = diskMetrics;
            this.bufferPoolManager = bufferPoolManager;
        }

        private static WorkloadContext create(int frameCount) throws Exception {
            Path tempDir = Files.createTempDirectory("akita-metrics-workload-");
            FileChannelVFS vfs = FileChannelVFS.create(tempDir);
            FileChannelBlockManager blockManager = FileChannelBlockManager.create(vfs);
            FileChannelContainerManager containerManager = FileChannelContainerManager.create(vfs);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            DiskSchedulerMetrics diskMetrics = new DiskSchedulerMetrics();

            Map<FrameId, Frame> frames = new HashMap<>();
            for (int i = 0; i < frameCount; i++) {
                FrameId id = new FrameId(i);
                frames.put(id, Frame.create(id));
            }

            BufferPoolManager bufferPoolManager = BufferPoolManager.create(
                    FCFSDiskScheduler.create(executor, blockManager, diskMetrics),
                    ArcReplacer.create(frameCount),
                    frames,
                    new HashMap<>()
            );

            return new WorkloadContext(
                    tempDir,
                    blockManager,
                    containerManager,
                    executor,
                    diskMetrics,
                    bufferPoolManager
            );
        }

        @Override
        public void close() throws Exception {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
            try (var walk = Files.walk(tempDir)) {
                walk.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(java.io.File::delete);
            }
        }
    }
}
