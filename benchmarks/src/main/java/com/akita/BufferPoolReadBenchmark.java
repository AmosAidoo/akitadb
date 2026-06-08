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
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class BufferPoolReadBenchmark {
    private static final int COLD_BATCH_SIZE = 1024;

    @State(Scope.Thread)
    public static class HotReadState {
        BenchmarkContext ctx;
        PageId pageId;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            ctx = BenchmarkContext.create(2);
            ContainerId containerId = ctx.containerManager.createContainer();
            pageId = new PageId(containerId, 0);

            try (WritePageGuard guard = ctx.bufferPoolManager.allocatePage(pageId)) {
                guard.getData().putInt(1234);
            }
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception {
            BufferPoolMetrics.Snapshot buffer = ctx.bufferPoolManager.metrics().snapshot();
            DiskSchedulerMetrics.Snapshot disk = ctx.diskMetrics.snapshot();
            BlockManagerMetrics.Snapshot block = ctx.blockManager.metrics().snapshot();

            if (buffer.readPageRequests() == 0) {
                throw new IllegalStateException("Hot read benchmark did not read any pages");
            }
            if (buffer.readPageHits() != buffer.readPageRequests()) {
                throw new IllegalStateException("Hot read benchmark expected every read to be a buffer hit: " + buffer);
            }
            if (buffer.readPageMisses() != 0 || disk.readRequests() != 0 || block.readBlockRequests() != 0) {
                throw new IllegalStateException(
                        "Hot read benchmark unexpectedly performed cold reads: buffer=" + buffer +
                                " disk=" + disk +
                                " block=" + block
                );
            }

            ctx.close();
        }
    }

    @State(Scope.Thread)
    public static class ColdReadState {
        BenchmarkContext ctx;
        PageId pageId;

        @Setup(Level.Invocation)
        public void setupInvocation() throws Exception {
            ctx = BenchmarkContext.create(2);
            ContainerId containerId = ctx.containerManager.createContainer();
            pageId = new PageId(containerId, 0);

            try (WritePageGuard guard = ctx.bufferPoolManager.allocatePage(pageId)) {
                guard.getData().putInt(1234);
            }

            ctx.bufferPoolManager.flushPage(pageId);
            ctx.bufferPoolManager.deletePage(pageId);
        }

        @TearDown(Level.Invocation)
        public void tearDownInvocation() throws Exception {
            ctx.close();
        }
    }

    @State(Scope.Thread)
    public static class BatchedColdReadState {
        BenchmarkContext ctx;
        PageId[] pageIds;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            ctx = BenchmarkContext.create(COLD_BATCH_SIZE);
            ContainerId containerId = ctx.containerManager.createContainer();
            pageIds = new PageId[COLD_BATCH_SIZE];

            for (int i = 0; i < COLD_BATCH_SIZE; i++) {
                PageId pageId = new PageId(containerId, i);
                pageIds[i] = pageId;
                try (WritePageGuard guard = ctx.bufferPoolManager.allocatePage(pageId)) {
                    guard.getData().putInt(i);
                }
            }

            ctx.bufferPoolManager.flushAllPages();
            for (PageId pageId : pageIds) {
                ctx.bufferPoolManager.deletePage(pageId);
            }
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception {
            BufferPoolMetrics.Snapshot buffer = ctx.bufferPoolManager.metrics().snapshot();
            DiskSchedulerMetrics.Snapshot disk = ctx.diskMetrics.snapshot();
            BlockManagerMetrics.Snapshot block = ctx.blockManager.metrics().snapshot();

            if (buffer.readPageRequests() == 0) {
                throw new IllegalStateException("Batched cold read benchmark did not read any pages");
            }
            if (buffer.readPageHits() != 0) {
                throw new IllegalStateException("Batched cold read benchmark expected no buffer hits: " + buffer);
            }
            if (buffer.readPageMisses() != buffer.readPageRequests()
                    || disk.readRequests() != buffer.readPageRequests()
                    || block.readBlockRequests() != buffer.readPageRequests()) {
                throw new IllegalStateException(
                        "Batched cold read benchmark expected every read to miss and reach storage: buffer=" + buffer +
                                " disk=" + disk +
                                " block=" + block
                );
            }

            ctx.close();
        }
    }

    @Benchmark
    public int hotReadResidentPage(HotReadState state) throws Exception {
        try (ReadPageGuard guard = state.ctx.bufferPoolManager.readPage(state.pageId)) {
            return guard.getData().getInt();
        }
    }

    @Benchmark
    @OperationsPerInvocation(COLD_BATCH_SIZE)
    public int coldReadAndRemovePreparedPages(BatchedColdReadState state) throws Exception {
        int sum = 0;
        for (PageId pageId : state.pageIds) {
            try (ReadPageGuard guard = state.ctx.bufferPoolManager.readPage(pageId)) {
                sum += guard.getData().getInt();
            }
            state.ctx.bufferPoolManager.deletePage(pageId);
        }
        return sum;
    }

    @Benchmark
    public int coldReadAfterRemoval(ColdReadState state) throws Exception {
        try (ReadPageGuard guard = state.ctx.bufferPoolManager.readPage(state.pageId)) {
            return guard.getData().getInt();
        }
    }

    private static class BenchmarkContext implements AutoCloseable {
        private final Path tempDir;
        private final FileChannelBlockManager blockManager;
        private final FileChannelContainerManager containerManager;
        private final ExecutorService executor;
        private final DiskSchedulerMetrics diskMetrics;
        private final BufferPoolManager bufferPoolManager;

        private BenchmarkContext(
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

        private static BenchmarkContext create(int frameCount) throws Exception {
            Path tempDir = Files.createTempDirectory("akita-buffer-read-benchmark-");
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

            return new BenchmarkContext(tempDir, blockManager, containerManager, executor, diskMetrics, bufferPoolManager);
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
