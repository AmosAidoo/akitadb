package com.akita;

import com.akita.buffer.BufferPoolManager;
import com.akita.buffer.BufferPoolMetrics;
import com.akita.buffer.DiskSchedulerMetrics;
import com.akita.buffer.FCFSDiskScheduler;
import com.akita.buffer.Frame;
import com.akita.buffer.FrameId;
import com.akita.buffer.PageId;
import com.akita.buffer.guards.ReadPageGuard;
import com.akita.buffer.replacers.arc.ArcReplacer;
import com.akita.buffer.replacers.arc.ArcReplacerMetrics;
import com.akita.storage.BlockManagerMetrics;
import com.akita.storage.BlockManager;
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
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.ByteBuffer;
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
@State(Scope.Thread)
public class BufferPoolWorkingSetBenchmark {
    private static final int READS_PER_INVOCATION = 256;

    @Param({"8", "64"})
    public int bufferPoolPages;

    @Param({"8", "64", "512"})
    public int workingSetPages;

    private BenchmarkContext ctx;
    private ArcReplacer replacer;
    private PageId[] pageIds;
    private int nextPageIndex;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        replacer = ArcReplacer.create(bufferPoolPages);
        ctx = BenchmarkContext.create(bufferPoolPages, replacer);
        ContainerId containerId = ctx.containerManager.createContainer();
        pageIds = new PageId[workingSetPages];

        for (int i = 0; i < workingSetPages; i++) {
            PageId pageId = new PageId(containerId, i);
            pageIds[i] = pageId;
            ByteBuffer page = ByteBuffer.allocate(BlockManager.BLOCK_SIZE);
            page.putInt(i);
            ctx.blockManager.writeBlock(containerId, i, page);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        BufferPoolMetrics.Snapshot buffer = ctx.bufferPoolManager.metrics().snapshot();
        DiskSchedulerMetrics.Snapshot disk = ctx.diskMetrics.snapshot();
        BlockManagerMetrics.Snapshot block = ctx.blockManager.metrics().snapshot();
        ArcReplacerMetrics.Snapshot arc = replacer.metrics();

        if (buffer.readPageRequests() == 0) {
            throw new IllegalStateException("Working-set benchmark did not read any pages");
        }
        if (buffer.readPageHits() + buffer.readPageMisses() != buffer.readPageRequests()) {
            throw new IllegalStateException("Read hit/miss counters do not add up: " + buffer);
        }
        if (disk.readRequests() != buffer.readPageMisses() || block.readBlockRequests() != buffer.readPageMisses()) {
            throw new IllegalStateException(
                    "Each buffer miss should reach disk/block storage: buffer=" + buffer +
                            " disk=" + disk +
                            " block=" + block
            );
        }

        System.out.println("[akita.derived] " +
                "buffer_pool_pages=" + bufferPoolPages +
                " working_set_pages=" + workingSetPages +
                " hit_ratio=" + ratio(buffer.readPageHits(), buffer.readPageRequests()) +
                " miss_ratio=" + ratio(buffer.readPageMisses(), buffer.readPageRequests()) +
                " evictions_per_read=" + ratio(buffer.frameEvictions(), buffer.readPageRequests()));
        System.out.println(arc.toLine());

        ctx.close();
    }

    @Benchmark
    @OperationsPerInvocation(READS_PER_INVOCATION)
    public int sequentialReadWorkingSet() throws Exception {
        int sum = 0;
        for (int i = 0; i < READS_PER_INVOCATION; i++) {
            PageId pageId = pageIds[nextPageIndex];
            nextPageIndex = (nextPageIndex + 1) % pageIds.length;
            try (ReadPageGuard guard = ctx.bufferPoolManager.readPage(pageId)) {
                sum += guard.getData().getInt();
            }
        }
        return sum;
    }

    private static String ratio(long numerator, long denominator) {
        if (denominator == 0) {
            return "0.000";
        }
        return String.format("%.3f", (double) numerator / denominator);
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

        private static BenchmarkContext create(int frameCount, ArcReplacer replacer) throws Exception {
            Path tempDir = Files.createTempDirectory("akita-working-set-benchmark-");
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
                    replacer,
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
