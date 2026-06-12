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
import com.akita.storage.BlockManager;
import com.akita.storage.BlockManagerMetrics;
import com.akita.storage.ContainerId;
import com.akita.storage.FileChannelBlockManager;
import com.akita.storage.FileChannelContainerManager;
import com.akita.storage.FileChannelVFS;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class StorageYcsbInspiredWorkload {
    public static void main(String[] args) throws Exception {
        Config config = Config.fromSystemProperties();
        runWorkloadCReadOnly(config);
        runWorkloadBReadMostly(config);
        runWorkloadAUpdateHeavy(config);
    }

    private static void runWorkloadCReadOnly(Config config) throws Exception {
        runWorkload(
                config,
                "C",
                "YCSB-C inspired read-only",
                "100% page reads with skew toward a hot set that fits in the buffer pool.",
                operation -> Operation.READ
        );
    }

    private static void runWorkloadBReadMostly(Config config) throws Exception {
        runWorkload(
                config,
                "B",
                "YCSB-B inspired read-mostly",
                "95% page reads and 5% page updates with skew toward a hot set.",
                operation -> operation % 20 == 0 ? Operation.WRITE : Operation.READ
        );
    }

    private static void runWorkloadAUpdateHeavy(Config config) throws Exception {
        runWorkload(
                config,
                "A",
                "YCSB-A inspired update-heavy",
                "50% page reads and 50% page updates with skew toward a hot set.",
                operation -> operation % 2 == 0 ? Operation.WRITE : Operation.READ
        );
    }

    private static void runWorkload(
            Config config,
            String workloadId,
            String name,
            String expectation,
            OperationMix operationMix
    ) throws Exception {
        try (WorkloadContext ctx = WorkloadContext.create(config.bufferPoolPages())) {
            ContainerId containerId = ctx.containerManager.createContainer();
            PageId[] pageIds = seedDataset(config, ctx.blockManager, containerId);

            long logicalReads = 0;
            long logicalWrites = 0;
            int checksum = 0;

            for (int operation = 0; operation < config.operations(); operation++) {
                PageId pageId = pageIds[pageIndex(config, operation)];
                if (operationMix.operationAt(operation) == Operation.WRITE) {
                    logicalWrites++;
                    try (WritePageGuard guard = ctx.bufferPoolManager.writePage(pageId)) {
                        guard.getData().putInt(0, operation);
                    }
                } else {
                    logicalReads++;
                    try (ReadPageGuard guard = ctx.bufferPoolManager.readPage(pageId)) {
                        checksum += guard.getData().getInt(0);
                    }
                }
            }

            printScenario(config, workloadId, name, expectation, logicalReads, logicalWrites, checksum, ctx);
        }
    }

    private static PageId[] seedDataset(Config config, FileChannelBlockManager blockManager, ContainerId containerId) throws Exception {
        PageId[] pageIds = new PageId[config.datasetPages()];
        for (int i = 0; i < config.datasetPages(); i++) {
            PageId pageId = new PageId(containerId, i);
            pageIds[i] = pageId;
            ByteBuffer page = ByteBuffer.allocate(BlockManager.BLOCK_SIZE);
            page.putInt(0, i);
            blockManager.writeBlock(containerId, i, page);
        }
        return pageIds;
    }

    private static int pageIndex(Config config, int operation) {
        if (operation % 5 != 0) {
            return Math.floorMod(operation * 7 + 3, config.hotSetPages());
        }
        return Math.floorMod(operation * 37 + 17, config.datasetPages());
    }

    private static void printScenario(
            Config config,
            String workloadId,
            String name,
            String expectation,
            long logicalReads,
            long logicalWrites,
            int checksum,
            WorkloadContext ctx
    ) {
        BufferPoolMetrics.Snapshot buffer = ctx.bufferPoolManager.metrics().snapshot();
        DiskSchedulerMetrics.Snapshot disk = ctx.diskMetrics.snapshot();
        BlockManagerMetrics.Snapshot block = ctx.blockManager.metrics().snapshot();

        System.out.println();
        System.out.println("=== " + name + " ===");
        System.out.println("expectation: " + expectation);
        System.out.println("config: " + config.toLine());
        System.out.println("logical: reads=" + logicalReads +
                " writes=" + logicalWrites +
                " checksum=" + checksum);
        System.out.println(buffer.toLine());
        System.out.println(disk.toLine());
        System.out.println(block.toLine());
        System.out.println(summary(buffer, disk, block, logicalReads, logicalWrites));
        System.out.println(baseline(config, workloadId, buffer, disk, block, logicalReads, logicalWrites));
    }

    private static String summary(
            BufferPoolMetrics.Snapshot buffer,
            DiskSchedulerMetrics.Snapshot disk,
            BlockManagerMetrics.Snapshot block,
            long logicalReads,
            long logicalWrites
    ) {
        return "[akita.ycsb.summary]\n" +
                "  logical.reads=" + logicalReads +
                " logical.writes=" + logicalWrites + "\n" +
                "  buffer.read_hit_ratio=" + ratio(buffer.readPageHits(), buffer.readPageRequests()) + "\n" +
                "  buffer.evictions=" + buffer.frameEvictions() +
                " dirty_flushes=" + buffer.dirtyPageFlushes() + "\n" +
                "  disk.reads=" + disk.readRequests() +
                " writes=" + disk.writeRequests() +
                " submitted=" + disk.submittedRequests() +
                " max_queue_depth=" + disk.maxQueueDepth() + "\n" +
                "  block.reads=" + block.readBlockRequests() +
                " writes=" + block.writeBlockRequests() +
                " allocations=" + block.allocateBlockRequests() + "\n" +
                "  interpretation: " + interpretation(buffer, disk, logicalWrites);
    }

    private static String baseline(
            Config config,
            String workloadId,
            BufferPoolMetrics.Snapshot buffer,
            DiskSchedulerMetrics.Snapshot disk,
            BlockManagerMetrics.Snapshot block,
            long logicalReads,
            long logicalWrites
    ) {
        return "[akita.ycsb.baseline] " +
                "workload=" + workloadId +
                " buffer=" + config.bufferPoolPages() +
                " dataset=" + config.datasetPages() +
                " hot_set=" + config.hotSetPages() +
                " ops=" + config.operations() +
                " logical_reads=" + logicalReads +
                " logical_writes=" + logicalWrites +
                " hit_ratio=" + ratio(buffer.readPageHits(), buffer.readPageRequests()) +
                " evictions=" + buffer.frameEvictions() +
                " dirty_flushes=" + buffer.dirtyPageFlushes() +
                " disk_reads=" + disk.readRequests() +
                " disk_writes=" + disk.writeRequests() +
                " max_queue_depth=" + disk.maxQueueDepth() +
                " block_reads=" + block.readBlockRequests() +
                " block_writes=" + block.writeBlockRequests();
    }

    private static String interpretation(
            BufferPoolMetrics.Snapshot buffer,
            DiskSchedulerMetrics.Snapshot disk,
            long logicalWrites
    ) {
        if (logicalWrites == 0) {
            return "read-only pressure shows cache retention without dirty write-back.";
        }
        if (logicalWrites < buffer.readPageRequests()) {
            return "read-mostly pressure introduces occasional dirty pages while reads still dominate.";
        }
        if (disk.writeRequests() > 0 && buffer.dirtyPageFlushes() > 0) {
            return "update-heavy pressure created dirty victims that had to be flushed before reuse.";
        }
        return "compare hit ratio, evictions, and dirty flushes against the workload mix.";
    }

    private static String ratio(long numerator, long denominator) {
        if (denominator == 0) {
            return "n/a";
        }
        return String.format(Locale.ROOT, "%.3f", (double) numerator / denominator);
    }

    private record Config(
            int bufferPoolPages,
            int datasetPages,
            int hotSetPages,
            int operations
    ) {
        private static Config fromSystemProperties() {
            Config config = new Config(
                    intProperty("akita.bufferPoolPages", 32),
                    intProperty("akita.datasetPages", 128),
                    intProperty("akita.hotSetPages", 16),
                    intProperty("akita.operations", 512)
            );
            config.validate();
            return config;
        }

        private static int intProperty(String name, int defaultValue) {
            String value = System.getProperty(name);
            if (value == null || value.isBlank()) {
                return defaultValue;
            }
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Expected integer system property " + name + ", got: " + value, e);
            }
        }

        private void validate() {
            if (bufferPoolPages <= 0) {
                throw new IllegalArgumentException("akita.bufferPoolPages must be > 0");
            }
            if (datasetPages <= 0) {
                throw new IllegalArgumentException("akita.datasetPages must be > 0");
            }
            if (hotSetPages <= 0) {
                throw new IllegalArgumentException("akita.hotSetPages must be > 0");
            }
            if (operations <= 0) {
                throw new IllegalArgumentException("akita.operations must be > 0");
            }
            if (hotSetPages > datasetPages) {
                throw new IllegalArgumentException("akita.hotSetPages must be <= akita.datasetPages");
            }
        }

        private String toLine() {
            return "buffer_pool_pages=" + bufferPoolPages +
                    " dataset_pages=" + datasetPages +
                    " hot_set_pages=" + hotSetPages +
                    " operations=" + operations;
        }
    }

    private enum Operation {
        READ,
        WRITE
    }

    @FunctionalInterface
    private interface OperationMix {
        Operation operationAt(int operation);
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
            Path tempDir = Files.createTempDirectory("akita-ycsb-inspired-workload-");
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
