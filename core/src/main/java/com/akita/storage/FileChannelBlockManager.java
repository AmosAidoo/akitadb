package com.akita.storage;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * FileChannelBlockManager is an interface that relies on the Path interface
 * to create physical containers on the current platform's filesystem.
 */
public class FileChannelBlockManager implements BlockManager {
    private final FileChannelVFS vfs;
    private final BlockManagerMetrics metrics;

    private FileChannelBlockManager(FileChannelVFS vfs, BlockManagerMetrics metrics) {
        this.vfs = vfs;
        this.metrics = metrics;
    }

    public static FileChannelBlockManager create(FileChannelVFS vfs) {
        return create(vfs, new BlockManagerMetrics());
    }

    public static FileChannelBlockManager create(FileChannelVFS vfs, BlockManagerMetrics metrics) {
        return new FileChannelBlockManager(vfs, metrics);
    }

    public BlockManagerMetrics metrics() {
        return metrics;
    }

    @Override
    public void allocateBlock(ContainerId containerId, long blockNumber) throws IOException {
        metrics.recordAllocateBlockRequest();
        VFSFile file = vfs.open(containerId, OpenMode.WRITE, OpenMode.CREATE);
        long numberOfBlocks = file.size() / BLOCK_SIZE;

        ByteBuffer buffer = ByteBuffer.allocate(BLOCK_SIZE);
        while (numberOfBlocks <= blockNumber) {
            metrics.recordBlockZeroFilled();
            file.write(buffer, numberOfBlocks * BLOCK_SIZE);
            numberOfBlocks++;
            buffer.clear();
        }
        file.close();
    }

    @Override
    public void writeBlock(ContainerId containerId, long blockNumber, ByteBuffer buffer) throws IOException, IllegalArgumentException {
        metrics.recordWriteBlockRequest();
        ByteBuffer toWrite = buffer.duplicate();
        toWrite.clear();
        if (toWrite.capacity() != BLOCK_SIZE) {
            throw new IllegalArgumentException("Buffer must be exactly BLOCK_SIZE bytes");
        }
        VFSFile file = vfs.open(containerId, OpenMode.WRITE, OpenMode.CREATE);
        if (!blockExists(file, blockNumber)) {
            allocateBlock(containerId, blockNumber);
        }
        file.write(toWrite, blockNumber * BLOCK_SIZE);
        file.close();
    }

    @Override
    public void readBlock(ContainerId containerId, long blockNumber, ByteBuffer buffer) throws IOException {
        metrics.recordReadBlockRequest();
        if (buffer.capacity() != BLOCK_SIZE) {
            throw new IllegalArgumentException("Buffer must be exactly BLOCK_SIZE bytes");
        }
        VFSFile file = vfs.open(containerId, OpenMode.READ);
        file.read(buffer, blockNumber * BLOCK_SIZE);
        file.close();
    }

    private boolean blockExists(VFSFile file, long blockNumber) throws IOException {
        long numberOfBlocks = file.size() / BLOCK_SIZE;
        return numberOfBlocks > blockNumber;
    }
}
