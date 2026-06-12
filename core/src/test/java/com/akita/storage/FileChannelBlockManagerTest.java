package com.akita.storage;

import com.akita.testing.AkitaExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(AkitaExtension.class)
class FileChannelBlockManagerTest {
    @Test
    void allocatesCorrectBlockSize(
            FileChannelBlockManager bm,
            FileChannelContainerManager cm,
            FileChannelVFS vfs  // you'll need to expose this from the extension
    ) throws Exception {
        ContainerId containerId = cm.createContainer();
        VFSFile file = vfs.open(containerId);

        bm.allocateBlock(containerId, 0);
        assertThat(file.size()).isEqualTo(BlockManager.BLOCK_SIZE);

        bm.allocateBlock(containerId, 9);
        assertThat(file.size()).isEqualTo(10L * BlockManager.BLOCK_SIZE);

        BlockManagerMetrics.Snapshot metrics = bm.metrics().snapshot();
        assertThat(metrics.allocateBlockRequests()).isEqualTo(2);
        assertThat(metrics.blocksZeroFilled()).isEqualTo(10);
        assertThat(metrics.allocateBlockTotalNanos()).isGreaterThan(0);
        assertThat(metrics.allocateBlockMaxNanos()).isGreaterThan(0);
    }

    @Test
    void recordsBlockReadAndWriteRequests(
            FileChannelBlockManager bm,
            FileChannelContainerManager cm
    ) throws Exception {
        ContainerId containerId = cm.createContainer();
        ByteBuffer writeBuffer = ByteBuffer.allocate(BlockManager.BLOCK_SIZE);
        writeBuffer.putInt(1234);

        bm.writeBlock(containerId, 0, writeBuffer);

        ByteBuffer readBuffer = ByteBuffer.allocate(BlockManager.BLOCK_SIZE);
        bm.readBlock(containerId, 0, readBuffer);
        readBuffer.clear();

        assertThat(readBuffer.getInt()).isEqualTo(1234);

        BlockManagerMetrics.Snapshot metrics = bm.metrics().snapshot();
        assertThat(metrics.writeBlockRequests()).isEqualTo(1);
        assertThat(metrics.readBlockRequests()).isEqualTo(1);
        assertThat(metrics.allocateBlockRequests()).isEqualTo(1);
        assertThat(metrics.blocksZeroFilled()).isEqualTo(1);
        assertThat(metrics.writeBlockTotalNanos()).isGreaterThan(0);
        assertThat(metrics.writeBlockMaxNanos()).isGreaterThan(0);
        assertThat(metrics.readBlockTotalNanos()).isGreaterThan(0);
        assertThat(metrics.readBlockMaxNanos()).isGreaterThan(0);
    }

//    @Test
//    void writesBlockToDiskWhenContainerExists() {}
//
//    @Test
//    void readsBlockFromDiskWhenContainerAndBlockExists() {}
//
//    @Test
//    void allocatesBlockImplicitlyWhenBlockDoesntExist() {}
}
