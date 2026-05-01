package com.akita.testing;

import com.akita.storage.*;

import java.nio.ByteBuffer;

public class ContainerFixture {
    private final FileChannelBlockManager blockManager;
    private final FileChannelContainerManager containerManager;

    private ContainerFixture(FileChannelBlockManager bm, FileChannelContainerManager cm) {
        this.blockManager = bm;
        this.containerManager = cm;
    }

    public static ContainerFixture create(FileChannelBlockManager bm, FileChannelContainerManager cm) {
        return new ContainerFixture(bm, cm);
    }

    /**
     * Creates a container with a valid header (block 0) and an empty
     * page directory (block 1) so HeapFile.open() can proceed without
     * blowing up before the test even starts.
     */
    public ContainerId createTable() throws Exception {
        ContainerId id = containerManager.createContainer();
        writeHeader(id);
        writeEmptyPageDirectory(id);
        return id;
    }

    private void writeHeader(ContainerId id) throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(BlockManager.BLOCK_SIZE);
        buf.putInt(0); // ObjectType.TABLE = 0
        blockManager.writeBlock(id, 0, buf);
    }

    private void writeEmptyPageDirectory(ContainerId id) throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(BlockManager.BLOCK_SIZE);
        // SlottedPage header: numberOfSlots = 0
        buf.putShort((short) 0);
        // PageDirectory extended header: nextBlockPointer = 0 (no next dir page)
        buf.putShort((short) 0);
        blockManager.writeBlock(id, 1, buf);
    }
}