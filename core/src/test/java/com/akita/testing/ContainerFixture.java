package com.akita.testing;

import com.akita.storage.*;
import com.akita.heap.HeapFileHeader;
import com.akita.heap.ObjectType;

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
     * Creates a container whose first page (block 0) contains the container
     * header followed by an empty page directory.
     */
    public ContainerId createTable() throws Exception {
        ContainerId id = containerManager.createContainer();
        writeFirstPageDirectory(id);
        return id;
    }

    private void writeFirstPageDirectory(ContainerId id) throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(BlockManager.BLOCK_SIZE);
        // Container header: ObjectType.TABLE = 0
        HeapFileHeader.write(buf, ObjectType.TABLE);
        // SlottedPage header: numberOfSlots = 0
        buf.putShort((short) 0);
        // PageDirectory extended header: nextBlockPointer = 0 (no next dir page)
        buf.putShort((short) 0);
        blockManager.writeBlock(id, 0, buf);
    }
}
