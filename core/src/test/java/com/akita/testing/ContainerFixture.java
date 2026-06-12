package com.akita.testing;

import com.akita.buffer.PageId;
import com.akita.heap.HeapFileHeader;
import com.akita.heap.ObjectType;
import com.akita.storage.ContainerId;
import com.akita.storage.Storage;

import java.nio.ByteBuffer;

public class ContainerFixture {
    private final Storage storage;

    private ContainerFixture(Storage storage) {
        this.storage = storage;
    }

    public static ContainerFixture create(Storage storage) {
        return new ContainerFixture(storage);
    }

    /**
     * Creates a container whose first page (block 0) contains the container
     * header followed by an empty page directory.
     */
    public ContainerId createTable() throws Exception {
        ContainerId id = storage.createContainer();
        writeFirstPageDirectory(id);
        return id;
    }

    private void writeFirstPageDirectory(ContainerId id) throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(Storage.PAGE_SIZE);
        // Container header: ObjectType.TABLE = 0
        HeapFileHeader.write(buf, ObjectType.TABLE);
        // SlottedPage header: numberOfSlots = 0
        buf.putShort((short) 0);
        // PageDirectory extended header: nextBlockPointer = 0 (no next dir page)
        buf.putShort((short) 0);
        storage.write(new PageId(id, 0), buf);
    }
}
