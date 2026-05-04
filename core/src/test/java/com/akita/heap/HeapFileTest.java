package com.akita.heap;

import com.akita.buffer.BufferPoolManager;
import com.akita.buffer.PageId;
import com.akita.page.PageHeader;
import com.akita.page.RecordId;
import com.akita.page.Slot;
import com.akita.page.Tuple;
import com.akita.storage.BlockManager;
import com.akita.storage.ContainerId;
import com.akita.storage.FileChannelBlockManager;
import com.akita.storage.FileChannelContainerManager;
import com.akita.testing.AkitaExtension;
import com.akita.testing.ContainerFixture;
import com.akita.testing.SlottedPageWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.ByteBuffer;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(AkitaExtension.class)
class HeapFileTest {

    @Test
    void getTupleReturnsCorrectData(
            BufferPoolManager bpm,
            FileChannelBlockManager bm,
            FileChannelContainerManager cm
    ) throws Exception {
        ContainerId containerId = ContainerFixture.create(bm, cm).createTable();
        PageId pageId = new PageId(containerId, 2);

        List<RecordId> records = SlottedPageWriter.create(bm)
                .addShortTuple((short) 10)
                .addShortTuple((short) 20)
                .writeTo(pageId, null);

        HeapFile heapFile = HeapFile.open(containerId, bpm);

        assertThat(heapFile.getTuple(records.get(0)).readShort()).isEqualTo((short) 10);
        assertThat(heapFile.getTuple(records.get(1)).readShort()).isEqualTo((short) 20);
    }

    @Test
    void shouldInsertTupleWhenSpaceIsAvailable(
            BufferPoolManager bpm,
            FileChannelBlockManager bm,
            FileChannelContainerManager cm
    ) throws Exception {
        ContainerId containerId = ContainerFixture.create(bm, cm).createTable();

        // Insert free space map for page 2
        ByteBuffer additionalHeaders = ByteBuffer.allocate(2);
        additionalHeaders.putShort((short) 0);
        SlottedPageWriter.create(bm)
                .addPageDirectoryTuple(2, BlockManager.BLOCK_SIZE - PageHeader.SIZE)
                .writeTo(new PageId(containerId, 1), additionalHeaders);

        HeapFile heapFile = HeapFile.open(containerId, bpm);

        // Insert number 10
        ByteBuffer buffer = ByteBuffer.allocate(2);
        buffer.putShort((short) 10);
        RecordId recordId = heapFile.insertTuple(new Tuple(buffer));
        assertThat(recordId).isNotNull();
        assertThat(heapFile.getTuple(recordId).readShort()).isEqualTo((short) 10);

        // Insert number 20
        buffer.clear();
        buffer.putShort((short) 20);
        recordId = heapFile.insertTuple(new Tuple(buffer));
        assertThat(recordId).isNotNull();
        assertThat(heapFile.getTuple(recordId).readShort()).isEqualTo((short) 20);
    }

    @Test
    void shouldUpdatePageDirectoryAfterInsert(
            BufferPoolManager bpm,
            FileChannelBlockManager bm,
            FileChannelContainerManager cm
    ) throws Exception {
        ContainerId containerId = ContainerFixture.create(bm, cm).createTable();
        int initialFreeSpace = BlockManager.BLOCK_SIZE - PageHeader.SIZE;

        ByteBuffer additionalHeaders = ByteBuffer.allocate(2);
        additionalHeaders.putShort((short) 0);
        SlottedPageWriter.create(bm)
                .addPageDirectoryTuple(2, initialFreeSpace)
                .writeTo(new PageId(containerId, 1), additionalHeaders);

        HeapFile heapFile = HeapFile.open(containerId, bpm);

        ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES);
        buffer.putShort((short) 42);
        Tuple tuple = new Tuple(buffer);
        heapFile.insertTuple(tuple);

        HeapFile freshHeapFile = HeapFile.open(containerId, bpm);
        PageId heapPageId = new PageId(containerId, 2);

        int expectedFreeSpace = initialFreeSpace - Slot.SERIALIZED_SIZE - Short.BYTES;
        assertThat(freshHeapFile.pageDirectory.getFreeSpaceForPage(heapPageId))
                .isEqualTo(expectedFreeSpace);
    }
}