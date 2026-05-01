package com.akita.heap;

import com.akita.buffer.BufferPoolManager;
import com.akita.buffer.PageId;
import com.akita.page.RecordId;
import com.akita.storage.ContainerId;
import com.akita.storage.FileChannelBlockManager;
import com.akita.storage.FileChannelContainerManager;
import com.akita.testing.AkitaExtension;
import com.akita.testing.ContainerFixture;
import com.akita.testing.SlottedPageWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(AkitaExtension.class)
class HeapFileTest {

    @Test
    void getTuple_returnsCorrectData(
            BufferPoolManager bpm,
            FileChannelBlockManager bm,
            FileChannelContainerManager cm
    ) throws Exception {
        ContainerId containerId = ContainerFixture.create(bm, cm).createTable();
        PageId pageId = new PageId(containerId, 2);

        List<RecordId> records = SlottedPageWriter.create(bm)
                .addShortTuple((short) 10)
                .addShortTuple((short) 20)
                .writeTo(pageId);

        HeapFile heapFile = HeapFile.open(containerId, bpm);

        assertThat(heapFile.getTuple(records.get(0)).readShort()).isEqualTo((short) 10);
        assertThat(heapFile.getTuple(records.get(1)).readShort()).isEqualTo((short) 20);
    }
}