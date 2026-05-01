package com.akita.testing;

import com.akita.buffer.PageId;
import com.akita.page.RecordId;
import com.akita.page.Slot;
import com.akita.storage.BlockManager;
import com.akita.storage.FileChannelBlockManager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SlottedPageWriter {
    private final FileChannelBlockManager blockManager;
    private final List<byte[]> tuples = new ArrayList<>();

    private SlottedPageWriter(FileChannelBlockManager blockManager) {
        this.blockManager = blockManager;
    }

    public static SlottedPageWriter create(FileChannelBlockManager blockManager) {
        return new SlottedPageWriter(blockManager);
    }

    public SlottedPageWriter addShortTuple(short value) {
        ByteBuffer b = ByteBuffer.allocate(2);
        b.putShort(value);
        tuples.add(b.array());
        return this;
    }

    public List<RecordId> writeTo(PageId pageId) throws Exception {
        ByteBuffer page = ByteBuffer.allocate(BlockManager.BLOCK_SIZE);

        // Header: number of slots
        page.putShort((short) tuples.size());

        // Compute slot offsets growing from the end of the page
        List<Slot> slots = new ArrayList<>();
        int tail = BlockManager.BLOCK_SIZE;
        for (byte[] tuple : tuples) {
            tail -= tuple.length;
            slots.add(Slot.create((short) tail, (short) tuple.length));
        }

        // Write slot directory (offset, length pairs) after the header
        for (Slot slot : slots) {
            page.putShort(slot.getOffset());
            page.putShort(slot.getLength());
        }

        // Write tuple data at their absolute offsets
        for (int i = 0; i < tuples.size(); i++) {
            page.put(slots.get(i).getOffset(), tuples.get(i));
        }

        blockManager.writeBlock(pageId.containerId(), pageId.blockNumber(), page);

        // Return RecordIds in insertion order
        List<RecordId> records = new ArrayList<>();
        for (Slot slot : slots) {
            records.add(new RecordId(pageId, slot));
        }
        return records;
    }
}