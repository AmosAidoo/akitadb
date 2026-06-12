package com.akita.testing;

import com.akita.buffer.PageId;
import com.akita.page.RecordId;
import com.akita.page.Slot;
import com.akita.page.Tuple;
import com.akita.storage.Storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SlottedPageWriter {
    private final Storage storage;
    private final List<byte[]> tuples = new ArrayList<>();

    private SlottedPageWriter(Storage storage) {
        this.storage = storage;
    }

    public static SlottedPageWriter create(Storage storage) {
        return new SlottedPageWriter(storage);
    }

    public SlottedPageWriter addShortTuple(short value) {
        ByteBuffer b = ByteBuffer.allocate(2);
        b.putShort(value);
        tuples.add(b.array());
        return this;
    }

    public SlottedPageWriter addTuple(Tuple tuple) {
        tuples.add(tuple.getBuffer().array());
        return this;
    }

    public SlottedPageWriter addPageDirectoryTuple(long blockNumber, int freeSpace) {
        ByteBuffer b = ByteBuffer.allocate(12);
        b.putLong(blockNumber);
        b.putInt(freeSpace);
        tuples.add(b.array());
        return this;
    }

    public List<RecordId> writeTo(PageId pageId, ByteBuffer additionalHeaders) throws Exception {
        return writeTo(pageId, null, additionalHeaders);
    }

    public List<RecordId> writeTo(PageId pageId, ByteBuffer prefixHeaders, ByteBuffer additionalHeaders) throws Exception {
        ByteBuffer page = ByteBuffer.allocate(Storage.PAGE_SIZE);

        if (prefixHeaders != null) {
            prefixHeaders.clear();
            page.put(prefixHeaders);
        }

        // Header: number of slots
        page.putShort((short) tuples.size());
        if (additionalHeaders != null) {
            additionalHeaders.clear();
            page.put(additionalHeaders);
        }

        // Compute slot offsets growing from the end of the page
        List<Slot> slots = new ArrayList<>();
        int tail = Storage.PAGE_SIZE;
        for (short i = 0; i < tuples.size(); i++) {
            byte[] tuple = tuples.get(i);
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

        storage.write(pageId, page);

        // Return RecordIds in insertion order
        List<RecordId> records = new ArrayList<>();
        for (short i = 0; i < slots.size(); i++) {
            records.add(new RecordId(pageId, i));
        }
        return records;
    }
}
