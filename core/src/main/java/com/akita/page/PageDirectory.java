package com.akita.page;

import com.akita.buffer.BufferPoolManager;
import com.akita.buffer.PageId;
import com.akita.buffer.guards.ReadPageGuard;
import com.akita.heap.HeapFileHeader;
import com.akita.storage.ContainerId;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * PageDirectory is a special page that contains metadata about pages in a database file
 * Page header: nextBlockPointer
 * Each element: blockNumber, freeSpace,
 */
public class PageDirectory extends SlottedPage {
    public final static long FIRST_PAGE_DIRECTORY_NUMBER = 0;

    private final ContainerId containerId;
    private final BufferPoolManager bufferPoolManager;
    private final long blockNumber;

    // PageDirectory specific headers
    short nextBlockPointer;

    private PageDirectory next;

    private PageDirectory(ContainerId containerId) {
        this(containerId, null, FIRST_PAGE_DIRECTORY_NUMBER);
    }

    private PageDirectory(ContainerId containerId, BufferPoolManager bufferPoolManager, long blockNumber) {
        this.containerId = containerId;
        this.bufferPoolManager = bufferPoolManager;
        this.blockNumber = blockNumber;
        this.slots = new ArrayList<>();
    }

    public static PageDirectory create(ContainerId containerId) {
        return new PageDirectory(containerId);
    }

    @Override
    protected void parseExtendedHeader(ByteBuffer data) {
        this.nextBlockPointer = data.getShort();
    }

    @Override
    protected int headerSize() {
        int pageDirectoryHeaderSize = PageHeader.SIZE + Short.BYTES;
        if (blockNumber == FIRST_PAGE_DIRECTORY_NUMBER) {
            return HeapFileHeader.SIZE + pageDirectoryHeaderSize;
        }
        return pageDirectoryHeaderSize;
    }

    @Override
    protected int numberOfSlotsOffset() {
        if (blockNumber == FIRST_PAGE_DIRECTORY_NUMBER) {
            return HeapFileHeader.SIZE + PageHeader.NUMBER_OF_SLOTS_OFFSET;
        }
        return PageHeader.NUMBER_OF_SLOTS_OFFSET;
    }

    public void parseFirstPage(ByteBuffer data) {
        data.position(HeapFileHeader.SIZE);
        parsePage(data);
    }

    public static PageDirectory load(
            ContainerId containerId,
            BufferPoolManager bufferPoolManager
    ) throws Exception {
        try (ReadPageGuard pageGuard = bufferPoolManager.readPage(
                new PageId(containerId, FIRST_PAGE_DIRECTORY_NUMBER)
        )) {
            PageDirectory first = new PageDirectory(
                    containerId,
                    bufferPoolManager,
                    FIRST_PAGE_DIRECTORY_NUMBER
            );
            first.parseFirstPage(pageGuard.getData());
            return first;
        }
    }

    public PageId findPageWithTargetSpace(int targetSpace) throws Exception {
        // The tuples in a page directory are of the format (blockNumber, freeSpace)
        PageDirectory current = this;
        while (current != null) {
            for (Slot slot : current.slots) {
                Tuple tuple = current.getTuple(slot.getOffset());
                long blockNumber = tuple.readLong();
                int freeSpace = tuple.readInt();
                if (freeSpace >= targetSpace) {
                    return new PageId(containerId, blockNumber);
                }
            }
            current = current.next();
        }
        return null;
    }

    public PageId dataPageId(Slot slot) {
        Tuple tuple = getTuple(slot.getOffset());
        return new PageId(containerId, tuple.readLong());
    }

    public void insertEntry(Tuple tuple) {
        insertTupleRaw(tuple);
    }

    public void cacheInsertedEntry(Tuple tuple) {
        int lastOffsetBase = lowestTupleOffset();
        Slot newSlot = Slot.create((short) slots.size(), (short) (lastOffsetBase - tuple.size()), (short) tuple.size());
        slots.add(newSlot);
        pageHeader.setNumberOfSlots((short) slots.size());
    }

    public PageId pageId() {
        return new PageId(containerId, blockNumber);
    }

    public PageDirectory next() throws Exception {
        if (next != null || nextBlockPointer == 0) {
            return next;
        }
        if (bufferPoolManager == null) {
            return null;
        }

        try (ReadPageGuard pageGuard = bufferPoolManager.readPage(new PageId(containerId, nextBlockPointer))) {
            PageDirectory loaded = new PageDirectory(containerId, bufferPoolManager, nextBlockPointer);
            loaded.parsePage(pageGuard.getData());
            next = loaded;
            return next;
        }
    }

    public void cacheNextDirectory(PageDirectory next) {
        this.next = next;
        this.nextBlockPointer = (short) next.pageId().blockNumber();
    }

    @Override
    public Tuple getTuple(short offset) {
        return super.getTuple(offset);
    }

    public static Tuple createTuple(long blockNumber, int freeSpace) {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        buf.putLong(blockNumber);
        buf.putInt(freeSpace);
        buf.clear();
        return new Tuple(buf);
    }

    public long getFreeSpaceForPage(PageId pageId) throws Exception {
        PageDirectory current = this;
        while (current != null) {
            for (Slot slot : current.slots) {
                Tuple entry = current.getTuple(slot.getOffset());
                long blockNumber = entry.readLong();
                int freeSpace = entry.readInt();
                if (blockNumber == pageId.blockNumber()) {
                    return freeSpace;
                }
            }
            current = current.next();
        }
        return -1;
    }

    public long highestKnownBlockNumber() {
        long highest = FIRST_PAGE_DIRECTORY_NUMBER;
        PageDirectory current = this;

        while (current != null) {
            for (Slot slot : current.getSlots()) {
                Tuple entry = current.getTuple(slot.getOffset());
                long blockNumber = entry.readLong();
                if (blockNumber > highest) {
                    highest = blockNumber;
                }
            }
            if (current.nextBlockPointer > highest) {
                highest = current.nextBlockPointer;
            }
            try {
                current = current.next();
            } catch (Exception e) {
                throw new IllegalStateException("Unable to load next page directory", e);
            }
        }

        return highest;
    }
}
