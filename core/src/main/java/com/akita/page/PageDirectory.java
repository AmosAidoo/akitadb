package com.akita.page;

import com.akita.buffer.BufferPoolManager;
import com.akita.buffer.PageId;
import com.akita.buffer.guards.ReadPageGuard;
import com.akita.heap.HeapFileHeader;
import com.akita.storage.ContainerId;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

/**
 * PageDirectory is a special page that contains metadata about pages in a database file
 * Page header: nextBlockPointer
 * Each element: blockNumber, freeSpace,
 */
public class PageDirectory extends SlottedPage {
    public final static long FIRST_PAGE_DIRECTORY_NUMBER = 0;

    private final ContainerId containerId;

    // PageDirectory specific headers
    short nextBlockPointer;

    public PageDirectory next;

    private PageDirectory(ContainerId containerId) {
        this.containerId = containerId;
    }

    public static PageDirectory create(ContainerId containerId) {
        return new PageDirectory(containerId);
    }

    @Override
    protected void parseExtendedHeader(ByteBuffer data) {
        this.nextBlockPointer = data.getShort();
    }

    public void parseFirstPage(ByteBuffer data) {
        data.position(HeapFileHeader.SIZE);
        parsePage(data);
    }

    // TODO: Lazy load page directory
    public static PageDirectory load(
            ContainerId containerId,
            BufferPoolManager bufferPoolManager
    ) throws ExecutionException, InterruptedException {
        PageDirectory first;
        try (ReadPageGuard pageGuard = bufferPoolManager.readPage(
                new PageId(containerId, FIRST_PAGE_DIRECTORY_NUMBER)
        )) {
            first = new PageDirectory(containerId);
            first.parseFirstPage(pageGuard.getData());
        }

        PageDirectory current = first;
        long nextBlockPointer = first.nextBlockPointer;
        while (nextBlockPointer != 0) {
            PageId pageId = new PageId(containerId, nextBlockPointer);
            try (ReadPageGuard pageGuard = bufferPoolManager.readPage(pageId)) {
                ByteBuffer data = pageGuard.getData();
                PageDirectory pageDirectory = new PageDirectory(containerId);
                pageDirectory.parsePage(data);
                current.next = pageDirectory;
                current = pageDirectory;
                nextBlockPointer = pageDirectory.nextBlockPointer;
            }
        }
        return first;
    }

    public PageId findPageWithTargetSpace(int targetSpace) {
        // The tuples in a page directory are of the format (blockNumber, freeSpace)
        for (Slot slot : slots) {
            Tuple tuple = getTuple(slot.getOffset());
            long blockNumber = tuple.readLong();
            int freeSpace = tuple.readInt();
            if (freeSpace >= targetSpace) {
                return new PageId(containerId, blockNumber);
            }
        }
        return null;
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

    public long getFreeSpaceForPage(PageId pageId) {
        for (Slot slot : slots) {
            Tuple entry = getTuple(slot.getOffset());
            long blockNumber = entry.readLong();
            int freeSpace = entry.readInt();
            if (blockNumber == pageId.blockNumber()) {
                return freeSpace;
            }
        }
        return -1;
    }
}
