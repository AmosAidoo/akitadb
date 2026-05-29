package com.akita.heap;

import com.akita.buffer.BufferPoolManager;
import com.akita.buffer.PageId;
import com.akita.buffer.guards.ReadPageGuard;
import com.akita.buffer.guards.WritePageGuard;
import com.akita.page.PageDirectory;
import com.akita.page.PageHeader;
import com.akita.page.RecordId;
import com.akita.page.Slot;
import com.akita.page.Tuple;
import com.akita.storage.ContainerId;

import java.nio.ByteBuffer;

public class HeapFile {
    final PageDirectory pageDirectory;
    private final ContainerId containerId;
    private final BufferPoolManager bufferPoolManager;

    private HeapFile(ContainerId containerId, PageDirectory pageDirectory, BufferPoolManager bufferPoolManager) {
        this.containerId = containerId;
        this.pageDirectory = pageDirectory;
        this.bufferPoolManager = bufferPoolManager;
    }

    public static HeapFile open(ContainerId containerId, BufferPoolManager bufferPoolManager) throws Exception {
        ReadPageGuard headerGuard = bufferPoolManager.readPage(
                new PageId(containerId, PageDirectory.FIRST_PAGE_DIRECTORY_NUMBER)
        );
        HeapFileHeader heapFileHeader = HeapFileHeader.parse(headerGuard.getData());
        headerGuard.close();

        PageDirectory pageDirectory = PageDirectory.load(
                containerId,
                bufferPoolManager
        );

        return new HeapFile(containerId, pageDirectory, bufferPoolManager);
    }

    public Tuple getTuple(RecordId recordId) throws Exception {
        try (HeapPage heapPage = HeapPage.create(bufferPoolManager.readPage(recordId.pageId()))) {
            return heapPage.getTuple(recordId.slotOffset());
        }
    }

    public HeapFileScan scanTuples() {
        return new HeapFileScan(pageDirectory, bufferPoolManager);
    }

    /**
     * Inserts a new tuple into this file and updates the relevant
     * page directory to reflect the changes
     * @param tuple Tule to be inserted
     * @return record id of newly inserted tuple
     * @throws Exception
     */
    public RecordId insertTuple(Tuple tuple) throws Exception {
        int targetSpace = tuple.serializedSize();
        PageId targetPage = findPageWithTargetSpace(targetSpace);
        if (targetPage != null) {
            int remainingFreeSpace;
            Slot slot;
            try (HeapPage heapPage = HeapPage.create(bufferPoolManager.writePage(targetPage))) {
                slot = heapPage.insertTuple(tuple);
                remainingFreeSpace = heapPage.getFreeSpace();
            }
            updatePageDirectoryEntry(targetPage, remainingFreeSpace);
            return new RecordId(targetPage, slot.getOffset());
        }
        PageId newPage = allocateDataPage();
        int remainingFreeSpace;
        Slot slot;
        try (HeapPage heapPage = HeapPage.create(bufferPoolManager.writePage(newPage))) {
            slot = heapPage.insertTuple(tuple);
            remainingFreeSpace = heapPage.getFreeSpace();
        }
        addPageDirectoryEntry(newPage, remainingFreeSpace);
        return new RecordId(newPage, slot.getOffset());
    }

    private PageId allocateDataPage() throws Exception {
        PageId pageId = new PageId(containerId, pageDirectory.highestKnownBlockNumber() + 1);
        try (WritePageGuard guard = bufferPoolManager.allocatePage(pageId)) {
            guard.getData().putShort(PageHeader.NUMBER_OF_SLOTS_OFFSET, (short) 0);
        }
        return pageId;
    }

    private void addPageDirectoryEntry(PageId dataPageId, int freeSpace) throws Exception {
        Tuple entry = PageDirectory.createTuple(dataPageId.blockNumber(), freeSpace);
        PageDirectory target = writableDirectoryFor(entry.serializedSize(), dataPageId.blockNumber() + 1);
        try (WritePageGuard dirGuard = bufferPoolManager.writePage(target.pageId())) {
            ByteBuffer dirData = dirGuard.getData();
            PageDirectory dir = PageDirectory.create(dataPageId.containerId());
            if (target.pageId().blockNumber() == PageDirectory.FIRST_PAGE_DIRECTORY_NUMBER) {
                dir.parseFirstPage(dirData);
            } else {
                dir.parsePage(dirData);
            }
            dir.insertEntry(entry);
        }
        target.cacheInsertedEntry(entry);
    }

    private PageDirectory writableDirectoryFor(int requiredSpace, long minimumNewBlockNumber) throws Exception {
        PageDirectory current = pageDirectory;
        PageDirectory previous = null;
        while (current != null) {
            if (current.getFreeSpace() >= requiredSpace) {
                return current;
            }
            previous = current;
            current = current.next();
        }
        return allocateDirectoryPage(previous, minimumNewBlockNumber);
    }

    private PageDirectory allocateDirectoryPage(PageDirectory previous, long minimumNewBlockNumber) throws Exception {
        if (previous == null) {
            throw new IllegalStateException("Heap file must have a first page directory");
        }
        PageId directoryPageId = new PageId(containerId, Math.max(pageDirectory.highestKnownBlockNumber() + 1, minimumNewBlockNumber));
        try (WritePageGuard guard = bufferPoolManager.allocatePage(directoryPageId)) {
            ByteBuffer data = guard.getData();
            data.putShort(PageHeader.NUMBER_OF_SLOTS_OFFSET, (short) 0);
            data.putShort(Short.BYTES, (short) 0);
        }
        try (WritePageGuard previousGuard = bufferPoolManager.writePage(previous.pageId())) {
            int nextPointerOffset = previous.pageId().blockNumber() == PageDirectory.FIRST_PAGE_DIRECTORY_NUMBER
                    ? HeapFileHeader.SIZE + PageHeader.SIZE
                    : PageHeader.SIZE;
            previousGuard.getData().putShort(nextPointerOffset, (short) directoryPageId.blockNumber());
        }
        PageDirectory current = PageDirectory.load(containerId, bufferPoolManager);
        while (current != null) {
            if (current.pageId().equals(directoryPageId)) {
                previous.cacheNextDirectory(current);
                return current;
            }
            current = current.next();
        }
        throw new IllegalStateException("Unable to load allocated page directory: " + directoryPageId);
    }

    private PageId findPageWithTargetSpace(int targetSpace) throws Exception {
        return pageDirectory.findPageWithTargetSpace(targetSpace);
    }

    private void updatePageDirectoryEntry(PageId targetPage, int newFreeSpace) throws Exception {
        PageDirectory current = pageDirectory;
        while (current != null) {
            if (updatePageDirectoryEntry(current, targetPage, newFreeSpace)) {
                return;
            }
            current = current.next();
        }
    }

    private boolean updatePageDirectoryEntry(PageDirectory directory, PageId targetPage, int newFreeSpace) throws Exception {
        try (WritePageGuard dirGuard = bufferPoolManager.writePage(directory.pageId())) {
            ByteBuffer dirData = dirGuard.getData();
            PageDirectory dir = PageDirectory.create(targetPage.containerId());
            if (directory.pageId().blockNumber() == PageDirectory.FIRST_PAGE_DIRECTORY_NUMBER) {
                dir.parseFirstPage(dirData);
            } else {
                dir.parsePage(dirData);
            }

            for (Slot slot : dir.getSlots()) {
                Tuple entry = dir.getTuple(slot.getOffset());
                long blockNumber = entry.readLong();
                if (blockNumber == targetPage.blockNumber()) {
                    Tuple updated = PageDirectory.createTuple(blockNumber, newFreeSpace);
                    dir.updateTuple(slot, updated);
                    return true;
                }
            }
        }
        return false;
    }
}
