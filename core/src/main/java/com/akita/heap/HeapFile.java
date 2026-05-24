package com.akita.heap;

import com.akita.buffer.BufferPoolManager;
import com.akita.buffer.PageId;
import com.akita.buffer.guards.ReadPageGuard;
import com.akita.buffer.guards.WritePageGuard;
import com.akita.page.PageDirectory;
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
        // TODO: allocate a new page when no free space exists (I think will be handled by caller)
        return null;
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
