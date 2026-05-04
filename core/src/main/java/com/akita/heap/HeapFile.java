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
    private final BufferPoolManager bufferPoolManager;

    private HeapFile(PageDirectory pageDirectory, BufferPoolManager bufferPoolManager) {
        this.pageDirectory = pageDirectory;
        this.bufferPoolManager = bufferPoolManager;
    }

    public static HeapFile open(ContainerId containerId, BufferPoolManager bufferPoolManager) throws Exception {
        ReadPageGuard headerGuard = bufferPoolManager.readPage(new PageId(containerId, 0));
        HeapFileHeader heapFileHeader = HeapFileHeader.parse(headerGuard.getData());
        headerGuard.close();

        PageDirectory pageDirectory = PageDirectory.load(
                containerId,
                bufferPoolManager
        );

        return new HeapFile(pageDirectory, bufferPoolManager);
    }

    public Tuple getTuple(RecordId recordId) throws Exception {
        try (HeapPage heapPage = HeapPage.create(bufferPoolManager.readPage(recordId.pageId()))) {
            return heapPage.getTuple(recordId.slot());
        }
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
            return new RecordId(targetPage, slot);
        }
        // TODO: allocate a new page when no free space exists (I think will be handled by caller)
        return null;
    }

    private PageId findPageWithTargetSpace(int targetSpace) {
        PageDirectory current = pageDirectory;

        while (current != null) {
            PageId pageId = current.findPageWithTargetSpace(targetSpace);
            if (pageId != null) {
                return pageId;
            }
            current = current.next;
        }
        return null;
    }

    private void updatePageDirectoryEntry(PageId targetPage, int newFreeSpace) throws Exception {
        // For now, only the first page directory (block 1) is handled.
        // Multi-directory traversal is a TODO once lazy loading is wired up.
        PageId directoryPageId = new PageId(targetPage.containerId(), PageDirectory.FIRST_PAGE_DIRECTORY_NUMBER);

        try (WritePageGuard dirGuard = bufferPoolManager.writePage(directoryPageId)) {
            ByteBuffer dirData = dirGuard.getData();
            PageDirectory dir = PageDirectory.create(targetPage.containerId());
            dir.parsePage(dirData);

            for (Slot slot : dir.getSlots()) {
                Tuple entry = dir.getTuple(slot);
                long blockNumber = entry.readLong();
                if (blockNumber == targetPage.blockNumber()) {
                    Tuple updated = PageDirectory.createTuple(blockNumber, newFreeSpace);
                    dir.updateTuple(slot, updated);
                    break;
                }
            }
        }
    }
}
