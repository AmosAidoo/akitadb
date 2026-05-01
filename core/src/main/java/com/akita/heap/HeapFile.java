package com.akita.heap;

import com.akita.buffer.BufferPoolManager;
import com.akita.buffer.PageId;
import com.akita.buffer.guards.ReadPageGuard;
import com.akita.page.PageDirectory;
import com.akita.page.RecordId;
import com.akita.page.Tuple;
import com.akita.storage.ContainerId;

public class HeapFile {
    private final PageDirectory pageDirectory;
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

    Tuple getTuple(RecordId recordId) throws Exception {
        try (HeapPage heapPage = HeapPage.create(bufferPoolManager.readPage(recordId.pageId()))) {
            return heapPage.getTuple(recordId.slot());
        }
    }
}
