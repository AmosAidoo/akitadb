package com.akita.page;

import com.akita.buffer.BufferPoolManager;
import com.akita.buffer.PageId;
import com.akita.buffer.guards.ReadPageGuard;
import com.akita.storage.ContainerId;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

/**
 * PageDirectory is a special page that contains metadata about pages in a database file
 * Page header: nextBlockPointer
 * Each element: blockNumber, freeSpace,
 */
public class PageDirectory extends SlottedPage {
    final static long FIRST_PAGE_DIRECTORY_NUMBER = 1;

    // PageDirectory specific headers
    short nextBlockPointer;

    PageDirectory next;

    private PageDirectory() {}

    @Override
    protected void parseExtendedHeader(ByteBuffer data) {
        this.nextBlockPointer = data.getShort();
    }

    public static PageDirectory load(
            ContainerId containerId,
            BufferPoolManager bufferPoolManager
    ) throws ExecutionException, InterruptedException {
        PageDirectory dummy = new PageDirectory();
        PageDirectory current = dummy;
        long nextBlockPointer = FIRST_PAGE_DIRECTORY_NUMBER;
        while (nextBlockPointer != 0) {
            PageId pageId = new PageId(containerId, nextBlockPointer);
            try (ReadPageGuard pageGuard = bufferPoolManager.readPage(pageId)) {
                ByteBuffer data = pageGuard.getData();
                PageDirectory pageDirectory = new PageDirectory();
                pageDirectory.parsePage(data);
                current.next = pageDirectory;
                current = pageDirectory;
                nextBlockPointer = pageDirectory.nextBlockPointer;
            }
        }
        return dummy.next;
    }
}
