package com.akita.buffer.guards;

import com.akita.buffer.BufferPoolManager;
import com.akita.buffer.Frame;
import com.akita.buffer.PageId;

import java.nio.ByteBuffer;

public class WritePageGuard implements AutoCloseable{
    private final PageId pageId;
    private final Frame frame;
    private final BufferPoolManager bufferPoolManager;

    private WritePageGuard(PageId pageId, Frame frame, BufferPoolManager bufferPoolManager) {
        this.pageId = pageId;
        this.frame = frame;
        this.bufferPoolManager = bufferPoolManager;
    }

    public static WritePageGuard create(PageId pageId, Frame frame, BufferPoolManager bufferPoolManager) {
        frame.getWriteLatch().lock();
        frame.pin();
        return new WritePageGuard(pageId, frame, bufferPoolManager);
    }

    public PageId getPageId() {
        return pageId;
    }

    public ByteBuffer getData() {
        return frame.getData();
    }

    public boolean isDirty() {
        return frame.getIsDirty();
    }

    public void flush() {
        bufferPoolManager.flushPage(pageId);
    }

    @Override
    public void close() {
        frame.unpin();
        frame.getWriteLatch().unlock();
        bufferPoolManager.onPageUnpinned(pageId);
    }
}
