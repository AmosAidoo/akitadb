package com.akita.buffer.guards;

import com.akita.buffer.BufferPoolManager;
import com.akita.buffer.Frame;
import com.akita.buffer.PageId;
import com.akita.buffer.replacers.Replacer;

import java.nio.ByteBuffer;

public class WritePageGuard implements PageGuard {
    private final PageId pageId;
    private final Frame frame;
    private final Replacer replacer;
    private final BufferPoolManager bufferPoolManager;

    private WritePageGuard(PageId pageId, Frame frame, Replacer replacer, BufferPoolManager bufferPoolManager) {
        this.pageId = pageId;
        this.frame = frame;
        this.replacer = replacer;
        this.bufferPoolManager = bufferPoolManager;
    }

    public static WritePageGuard create(PageId pageId, Frame frame, Replacer replacer, BufferPoolManager bufferPoolManager) {
        frame.getWriteLatch().lock();
        frame.pin();
        replacer.setEvictable(frame.getFrameId(), false);
        return new WritePageGuard(pageId, frame, replacer, bufferPoolManager);
    }

    @Override
    public PageId getPageId() {
        return pageId;
    }

    @Override
    public ByteBuffer getData() {
        return frame.getData();
    }

    @Override
    public void close() {
        frame.markDirty();
        frame.unpin();
        replacer.setEvictable(frame.getFrameId(), true);
        frame.getWriteLatch().unlock();
        bufferPoolManager.onPageUnpinned(pageId);
    }
}
