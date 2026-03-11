package com.akita.buffer.guards;

import com.akita.buffer.DiskScheduler;
import com.akita.buffer.Frame;
import com.akita.buffer.PageId;
import com.akita.buffer.replacers.Replacer;

import java.nio.ByteBuffer;

public class ReadPageGuard implements AutoCloseable {
    private final PageId pageId;
    private final Frame frame;
    private final Replacer replacer;

    private ReadPageGuard(PageId pageId, Frame frame, Replacer replacer) {
        this.pageId = pageId;
        this.frame = frame;
        this.replacer = replacer;
    }

    public static ReadPageGuard create(PageId pageId, Frame frame, Replacer replacer) {
        frame.getReadLatch().lock();
        frame.pin();
        replacer.setEvictable(frame.getFrameId(), false);
        return new ReadPageGuard(pageId, frame, replacer);
    }

    public PageId getPageId() {
        return pageId;
    }

    public ByteBuffer getData() {
        return frame.getReadOnlyData();
    }

    public boolean isDirty() {
        return frame.getIsDirty();
    }

    @Override
    public void close() {
        frame.unpin();
        replacer.setEvictable(frame.getFrameId(), true);
        frame.getReadLatch().unlock();
    }

    public Replacer getReplacer() {
        return replacer;
    }
}
