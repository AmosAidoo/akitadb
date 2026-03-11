package com.akita.buffer;

import com.akita.buffer.guards.ReadPageGuard;
import com.akita.buffer.guards.WritePageGuard;
import com.akita.buffer.replacers.Replacer;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

/**
 * BufferPoolManager is responsible for fetching blocks from the storage
 * device with the {@link DiskScheduler}
 */
public class BufferPoolManager {
    private final ExecutorService callbackExecutor = Executors.newSingleThreadExecutor();
    private final ReentrantLock latch = new ReentrantLock();
    private final DiskScheduler diskScheduler;
    private final Replacer replacer;
    private final Map<FrameId, Frame> frames;
    private final Queue<Frame> freeFrames;
    private final Map<PageId, Frame> pageTable;

    private BufferPoolManager(DiskScheduler diskScheduler, Replacer replacer, Map<FrameId, Frame> frames, Map<PageId, Frame> pageTable) {
        this.diskScheduler = diskScheduler;
        this.replacer = replacer;
        this.pageTable = pageTable;
        this.frames = frames;
        this.freeFrames = new LinkedList<>(frames.values());
    }

    public static BufferPoolManager create(DiskScheduler diskScheduler, Replacer replacer, Map<FrameId, Frame> frames, Map<PageId, Frame> pageTable) {
        return new BufferPoolManager(diskScheduler, replacer, frames, pageTable);
    }

    public ReadPageGuard readPage(PageId pageId) throws InterruptedException, ExecutionException {
        FrameId frameId;
        while (true) {
            latch.lock();
            try {
                Frame frame = pageTable.get(pageId);
                if (frame != null) {
                    replacer.recordAccess(frame.getFrameId(), pageId);
                    return ReadPageGuard.create(pageId, frame, replacer);
                }

                if (!freeFrames.isEmpty()) {
                    frameId = freeFrames.remove().getFrameId();
                    break;
                }

                frameId = replacer.evict();
                if (frameId != null) {
                    break;
                }
            } finally {
                latch.unlock();
            }
        }

        Future<ByteBuffer> future = diskScheduler.schedulePageRead(pageId);
        ByteBuffer data = future.get();
        latch.lock();
        try {
            Frame frame = frames.get(frameId);
            pageTable.put(pageId, frame);
            replacer.recordAccess(frameId, pageId);
            return ReadPageGuard.create(pageId, frame, replacer);
        } finally {
            latch.unlock();
        }
    }

    public WritePageGuard writePage(PageId pageId) throws InterruptedException, ExecutionException {
        FrameId frameId;
        while (true) {
            latch.lock();
            try {
                Frame frame = pageTable.get(pageId);
                if (frame != null) {
                    replacer.recordAccess(frame.getFrameId(), pageId);
                    return WritePageGuard.create(pageId, frame, this);
                }

                if (!freeFrames.isEmpty()) {
                    frameId = freeFrames.remove().getFrameId();
                    break;
                }

                frameId = replacer.evict();
                if (frameId != null) {
                    break;
                }
            } finally {
                latch.unlock();
            }
        }

        Future<ByteBuffer> future = diskScheduler.schedulePageRead(pageId);
        ByteBuffer data = future.get();
        latch.lock();
        try {
            Frame frame = frames.get(frameId);
            pageTable.put(pageId, frame);
            replacer.recordAccess(frameId, pageId);
            return WritePageGuard.create(pageId, frame, this);
        } finally {
            latch.unlock();
        }
    }

    public boolean flushPage(PageId pageId) {
        latch.lock();
        Frame frame;
        try {
            frame = pageTable.get(pageId);
            if (frame == null) {
                return false;
            }
        } finally {
            latch.unlock();
        }

        frame.getWriteLatch().lock();
        try {
            if (frame.getIsDirty() &&
                    (frame.getPendingWrite() == null || frame.getPendingWrite().isDone())) {

                Future<?> writeFuture = diskScheduler.schedulePageWrite(pageId, frame.getData());
                frame.setPendingWrite(writeFuture);

                callbackExecutor.submit(() -> {
                    try {
                        writeFuture.get();
                        frame.setIsDirty(false);
                        frame.setPendingWrite(null);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
        } finally {
            frame.getWriteLatch().unlock();
        }
        return true;
    }

    public Integer getPinCount(PageId pageId) {
        latch.lock();
        try {
            Frame frame = pageTable.get(pageId);
            if (frame == null) {
                return null;
            }
            return frame.getPinCount();
        } finally {
            latch.unlock();
        }
    }

    public boolean deletePage(PageId pageId) {
        latch.lock();
        try {
            Frame frame = pageTable.get(pageId);
            if (frame == null) {
                throw new IllegalStateException("Page should exist anytime deletePage is called");
            }
            if (frame.getPinCount() > 0) {
                return false;
            }
            pageTable.remove(pageId);
            freeFrames.add(frame);
            replacer.remove(frame.getFrameId());
            // TODO: Deallocate this page in disk scheduler to make it available again
            return true;
        } finally {
            latch.unlock();
        }
    }
}
