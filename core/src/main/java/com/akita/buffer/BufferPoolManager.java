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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * BufferPoolManager is responsible for fetching blocks from the storage
 * device with the {@link DiskScheduler}
 */
public class BufferPoolManager {
    private final ExecutorService callbackExecutor = Executors.newSingleThreadExecutor();
    private final ReentrantLock latch = new ReentrantLock();
    private final Condition frameAvailable = latch.newCondition();
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

    // Extracted helper: finds a free frameId or waits until one becomes available.
    // Must be called with latch held. Uses a while loop around await() — this is
    // the standard pattern because await() can wake spuriously (OS-level behaviour),
    // so you always re-check the condition after waking up.
    private FrameId acquireFrameId() throws InterruptedException {
        while (true) {
            if (!freeFrames.isEmpty()) {
                return freeFrames.remove().getFrameId();
            }

            FrameId frameId = replacer.evict();
            if (frameId != null) {
                return frameId;
            }

            // All frames are pinned. Release the latch and sleep until
            // a frame becomes available (signalled from unpin/delete).
            frameAvailable.await();
        }
    }

    public ReadPageGuard readPage(PageId pageId) throws InterruptedException, ExecutionException {
        latch.lock();
        try {
            // Page already in buffer pool — fast path, no disk I/O needed
            Frame frame = pageTable.get(pageId);
            if (frame != null) {
                replacer.recordAccess(frame.getFrameId(), pageId);
                return ReadPageGuard.create(pageId, frame, replacer, this);
            }
        } finally {
            latch.unlock();
        }

        // Page not in pool — we need to find a frame, read from disk, then load it.
        // acquireFrameId() blocks here (without spinning) if no frame is available.
        latch.lock();
        FrameId frameId;
        try {
            frameId = acquireFrameId();
        } finally {
            latch.unlock();
        }

        // Disk I/O happens outside the latch — we don't want to hold the lock
        // while waiting on disk since that would block all other threads.
        Future<ByteBuffer> future = diskScheduler.schedulePageRead(pageId);
        ByteBuffer data = future.get();

        latch.lock();
        try {
            Frame frame = frames.get(frameId);
            frame.getData().put(data);
            pageTable.put(pageId, frame);
            replacer.recordAccess(frameId, pageId);
            return ReadPageGuard.create(pageId, frame, replacer, this);
        } finally {
            latch.unlock();
        }
    }

    public WritePageGuard writePage(PageId pageId) throws InterruptedException, ExecutionException {
        latch.lock();
        try {
            Frame frame = pageTable.get(pageId);
            if (frame != null) {
                replacer.recordAccess(frame.getFrameId(), pageId);
                return WritePageGuard.create(pageId, frame, this);
            }
        } finally {
            latch.unlock();
        }

        latch.lock();
        FrameId frameId;
        try {
            frameId = acquireFrameId();
        } finally {
            latch.unlock();
        }

        Future<ByteBuffer> future = diskScheduler.schedulePageRead(pageId);
        ByteBuffer data = future.get();

        latch.lock();
        try {
            Frame frame = frames.get(frameId);
            // FIX: same data load fix as readPage
            frame.getData().put(data);
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

            // A frame just became free — wake any threads waiting in acquireFrameId()
            frameAvailable.signalAll();

            // TODO: Deallocate this page in disk scheduler to make it available again
            return true;
        } finally {
            latch.unlock();
        }
    }

    // Called by the guards when a page is unpinned (via close()).
    // If the frame becomes evictable again, waiting threads should be notified.
    public void onPageUnpinned(PageId pageId) {
        latch.lock();
        try {
            Frame frame = pageTable.get(pageId);
            if (frame != null && frame.getPinCount() == 0) {
                frameAvailable.signalAll();
            }
        } finally {
            latch.unlock();
        }
    }
}
