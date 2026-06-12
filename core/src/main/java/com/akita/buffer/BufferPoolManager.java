package com.akita.buffer;

import com.akita.buffer.guards.ReadPageGuard;
import com.akita.buffer.guards.WritePageGuard;
import com.akita.buffer.replacers.Replacer;
import com.akita.storage.Storage;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * BufferPoolManager is responsible for fetching blocks from the storage
 * device with the {@link DiskScheduler}
 */
public class BufferPoolManager {
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

    private record FrameReservation(FrameId frameId, boolean pinnedForReuse) {}

    @FunctionalInterface
    private interface GuardFactory<T> {
        T create(PageId pageId, Frame frame);
    }

    private void loadPageIntoFrame(Frame frame, PageId pageId, ByteBuffer data) {
        frame.getData().put(data);
        frame.setPageId(pageId);
        frame.setIsDirty(false);
        pageTable.put(pageId, frame);
        replacer.recordAccess(frame.getFrameId(), pageId);
    }

    private ByteBuffer snapshot(Frame frame) {
        ByteBuffer source = frame.getData();
        ByteBuffer copy = ByteBuffer.allocate(Storage.PAGE_SIZE);
        copy.put(source);
        copy.clear();
        return copy;
    }

    private record PageSnapshot(PageId pageId, ByteBuffer data, long dirtyVersion) {}

    private PageSnapshot snapshotDirtyPage(Frame frame) throws ExecutionException, InterruptedException {
        frame.getWriteLatch().lock();
        try {
            Future<?> pendingWrite = frame.getPendingWrite();
            if (pendingWrite != null) {
                pendingWrite.get();
                frame.setPendingWrite(null);
            }

            if (!frame.getIsDirty()) {
                return null;
            }

            return new PageSnapshot(frame.getPageId(), snapshot(frame), frame.getDirtyVersion());
        } finally {
            frame.getWriteLatch().unlock();
        }
    }

    private void markSnapshotFlushed(Frame frame, long dirtyVersion) {
        frame.getWriteLatch().lock();
        try {
            frame.markClean(dirtyVersion);
            frame.setPendingWrite(null);
        } finally {
            frame.getWriteLatch().unlock();
        }
    }

    private void flushSnapshot(Frame frame, PageSnapshot snapshot) throws ExecutionException, InterruptedException {
        if (snapshot == null) {
            return;
        }

        Future<?> writeFuture = diskScheduler.schedulePageWrite(snapshot.pageId(), snapshot.data());
        frame.getWriteLatch().lock();
        try {
            frame.setPendingWrite(writeFuture);
        } finally {
            frame.getWriteLatch().unlock();
        }
        writeFuture.get();
        markSnapshotFlushed(frame, snapshot.dirtyVersion());
    }

    private void prepareFrameForReuse(Frame frame) throws ExecutionException, InterruptedException {
        PageId oldPageId = frame.getPageId();
        if (oldPageId == null) {
            return;
        }

        PageSnapshot snapshot = snapshotDirtyPage(frame);
        flushSnapshot(frame, snapshot);

        frame.getWriteLatch().lock();
        try {
            Future<?> pendingWrite = frame.getPendingWrite();
            if (pendingWrite != null) {
                pendingWrite.get();
                frame.setPendingWrite(null);
            }

            pageTable.remove(oldPageId);
            frame.setPageId(null);
        } finally {
            frame.getWriteLatch().unlock();
        }
    }

    // Extracted helper: finds a free frameId or waits until one becomes available.
    // Must be called with latch held. Uses a while loop around await() — this is
    // the standard pattern because await() can wake spuriously (OS-level behaviour),
    // so you always re-check the condition after waking up.
    private FrameReservation acquireFrameId() throws InterruptedException {
        while (true) {
            if (!freeFrames.isEmpty()) {
                return new FrameReservation(freeFrames.remove().getFrameId(), false);
            }

            FrameId frameId = replacer.evict();
            if (frameId != null) {
                frames.get(frameId).pin();
                PageId oldPageId = frames.get(frameId).getPageId();
                if (oldPageId != null) {
                    pageTable.remove(oldPageId);
                }
                return new FrameReservation(frameId, true);
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

        return loadPageFromDisk(pageId, (loadedPageId, frame) -> ReadPageGuard.create(loadedPageId, frame, replacer, this));
    }

    public WritePageGuard writePage(PageId pageId) throws InterruptedException, ExecutionException {
        latch.lock();
        try {
            Frame frame = pageTable.get(pageId);
            if (frame != null) {
                replacer.recordAccess(frame.getFrameId(), pageId);
                return WritePageGuard.create(pageId, frame, replacer, this);
            }
        } finally {
            latch.unlock();
        }

        return loadPageFromDisk(pageId, (loadedPageId, frame) -> WritePageGuard.create(loadedPageId, frame, replacer, this));
    }

    private <T> T loadPageFromDisk(PageId pageId, GuardFactory<T> guardFactory) throws InterruptedException, ExecutionException {
        // Page not in pool — we need to find a frame, read from disk, then load it.
        // acquireFrameId() blocks here (without spinning) if no frame is available.
        latch.lock();
        FrameReservation reservation;
        try {
            reservation = acquireFrameId();
        } finally {
            latch.unlock();
        }

        // Disk I/O happens outside the latch — we don't want to hold the lock
        // while waiting on disk since that would block all other threads.
        Frame frame = frames.get(reservation.frameId());
        prepareFrameForReuse(frame);

        Future<ByteBuffer> future = diskScheduler.schedulePageRead(pageId);
        ByteBuffer data = future.get();

        latch.lock();
        try {
            loadPageIntoFrame(frame, pageId, data);
            T guard = guardFactory.create(pageId, frame);
            if (reservation.pinnedForReuse()) {
                frame.unpin();
            }
            return guard;
        } finally {
            latch.unlock();
        }
    }

    public WritePageGuard allocatePage(PageId pageId) throws InterruptedException, ExecutionException {
        latch.lock();
        try {
            if (pageTable.containsKey(pageId)) {
                throw new IllegalStateException("Page already exists in buffer pool: " + pageId);
            }
        } finally {
            latch.unlock();
        }

        latch.lock();
        FrameReservation reservation;
        try {
            reservation = acquireFrameId();
        } finally {
            latch.unlock();
        }

        diskScheduler.schedulePageAllocate(pageId).get();

        Frame frame = frames.get(reservation.frameId());
        prepareFrameForReuse(frame);

        ByteBuffer data = ByteBuffer.allocate(Storage.PAGE_SIZE);

        return getWritePageGuard(pageId, reservation, frame, data);
    }

    private WritePageGuard getWritePageGuard(PageId pageId, FrameReservation reservation, Frame frame, ByteBuffer data) {
        latch.lock();
        try {
            loadPageIntoFrame(frame, pageId, data);
            WritePageGuard guard = WritePageGuard.create(pageId, frame, replacer, this);
            if (reservation.pinnedForReuse()) {
                frame.unpin();
            }
            return guard;
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
        PageSnapshot snapshot;
        try {
            Future<?> pendingWrite = frame.getPendingWrite();
            if (pendingWrite != null) {
                pendingWrite.get();
                frame.setPendingWrite(null);
            }

            if (!frame.getIsDirty()) {
                return true;
            }
            snapshot = new PageSnapshot(pageId, snapshot(frame), frame.getDirtyVersion());
        } catch (ExecutionException | InterruptedException e) {
            throw new IllegalStateException("Unable to flush page: " + pageId, e);
        } finally {
            frame.getWriteLatch().unlock();
        }

        try {
            flushSnapshot(frame, snapshot);
        } catch (ExecutionException | InterruptedException e) {
            throw new IllegalStateException("Unable to flush page: " + pageId, e);
        }
        return true;
    }

    public void flushAllPages() {
        PageId[] pageIds;
        latch.lock();
        try {
            pageIds = pageTable.keySet().toArray(PageId[]::new);
        } finally {
            latch.unlock();
        }

        for (PageId pageId : pageIds) {
            flushPage(pageId);
        }
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
            frame.setPageId(null);
            frame.setIsDirty(false);

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
