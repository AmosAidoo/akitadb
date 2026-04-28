package com.akita.buffer.replacers.arc;

import com.akita.buffer.FrameId;
import com.akita.buffer.PageId;
import com.akita.buffer.replacers.Replacer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class ArcReplacer implements Replacer {
    private final int capacity;

    private final ReentrantLock latch = new ReentrantLock();
    private int currentEvictableSize;
    private int t1Target;
    private final ArcList t1 = ArcList.create();
    private final ArcList t2 = ArcList.create();
    private final ArcList b1 = ArcList.create();
    private final ArcList b2 = ArcList.create();
    private final Map<PageId, ArcListItem> pageTable;
    private final Map<FrameId, ArcListItem> frameTable;

    private ArcReplacer(int capacity) {
        this.capacity = capacity;
        pageTable = new HashMap<>();
        frameTable = new HashMap<>();
    }

    public static ArcReplacer create(int capacity) {
        return new ArcReplacer(capacity);
    }

    @Override
    public int size() {
        return currentEvictableSize;
    }

    @Override
    public void setEvictable(FrameId frameId, boolean evictable) {
        latch.lock();
        try {
            ArcListItem item = frameTable.get(frameId);
            if (item == null) {
                throw new IllegalStateException("Frame should be available anytime setEvictable is called");
            }
            if (item.getIsEvictable() && !evictable) {
                item.setIsEvictable(false);
                currentEvictableSize--;
            } else if (!item.getIsEvictable() && evictable) {
                item.setIsEvictable(true);
                currentEvictableSize++;
            }
            // Ignore every other case
        } finally {
            latch.unlock();
        }
    }

    private boolean shouldEvictFromT1() {
        return t1.size() >= t1Target;
    }

    private FrameId replaceFromListUnsafe(ArcList victimList) {
        ArcLocation ghostLocation = (victimList == t1) ? ArcLocation.B1 : ArcLocation.B2;
        ArcList ghostList = (victimList == t1) ? b1 : b2;

        ArcListItem victim = victimList.lruPeek();
        if (victim == null) {
            return null;
        }

        FrameId frameId = victim.getFrameId();
        victimList.lruRemove();
        victim.setFrameId(null);
        victim.setIsEvictable(false);
        frameTable.remove(frameId);
        currentEvictableSize--;

        victim.setLocation(ghostLocation);
        ghostList.mruInsert(victim);

        if (t1.size() + t2.size() > capacity)
            throw new IllegalStateException("t1 and t2 cannot be greater than capacity");

        return frameId;
    }

    private FrameId replaceUnsafe() {
        if (shouldEvictFromT1()) {
            FrameId victim = replaceFromListUnsafe(t1);
            if (victim != null)
                return victim;
            return replaceFromListUnsafe(t2);
        } else {
            FrameId victim = replaceFromListUnsafe(t2);
            if (victim != null)
                return victim;
            return replaceFromListUnsafe(t1);
        }
    }

    @Override
    public void recordAccess(FrameId frameId, PageId pageId) {
        latch.lock();
        try {
            ArcListItem item = pageTable.get(pageId);
            if (item != null) {
                ArcLocation location = item.getLocation();
                switch (location) {
                    case T1:
                        t1.removeFromList(item);
                        item.setLocation(ArcLocation.T2);
                        t2.mruInsert(item);
                        break;
                    case T2:
                        t2.removeFromList(item);
                        item.setLocation(ArcLocation.T2);
                        t2.mruInsert(item);
                        break;
                    case B1, B2:
                        if (location == ArcLocation.B1) {
                            t1Target = Math.min(t1Target + Math.max(b2.size() / b1.size(), 1), capacity);
                            b1.removeFromList(item);
                        } else {
                            t1Target = Math.max(t1Target - Math.max(b1.size() / b2.size(), 1), 0);
                            b2.removeFromList(item);
                        }
                        item.setFrameId(frameId);
                        item.setPageId(pageId);
                        t2.mruInsert(item);
                        item.setLocation(ArcLocation.T2);
                        frameTable.put(frameId, item);
                        break;
                }
            } else {
                if (t1.size() + b1.size() == capacity) {
                    // Not b1.lruRemove because evict sets isEvictable to false
                    // and lruRemove only removes when isEvictable is true
                    ArcListItem b1Tail = b1.removeTail();
                    pageTable.remove(b1Tail.getPageId());
                } else if (t1.size() + b1.size() + t2.size() + b2.size() == 2 * capacity) {
                    ArcListItem b2Tail = b2.removeTail();
                    pageTable.remove(b2Tail.getPageId());
                }
                ArcListItem newItem = ArcListItem.create(frameId, pageId, ArcLocation.T1);
                pageTable.put(pageId, newItem);
                frameTable.put(frameId, newItem);
                t1.mruInsert(newItem);

                // This invariant should hold after the above operations
                if (t1.size() + b1.size() > capacity) {
                    throw new IllegalStateException("t1 and b1 total size cannot be greater than capacity");
                }
            }
        }  finally {
            latch.unlock();
        }
    }

    @Override
    public FrameId evict() {
        latch.lock();
        try {
            return replaceUnsafe();
        } finally {
            latch.unlock();
        }
    }

    @Override
    public void remove(FrameId frameId) {
        latch.lock();
        try {
            ArcListItem item = frameTable.get(frameId);

            if (item == null)
                throw new IllegalStateException("Cannot remove a frame that doesn't exist");
            if (!item.getIsEvictable())
                throw new IllegalStateException("Cannot remove a frame that is not evictable");

            frameTable.remove(frameId);
            pageTable.remove(item.getPageId());
            switch (item.getLocation()) {
                case T1:
                    t1.removeFromList(item);
                    // Deliberate fall through
                case T2:
                    t2.removeFromList(item);
                    item.setIsEvictable(false);
                    currentEvictableSize--;
                    break;
                case B1:
                    b1.removeFromList(item);
                    break;
                case B2:
                    b2.removeFromList(item);
                    break;
            }
        } finally {
            latch.unlock();
        }
    }

    @Override
    public String toString() {
        return "t1: " +
                t1 +
                '\n' +
                "t2: " +
                t2 +
                '\n' +
                "b1: " +
                b1 +
                '\n' +
                "b2: " +
                b2 +
                '\n';
    }
}
