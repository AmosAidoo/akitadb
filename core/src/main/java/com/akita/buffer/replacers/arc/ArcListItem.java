package com.akita.buffer.replacers.arc;

import com.akita.buffer.FrameId;
import com.akita.buffer.PageId;

class ArcListItem {
    private PageId pageId;
    private FrameId frameId;
    private ArcListItem prev = null;
    private ArcListItem next = null;
    private ArcLocation location;
    private boolean isEvictable = false;

    private ArcListItem(FrameId frameId, PageId pageId, ArcLocation location) {
        this.pageId = pageId;
        this.frameId = frameId;
        this.location = location;
    }

    public static ArcListItem create(FrameId frameId, PageId pageId, ArcLocation location) {
        return new ArcListItem(frameId, pageId, location);
    }

    public ArcListItem getPrev() {
        return prev;
    }

    public void setPrev(ArcListItem prev) {
        this.prev = prev;
    }

    public ArcListItem getNext() {
        return next;
    }

    public void setNext(ArcListItem next) {
        this.next = next;
    }

    public ArcLocation getLocation() {
        return location;
    }

    public void setLocation(ArcLocation location) {
        this.location = location;
    }

    public PageId getPageId() {
        return pageId;
    }

    public void setPageId(PageId pageId) {
        this.pageId = pageId;
    }

    public FrameId getFrameId() {
        return frameId;
    }

    public void setFrameId(FrameId frameId) {
        this.frameId = frameId;
    }

    public boolean getIsEvictable() {
        return isEvictable;
    }

    public void setIsEvictable(boolean isEvictable) {
        this.isEvictable = isEvictable;
    }

    @Override
    public String toString() {
        return "ArcListItem{" +
                "pageId=" + pageId +
                ", frameId=" + frameId +
                ", location=" + location +
                ", isEvictable=" + isEvictable +
                '}';
    }
}
