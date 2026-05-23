package com.akita.page;

import com.akita.buffer.PageId;

public record RecordId(PageId pageId, short slotOffset) implements Comparable<RecordId> {
    @Override
    public int compareTo(RecordId o) {
        int cmp = pageId.compareTo(o.pageId);
        return cmp != 0 ? cmp : Short.compare(slotOffset, o.slotOffset);
    }
}