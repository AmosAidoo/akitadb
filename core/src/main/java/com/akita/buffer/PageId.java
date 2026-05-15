package com.akita.buffer;

import com.akita.storage.ContainerId;

public record PageId(ContainerId containerId, long blockNumber) implements Comparable<PageId> {
    @Override
    public int compareTo(PageId o) {
        int cmp = containerId.compareTo(o.containerId);
        if (cmp != 0) {
            return cmp;
        }
        return Long.compare(blockNumber, o.blockNumber);
    }
}
