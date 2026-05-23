package com.akita.storage;

import com.akita.buffer.PageId;
import com.akita.page.PageDirectory;

public class BootstrapPageAllocator implements PageAllocator {
    private long nextBlockNumber;

    private BootstrapPageAllocator(long nextBlockNumber) {
        this.nextBlockNumber = nextBlockNumber;
    }

    public static BootstrapPageAllocator create(PageDirectory pageDirectory, long minimumAllocatedBlockNumber) {
        long highestKnownBlockNumber = Math.max(
                minimumAllocatedBlockNumber,
                pageDirectory.highestKnownBlockNumber()
        );
        return new BootstrapPageAllocator(highestKnownBlockNumber + 1);
    }

    @Override
    public PageId allocate(ContainerId containerId) {
        return new PageId(containerId, nextBlockNumber++);
    }
}
