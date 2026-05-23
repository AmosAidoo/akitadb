package com.akita.storage;

import com.akita.buffer.PageId;

public interface PageAllocator {
    PageId allocate(ContainerId containerId);
}
