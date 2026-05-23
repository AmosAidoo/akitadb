package com.akita.storage;

import com.akita.buffer.PageId;
import com.akita.page.PageDirectory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BootstrapPageAllocatorTest {

    @Test
    void startsAfterMinimumAllocatedBlockNumber() {
        ContainerId containerId = ContainerId.generate();
        PageDirectory pageDirectory = PageDirectory.create(containerId);

        BootstrapPageAllocator allocator = BootstrapPageAllocator.create(pageDirectory, 1);

        assertThat(allocator.allocate(containerId)).isEqualTo(new PageId(containerId, 2));
        assertThat(allocator.allocate(containerId)).isEqualTo(new PageId(containerId, 3));
    }
}
