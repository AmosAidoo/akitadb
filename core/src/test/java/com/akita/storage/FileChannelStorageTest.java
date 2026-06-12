package com.akita.storage;

import com.akita.buffer.PageId;
import com.akita.testing.AkitaExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(AkitaExtension.class)
class FileChannelStorageTest {
    @Test
    void createContainerCreatesEmptyContainerFile(FileChannelStorage storage) throws Exception {
        ContainerId containerId = storage.createContainer();

        assertThat(storage.size(containerId)).isZero();
    }

    @Test
    void allocateCreatesExactlyOnePage(FileChannelStorage storage) throws Exception {
        ContainerId containerId = storage.createContainer();

        storage.allocate(new PageId(containerId, 0));

        assertThat(storage.size(containerId)).isEqualTo(Storage.PAGE_SIZE);
    }

    @Test
    void allocateGrowsToRequestedPage(FileChannelStorage storage) throws Exception {
        ContainerId containerId = storage.createContainer();

        storage.allocate(new PageId(containerId, 9));

        assertThat(storage.size(containerId)).isEqualTo(10L * Storage.PAGE_SIZE);
    }

    @Test
    void writeImplicitlyAllocatesMissingPages(FileChannelStorage storage) throws Exception {
        ContainerId containerId = storage.createContainer();
        PageId pageId = new PageId(containerId, 3);
        ByteBuffer page = ByteBuffer.allocate(Storage.PAGE_SIZE);
        page.putInt(42);

        storage.write(pageId, page);

        assertThat(storage.size(containerId)).isEqualTo(4L * Storage.PAGE_SIZE);
        assertThat(storage.read(pageId).getInt()).isEqualTo(42);
    }

    @Test
    void readReturnsFullPagePositionedForReading(FileChannelStorage storage) throws Exception {
        ContainerId containerId = storage.createContainer();
        PageId pageId = new PageId(containerId, 0);
        ByteBuffer page = ByteBuffer.allocate(Storage.PAGE_SIZE);
        page.putInt(1234);
        storage.write(pageId, page);

        ByteBuffer read = storage.read(pageId);

        assertThat(read.position()).isZero();
        assertThat(read.limit()).isEqualTo(Storage.PAGE_SIZE);
        assertThat(read.capacity()).isEqualTo(Storage.PAGE_SIZE);
        assertThat(read.getInt()).isEqualTo(1234);
    }

    @Test
    void writeRejectsInvalidBufferSize(FileChannelStorage storage) throws Exception {
        ContainerId containerId = storage.createContainer();

        assertThatThrownBy(() -> storage.write(new PageId(containerId, 0), ByteBuffer.allocate(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("PAGE_SIZE");
    }
}
