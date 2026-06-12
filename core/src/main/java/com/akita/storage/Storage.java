package com.akita.storage;

import com.akita.buffer.PageId;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface Storage {
    int PAGE_SIZE = 8192;

    ContainerId createContainer() throws IOException;

    void allocate(PageId pageId) throws IOException;

    ByteBuffer read(PageId pageId) throws IOException;

    void write(PageId pageId, ByteBuffer page) throws IOException;

    long size(ContainerId containerId) throws IOException;
}
