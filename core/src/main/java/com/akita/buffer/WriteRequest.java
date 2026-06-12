package com.akita.buffer;

import com.akita.storage.Storage;

import java.io.IOException;
import java.nio.ByteBuffer;

public class WriteRequest implements Runnable {
    private final PageId pageId;
    private final ByteBuffer buffer;
    private final Storage storage;

    private WriteRequest(PageId pageId, ByteBuffer buffer, Storage storage) {
        this.pageId = pageId;
        this.buffer = buffer;
        this.storage = storage;
    }

    public static WriteRequest create(PageId pageId, ByteBuffer buffer, Storage storage) {
        return new WriteRequest(pageId, buffer, storage);
    }

    @Override
    public void run() {
        try {
            synchronized (storage) {
                storage.write(pageId, buffer);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Unable to write page: " + pageId, e);
        }
    }
}
