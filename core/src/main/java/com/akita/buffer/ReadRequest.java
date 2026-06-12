package com.akita.buffer;

import com.akita.storage.Storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

/**
 * ReadRequest is a task that represents a single read request
 * from {@link Storage}
 */
public class ReadRequest implements Callable<ByteBuffer> {
    private final PageId pageId;
    private final Storage storage;

    private ReadRequest(PageId pageId, Storage storage) {
        this.pageId = pageId;
        this.storage = storage;
    }

    public static ReadRequest create(PageId pageId, Storage storage) {
        return new ReadRequest(pageId, storage);
    }

    @Override
    public ByteBuffer call() throws IOException {
        synchronized (storage) {
            return storage.read(pageId);
        }
    }
}
