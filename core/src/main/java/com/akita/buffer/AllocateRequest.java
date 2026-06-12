package com.akita.buffer;

import com.akita.storage.Storage;

import java.io.IOException;

public class AllocateRequest implements Runnable {
    private final PageId pageId;
    private final Storage storage;

    private AllocateRequest(PageId pageId, Storage storage) {
        this.pageId = pageId;
        this.storage = storage;
    }

    public static AllocateRequest create(PageId pageId, Storage storage) {
        return new AllocateRequest(pageId, storage);
    }

    @Override
    public void run() {
        try {
            synchronized (storage) {
                storage.allocate(pageId);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Unable to allocate page: " + pageId, e);
        }
    }
}
