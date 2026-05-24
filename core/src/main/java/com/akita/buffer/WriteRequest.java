package com.akita.buffer;

import com.akita.storage.BlockManager;

import java.io.IOException;
import java.nio.ByteBuffer;

public class WriteRequest implements Runnable {
    private final PageId pageId;
    private final ByteBuffer buffer;
    private final BlockManager blockManager;

    private WriteRequest(PageId pageId, ByteBuffer buffer, BlockManager blockManager) {
        this.pageId = pageId;
        this.buffer = buffer;
        this.blockManager = blockManager;
    }

    public static WriteRequest create(PageId pageId, ByteBuffer buffer, BlockManager blockManager) {
        return new WriteRequest(pageId, buffer, blockManager);
    }

    @Override
    public void run() {
        try {
            synchronized (blockManager) {
                blockManager.writeBlock(pageId.containerId(), pageId.blockNumber(), buffer);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Unable to write page: " + pageId, e);
        }
    }
}
