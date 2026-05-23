package com.akita.buffer;

import com.akita.storage.BlockManager;

import java.io.IOException;

public class AllocateRequest implements Runnable {
    private final PageId pageId;
    private final BlockManager blockManager;

    private AllocateRequest(PageId pageId, BlockManager blockManager) {
        this.pageId = pageId;
        this.blockManager = blockManager;
    }

    public static AllocateRequest create(PageId pageId, BlockManager blockManager) {
        return new AllocateRequest(pageId, blockManager);
    }

    @Override
    public void run() {
        try {
            synchronized (blockManager) {
                blockManager.allocateBlock(pageId.containerId(), pageId.blockNumber());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
