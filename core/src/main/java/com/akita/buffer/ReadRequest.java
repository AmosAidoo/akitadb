package com.akita.buffer;

import com.akita.storage.BlockManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

/**
 * ReadRequest is a task that represents a single read request
 * from the {@link BlockManager}
 */
public class ReadRequest implements Callable<ByteBuffer> {
    private final PageId pageId;
    private final BlockManager blockManager;

    private ReadRequest(PageId pageId, BlockManager blockManager) {
        this.pageId = pageId;
        this.blockManager = blockManager;
    }

    public static ReadRequest create(PageId pageId, BlockManager blockManager) {
        return new ReadRequest(pageId, blockManager);
    }

    @Override
    public ByteBuffer call() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(BlockManager.BLOCK_SIZE);
        synchronized (blockManager) {
            blockManager.readBlock(pageId.containerId(), pageId.blockNumber(), buffer);
        }
        buffer.clear();
        return buffer;
    }
}
