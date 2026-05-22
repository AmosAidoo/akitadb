package com.akita.buffer;

import java.nio.ByteBuffer;
import java.util.concurrent.Future;

public interface DiskScheduler {

    Future<ByteBuffer> schedulePageRead(PageId pageId);

    Future<?> schedulePageWrite(PageId pageId, ByteBuffer buffer);

    Future<?> schedulePageAllocate(PageId pageId);
}
