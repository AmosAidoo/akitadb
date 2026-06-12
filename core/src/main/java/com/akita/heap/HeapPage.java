package com.akita.heap;

import com.akita.buffer.guards.PageGuard;
import com.akita.buffer.guards.WritePageGuard;
import com.akita.page.SlottedPage;
import com.akita.page.Tuple;

import java.nio.ByteBuffer;

public class HeapPage extends SlottedPage implements AutoCloseable {
    private final PageGuard pageGuard;

    private HeapPage(PageGuard pageGuard) {
        this.pageGuard = pageGuard;
    }

    public static HeapPage create(PageGuard pageGuard) {
        HeapPage page = new HeapPage(pageGuard);
        ByteBuffer data = pageGuard.getData();
        page.parsePage(data);
        return page;
    }

    public Tuple getTuple(short slotIndex) {
        return tupleAt(slotIndex);
    }

    /**
     * Inserts a new tuple
     * @param tuple The tuple to be inserted
     */
    public short insertTuple(Tuple tuple) {
        if (!(pageGuard instanceof WritePageGuard)) {
            throw new IllegalStateException("pageGuard must be a WritePageGuard");
        }
        return appendTuple(tuple);
    }

    @Override
    public void close() throws Exception {
        pageGuard.close();
    }
}
