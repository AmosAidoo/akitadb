package com.akita.heap;

import com.akita.buffer.guards.PageGuard;
import com.akita.buffer.guards.WritePageGuard;
import com.akita.page.PageHeader;
import com.akita.page.Slot;
import com.akita.page.SlottedPage;
import com.akita.page.Tuple;
import com.akita.storage.Storage;

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

    @Override
    public Tuple getTuple(short offset) {
        return super.getTuple(offset);
    }

    /**
     * Inserts a new tuple
     * @param tuple The tuple to be inserted
     */
    public Slot insertTuple(Tuple tuple) {
        if (!(pageGuard instanceof WritePageGuard)) {
            throw new IllegalStateException("pageGuard must be a WritePageGuard");
        }
        return super.insertTupleRaw(tuple);
    }

    @Override
    public void close() throws Exception {
        pageGuard.close();
    }
}
