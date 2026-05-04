package com.akita.heap;

import com.akita.buffer.guards.PageGuard;
import com.akita.buffer.guards.WritePageGuard;
import com.akita.page.Slot;
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

    @Override
    public Slot insertTuple(Tuple tuple) {
        if (!(pageGuard instanceof WritePageGuard)) {
            throw new IllegalStateException("pageGuard must be a WritePageGuard");
        }
        return super.insertTuple(tuple);
    }

    @Override
    public void close() throws Exception {
        pageGuard.close();
    }
}
