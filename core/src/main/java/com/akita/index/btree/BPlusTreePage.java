package com.akita.index.btree;

import com.akita.buffer.guards.PageGuard;
import com.akita.catalog.IndexMetadata;
import com.akita.page.SlottedPage;
import com.akita.page.Tuple;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class BPlusTreePage extends SlottedPage implements AutoCloseable {
    private BTreePageType pageType;
    private long rightmostChildBlockNumber; // internal pages only

    private final PageGuard pageGuard;
    private final IndexMetadata indexMetadata;

    private BPlusTreePage(PageGuard pageGuard, IndexMetadata indexMetadata) {
        this.pageGuard = pageGuard;
        this.indexMetadata = indexMetadata;
    }

    public static BPlusTreePage create(PageGuard pageGuard, IndexMetadata indexMetadata) {
        BPlusTreePage page = new BPlusTreePage(pageGuard, indexMetadata);
        page.parsePage(pageGuard.getData());
        return page;
    }

    public int lowerBound(Tuple searchTuple, Comparator<Tuple> cmp) {
        int l = 0, r = slots.size();
        while (l < r) {
            int mid = l + (r - l) / 2;
            Tuple midTuple = tupleAt(mid);
            if (cmp.compare(midTuple, searchTuple) < 0) l = mid + 1;
            else r = mid;
        }
        return l;
    }

    public int upperBound(Tuple searchTuple, Comparator<Tuple> cmp) {
        int l = 0, r = slots.size();
        while (l < r) {
            int mid = l + (r - l) / 2;
            Tuple midTuple = tupleAt(mid);
            if (cmp.compare(midTuple, searchTuple) <= 0) l = mid + 1;
            else r = mid;
        }
        return l;
    }

    public Tuple tupleAt(int slotIndex) {
        if (slotIndex < 0 || slotIndex >= slots.size()) {
            return null;
        }
        return super.getTuple(slots.get(slotIndex));
    }

    public int tupleCount() {
        return slots.size();
    }

    public boolean isLeaf() {
        return pageType == BTreePageType.LEAF;
    }

    public boolean isInternal() {
        return pageType == BTreePageType.INTERNAL;
    }

    public long getRightmostChildBlockNumber() {
        if (!isInternal()) {
            throw new IllegalStateException("Only internal pages have a rightmost child pointer");
        }
        return rightmostChildBlockNumber;
    }

    // Extended header layout: | pageType (byte) | rightmostChildBlockNumber (long, internal only) |
    @Override
    protected void parseExtendedHeader(ByteBuffer data) {
        byte pageTypeByte = data.get();
        this.pageType = switch (pageTypeByte) {
            case 1  -> BTreePageType.INTERNAL;
            case 2  -> BTreePageType.LEAF;
            default -> BTreePageType.INVALID;
        };

        if (this.pageType == BTreePageType.INTERNAL) {
            this.rightmostChildBlockNumber = data.getLong();
        }
    }

    @Override
    public void close() throws Exception {
        pageGuard.close();
    }
}