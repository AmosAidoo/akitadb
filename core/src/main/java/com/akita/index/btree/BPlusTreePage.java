package com.akita.index.btree;

import com.akita.buffer.guards.PageGuard;
import com.akita.buffer.guards.WritePageGuard;
import com.akita.catalog.IndexMetadata;
import com.akita.page.PageHeader;
import com.akita.page.Slot;
import com.akita.page.SlottedPage;
import com.akita.page.Tuple;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

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

    public static BPlusTreePage initializeLeaf(WritePageGuard pageGuard, IndexMetadata indexMetadata) {
        initializeHeader(pageGuard.getData(), BTreePageType.LEAF, 0);
        return create(pageGuard, indexMetadata);
    }

    public static BPlusTreePage initializeInternal(
            WritePageGuard pageGuard,
            IndexMetadata indexMetadata,
            long rightmostChildBlockNumber
    ) {
        initializeHeader(pageGuard.getData(), BTreePageType.INTERNAL, rightmostChildBlockNumber);
        return create(pageGuard, indexMetadata);
    }

    private static void initializeHeader(
            ByteBuffer data,
            BTreePageType pageType,
            long rightmostChildBlockNumber
    ) {
        data.clear();
        data.putShort(PageHeader.NUMBER_OF_SLOTS_OFFSET, (short) 0);
        data.position(PageHeader.SIZE);
        data.put(pageTypeCode(pageType));
        if (pageType == BTreePageType.INTERNAL) {
            data.putLong(rightmostChildBlockNumber);
        }
    }

    private static byte pageTypeCode(BTreePageType pageType) {
        return switch (pageType) {
            case INTERNAL -> 1;
            case LEAF -> 2;
            case INVALID -> 0;
        };
    }

    public Slot insertTupleSorted(Tuple tuple, Comparator<Tuple> comparator) {
        if (!(pageGuard instanceof WritePageGuard)) {
            throw new IllegalStateException("pageGuard must be a WritePageGuard");
        }
        Slot newSlot = super.insertTupleRaw(tuple);
        slots.sort((s1, s2) -> {
            Tuple t1 = getTuple(s1);
            Tuple t2 = getTuple(s2);
            return comparator.compare(t1, t2);
        });
        // Serialize sorted slots
        for (int i = 0; i < slots.size(); i++) {
            Slot slot = slots.get(i);
            data.put(slotDirectoryOffset(i), slot.getBytes());
        }
        return newSlot;
    }

    public List<Tuple> tuples() {
        List<Tuple> tuples = new ArrayList<>(tupleCount());
        for (int i = 0; i < tupleCount(); i++) {
            tuples.add(tupleAt(i));
        }
        return tuples;
    }

    /**
     * Replaces tuples in page with new tuples.
     * Assumes the caller provides tuples in the desired logical order
     * @param tuples New list of tuples to replace current ones
     */
    public void replaceTuples(List<Tuple> tuples) {
        if (!(pageGuard instanceof WritePageGuard)) {
            throw new IllegalStateException("pageGuard must be a WritePageGuard");
        }

        clearTuples();
        for (Tuple tuple : tuples) {
            super.insertTupleRaw(tuple);
        }
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
        return super.getTupleBySlotIndex(slotIndex);
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

    public int getRemainingSpace() {
        return lowestTupleOffset() - (headerSize() + (Slot.SERIALIZED_SIZE * slots.size()));
    }

    @Override
    protected int headerSize() {
        return super.headerSize() + extendedHeaderSize();
    }

    private int extendedHeaderSize() {
        int extendedHeaderSize = Byte.BYTES;
        if (pageType == BTreePageType.INTERNAL) {
            extendedHeaderSize += Long.BYTES;
        }
        return extendedHeaderSize;
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
