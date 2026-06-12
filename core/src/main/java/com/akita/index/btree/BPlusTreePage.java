package com.akita.index.btree;

import com.akita.buffer.guards.PageGuard;
import com.akita.buffer.guards.WritePageGuard;
import com.akita.buffer.PageId;
import com.akita.catalog.IndexMetadata;
import com.akita.page.PageHeader;
import com.akita.page.SlottedPage;
import com.akita.page.Tuple;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class BPlusTreePage extends SlottedPage implements AutoCloseable {
    public static final long NO_NEXT_LEAF = 0;

    private BTreePageType pageType;
    private long rightmostChildBlockNumber; // internal pages only
    private long nextLeafBlockNumber; // leaf pages only

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
        initializeHeader(pageGuard.getData(), BTreePageType.LEAF, NO_NEXT_LEAF);
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
            long pageTypeSpecificBlockNumber
    ) {
        data.clear();
        data.putShort(PageHeader.NUMBER_OF_SLOTS_OFFSET, (short) 0);
        data.position(PageHeader.SIZE);
        data.put(pageTypeCode(pageType));
        data.putLong(pageTypeSpecificBlockNumber);
    }

    private static byte pageTypeCode(BTreePageType pageType) {
        return switch (pageType) {
            case INTERNAL -> 1;
            case LEAF -> 2;
            case INVALID -> 0;
        };
    }

    public short insertTupleSorted(Tuple tuple, Comparator<Tuple> comparator) {
        if (!(pageGuard instanceof WritePageGuard)) {
            throw new IllegalStateException("pageGuard must be a WritePageGuard");
        }
        short slotIndex = appendTuple(tuple);
        sortSlotsByTuple(comparator);
        return slotIndex;
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

        replaceTuplesRaw(tuples);
    }

    public void replaceInternalTuples(long rightmostChildBlockNumber, List<Tuple> tuples) {
        if (!(pageGuard instanceof WritePageGuard)) {
            throw new IllegalStateException("pageGuard must be a WritePageGuard");
        }

        initializeHeader(data, BTreePageType.INTERNAL, rightmostChildBlockNumber);
        this.pageType = BTreePageType.INTERNAL;
        this.rightmostChildBlockNumber = rightmostChildBlockNumber;
        replaceTuplesRaw(tuples);
    }

    public long getNextLeafBlockNumber() {
        if (!isLeaf()) {
            throw new IllegalStateException("Only leaf pages have a next leaf pointer");
        }
        return nextLeafBlockNumber;
    }

    public void setNextLeafBlockNumber(long nextLeafBlockNumber) {
        if (!(pageGuard instanceof WritePageGuard)) {
            throw new IllegalStateException("pageGuard must be a WritePageGuard");
        }
        if (!isLeaf()) {
            throw new IllegalStateException("Only leaf pages have a next leaf pointer");
        }
        this.nextLeafBlockNumber = nextLeafBlockNumber;
        data.putLong(PageHeader.SIZE + Byte.BYTES, nextLeafBlockNumber);
    }

    public int lowerBound(Tuple searchTuple, Comparator<Tuple> cmp) {
        int l = 0, r = tupleCount();
        while (l < r) {
            int mid = l + (r - l) / 2;
            Tuple midTuple = tupleAt(mid);
            if (cmp.compare(midTuple, searchTuple) < 0) l = mid + 1;
            else r = mid;
        }
        return l;
    }

    public int upperBound(Tuple searchTuple, Comparator<Tuple> cmp) {
        int l = 0, r = tupleCount();
        while (l < r) {
            int mid = l + (r - l) / 2;
            Tuple midTuple = tupleAt(mid);
            if (cmp.compare(midTuple, searchTuple) <= 0) l = mid + 1;
            else r = mid;
        }
        return l;
    }

    public Tuple tupleAt(int slotIndex) {
        if (slotIndex < 0 || slotIndex >= tupleCount()) {
            return null;
        }
        return super.tupleAt(slotIndex);
    }

    public PageId getPageId() {
        return pageGuard.getPageId();
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
        return getFreeSpace();
    }

    @Override
    protected int headerSize() {
        return super.headerSize() + extendedHeaderSize();
    }

    private int extendedHeaderSize() {
        return Byte.BYTES + Long.BYTES;
    }

    // Extended header layout:
    // | pageType (byte) | rightmostChildBlockNumber (long, internal) OR nextLeafBlockNumber (long, leaf) |
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
        } else if (this.pageType == BTreePageType.LEAF) {
            this.nextLeafBlockNumber = data.getLong();
        }
    }

    @Override
    public void close() throws Exception {
        pageGuard.close();
    }
}
