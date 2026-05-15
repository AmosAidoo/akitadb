package com.akita.index.btree;

import com.akita.buffer.BufferPoolManager;
import com.akita.buffer.PageId;
import com.akita.catalog.IndexMetadata;
import com.akita.page.RecordId;
import com.akita.page.Tuple;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class BPlusTree {
    private static final long ROOT_BLOCK_NUMBER = 1;
    private static final int MAX_KEY_BYTES = 512;

    private final IndexMetadata indexMetadata;
    private final BufferPoolManager bufferPoolManager;

    private BPlusTree(IndexMetadata indexMetadata, BufferPoolManager bufferPoolManager) {
        this.indexMetadata = indexMetadata;
        this.bufferPoolManager = bufferPoolManager;
    }

    public static BPlusTree create(IndexMetadata indexMetadata, BufferPoolManager bufferPoolManager) {
        return new BPlusTree(indexMetadata, bufferPoolManager);
    }

    public RecordId find(LeafBTreeKey fullKey) throws Exception {
        try (BPlusTreePage leaf = findLeafPage(fullKey)) {
            Tuple result = findExactOnLeaf(leaf, fullKey);
            return result == null ? null : LeafBTreeKey.ofLeaf(result, indexMetadata).rid();
        }
    }

    public List<RecordId> scanEquals(BTreeKey logicalKey) throws Exception {
        LeafBTreeKey lower = LeafBTreeKey.withMinRid(logicalKey.columns());
        LeafBTreeKey upper = LeafBTreeKey.withMaxRid(logicalKey.columns());
        return scanRange(lower, upper);
    }

    public List<RecordId> scanRange(LeafBTreeKey lower, LeafBTreeKey upper) throws Exception {
        List<RecordId> results = new ArrayList<>();

        try (BPlusTreePage leaf = findLeafPage(lower)) {
            Comparator<Tuple> leafCmp = leafComparator();
            Tuple lowerTuple = TupleSerializer.serializeLeaf(lower, indexMetadata);
            Tuple upperTuple = TupleSerializer.serializeLeaf(upper, indexMetadata);

            int startPos = leaf.lowerBound(lowerTuple, leafCmp);

            for (int i = startPos; i < leaf.tupleCount(); i++) {
                Tuple tuple = leaf.tupleAt(i);
                if (leafCmp.compare(tuple, upperTuple) > 0) break;
                results.add(LeafBTreeKey.ofLeaf(tuple, indexMetadata).rid());
            }
        }

        // TODO: follow sibling pointers across leaf pages once next-leaf
        // pointer is added to the leaf page header

        return results;
    }

    private BPlusTreePage findLeafPage(LeafBTreeKey searchKey) throws Exception {
        PageId currentPageId = new PageId(
                indexMetadata.containerId(),
                ROOT_BLOCK_NUMBER
        );

        while (true) {
            BPlusTreePage page = BPlusTreePage.create(
                    bufferPoolManager.readPage(currentPageId),
                    indexMetadata
            );

            if (page.isLeaf()) {
                return page; // caller is responsible for closing
            }

            PageId childPageId;
            try {
                childPageId = findChildPageId(page, searchKey);
            } finally {
                page.close(); // release read latch before descending
            }

            currentPageId = childPageId;
        }
    }

    // Routes through an internal page using upperBound on column-only comparison.
    // upperBound gives us the first separator strictly greater than the search key,
    // which is the correct child to descend into.
    private PageId findChildPageId(BPlusTreePage page, LeafBTreeKey searchKey) throws Exception {
        Comparator<Tuple> internalCmp = internalComparator();
        Tuple searchTuple = TupleSerializer.serializeLeaf(searchKey, indexMetadata);

        int pos = page.upperBound(searchTuple, internalCmp);

        if (pos == page.tupleCount()) {
            // Beyond all separators — take the rightmost child
            return new PageId(
                    indexMetadata.containerId(),
                    page.getRightmostChildBlockNumber()
            );
        }

        Tuple tuple = page.tupleAt(pos);
        long leftChildBlockNumber = tuple.readLong(); // first field of internal tuple
        return new PageId(indexMetadata.containerId(), leftChildBlockNumber);
    }

    private Tuple findExactOnLeaf(BPlusTreePage leaf, LeafBTreeKey fullKey) {
        Comparator<Tuple> leafCmp = leafComparator();
        Tuple searchTuple = TupleSerializer.serializeLeaf(fullKey, indexMetadata);

        int pos = leaf.lowerBound(searchTuple, leafCmp);

        if (pos >= leaf.tupleCount()) return null;

        Tuple candidate = leaf.tupleAt(pos);
        return leafCmp.compare(candidate, searchTuple) == 0 ? candidate : null;
    }

    private Comparator<Tuple> internalComparator() {
        return (t1, t2) -> {
            BTreeKey k1 = BTreeKey.ofInternal(t1, indexMetadata);
            BTreeKey k2 = BTreeKey.ofInternal(t2, indexMetadata);
            return k1.compareColumnsTo(k2);
        };
    }

    private Comparator<Tuple> leafComparator() {
        return (t1, t2) -> {
            LeafBTreeKey k1 = LeafBTreeKey.ofLeaf(t1, indexMetadata);
            LeafBTreeKey k2 = LeafBTreeKey.ofLeaf(t2, indexMetadata);
            return k1.compareTo(k2);
        };
    }

    private void validateKeySize(LeafBTreeKey key) {
        int size = TupleSerializer.computeLeafSize(key, indexMetadata);
        if (size > MAX_KEY_BYTES) {
            throw new IllegalArgumentException(
                    "Index key size " + size + " bytes exceeds maximum of " + MAX_KEY_BYTES + " bytes"
            );
        }
    }
}