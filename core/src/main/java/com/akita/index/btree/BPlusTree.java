package com.akita.index.btree;

import com.akita.buffer.BufferPoolManager;
import com.akita.buffer.PageId;
import com.akita.catalog.IndexMetadata;
import com.akita.page.PageDirectory;
import com.akita.page.RecordId;
import com.akita.page.Tuple;
import com.akita.storage.BootstrapPageAllocator;
import com.akita.storage.ContainerId;
import com.akita.storage.PageAllocator;

import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class BPlusTree {
    private static final long ROOT_BLOCK_NUMBER = 1;
    private static final int MAX_KEY_BYTES = 512;

    private final IndexMetadata indexMetadata;
    private final BufferPoolManager bufferPoolManager;
    private final PageAllocator pageAllocator;
    final PageDirectory pageDirectory;

    private BPlusTree(
            IndexMetadata indexMetadata,
            BufferPoolManager bufferPoolManager,
            PageAllocator pageAllocator,
            PageDirectory pageDirectory
    ) {
        this.indexMetadata = indexMetadata;
        this.bufferPoolManager = bufferPoolManager;
        this.pageAllocator = pageAllocator;
        this.pageDirectory = pageDirectory;
    }

    public static BPlusTree createNew(IndexMetadata indexMetadata, BufferPoolManager bufferPoolManager) throws Exception {
        PageId rootPageId = new PageId(indexMetadata.containerId(), ROOT_BLOCK_NUMBER);
        try (BPlusTreePage ignored = BPlusTreePage.initializeLeaf(
                bufferPoolManager.allocatePage(rootPageId),
                indexMetadata
        )) {
            // Root page is initialized by the BPlusTreePage factory.
        }
        return open(indexMetadata, bufferPoolManager);
    }

    public static BPlusTree open(IndexMetadata indexMetadata, BufferPoolManager bufferPoolManager) throws ExecutionException, InterruptedException {
        ContainerId containerId = indexMetadata.containerId();
        PageDirectory pageDirectory = PageDirectory.load(
                containerId,
                bufferPoolManager
        );
        return new BPlusTree(
                indexMetadata,
                bufferPoolManager,
                BootstrapPageAllocator.create(pageDirectory, ROOT_BLOCK_NUMBER),
                pageDirectory
        );
    }

    IndexMetadata indexMetadata() {
        return indexMetadata;
    }

    BufferPoolManager bufferPoolManager() {
        return bufferPoolManager;
    }

    public void insert(LeafBTreeKey fullKey) throws Exception {
        int size = TupleSerializer.computeLeafSize(fullKey, indexMetadata);
        if (size > MAX_KEY_BYTES) {
            throw new IllegalArgumentException(
                    "Index key size " + size + " bytes exceeds maximum of " + MAX_KEY_BYTES + " bytes"
            );
        }
        LeafDescent leafDescent = findLeafPageForWriteWithParents(fullKey);
        try (BPlusTreePage leaf = leafDescent.leaf()) {
            if (size <= leaf.getRemainingSpace()) {
                leaf.insertTupleSorted(
                        TupleSerializer.serializeLeaf(fullKey, indexMetadata),
                        leafComparator()
                );
            } else {
                if (leaf.isLeaf() && isRootPage(leaf)) {
                    splitRootLeaf(fullKey, leaf);
                } else {
                    splitNonRootLeaf(fullKey, leaf, leafDescent.parentPageIds());
                }
            }
        }
    }

    private void splitNonRootLeaf(
            LeafBTreeKey fullKey,
            BPlusTreePage leaf,
            Deque<PageId> parentPageIds
    ) throws Exception {
        LeafSplit split = splitLeafTuples(fullKey, leaf);
        PageId rightPageId = allocatePageId();
        long oldNextLeafBlockNumber = leaf.getNextLeafBlockNumber();

        leaf.replaceTuples(split.leftTuples());
        leaf.setNextLeafBlockNumber(rightPageId.blockNumber());
        try (BPlusTreePage rightLeafPage = BPlusTreePage.initializeLeaf(
                bufferPoolManager.allocatePage(rightPageId),
                indexMetadata
        )) {
            rightLeafPage.replaceTuples(split.rightTuples());
            rightLeafPage.setNextLeafBlockNumber(oldNextLeafBlockNumber);
        }

        insertIntoParent(
                leaf.getPageId(),
                split.separatorKey(),
                rightPageId,
                parentPageIds
        );
    }

    private void splitRootLeaf(LeafBTreeKey fullKey, BPlusTreePage rootLeaf) throws Exception {
        LeafSplit split = splitLeafTuples(fullKey, rootLeaf);

        PageId leftPageId = allocatePageId();
        PageId rightPageId = allocatePageId();
        try (BPlusTreePage leftLeafPage = BPlusTreePage.initializeLeaf(
                bufferPoolManager.allocatePage(leftPageId),
                indexMetadata
        )) {
            leftLeafPage.replaceTuples(split.leftTuples());
            leftLeafPage.setNextLeafBlockNumber(rightPageId.blockNumber());
        }

        try (BPlusTreePage rightLeafPage = BPlusTreePage.initializeLeaf(
                bufferPoolManager.allocatePage(rightPageId),
                indexMetadata
        )) {
            rightLeafPage.replaceTuples(split.rightTuples());
        }

        Tuple rootTuple = TupleSerializer.serializeInternal(
                split.separatorKey(),
                leftPageId.blockNumber(),
                indexMetadata
        );

        PageId rootPageId = new PageId(indexMetadata.containerId(), ROOT_BLOCK_NUMBER);
        try (BPlusTreePage rootPage = BPlusTreePage.initializeInternal(
                bufferPoolManager.writePage(rootPageId),
                indexMetadata,
                rightPageId.blockNumber()
        )) {
            rootPage.replaceTuples(List.of(rootTuple));
        }
    }

    private LeafSplit splitLeafTuples(LeafBTreeKey fullKey, BPlusTreePage leaf) {
        List<Tuple> tuples = leaf.tuples();
        tuples.add(TupleSerializer.serializeLeaf(fullKey, indexMetadata));
        tuples.sort(leafComparator());

        int middle = tuples.size() / 2;
        List<Tuple> leftTuples = new ArrayList<>(tuples.subList(0, middle));
        List<Tuple> rightTuples = new ArrayList<>(tuples.subList(middle, tuples.size()));
        LeafBTreeKey firstRightKey = LeafBTreeKey.ofLeaf(rightTuples.getFirst(), indexMetadata);

        return new LeafSplit(
                leftTuples,
                rightTuples,
                new BTreeKey(firstRightKey.columns())
        );
    }

    private BPlusTreePage findLeafPageForWrite(LeafBTreeKey searchKey) throws Exception {
        PageId leafPageId = findLeafPageId(searchKey);
        return BPlusTreePage.create(
                bufferPoolManager.writePage(leafPageId),
                indexMetadata
        );
    }

    private LeafDescent findLeafPageForWriteWithParents(LeafBTreeKey searchKey) throws Exception {
        DescentPath path = findLeafPageIdWithParents(searchKey);
        BPlusTreePage leaf = BPlusTreePage.create(
                bufferPoolManager.writePage(path.leafPageId()),
                indexMetadata
        );
        return new LeafDescent(leaf, path.parentPageIds());
    }

    private boolean isRootPage(BPlusTreePage page) {
        return page.getPageId().blockNumber() == ROOT_BLOCK_NUMBER;
    }

    private PageId allocatePageId() {
        return pageAllocator.allocate(indexMetadata.containerId());
    }

    private void insertIntoParent(
            PageId leftChildPageId,
            BTreeKey separatorKey,
            PageId rightChildPageId,
            Deque<PageId> parentPageIds
    ) throws Exception {
        if (parentPageIds.isEmpty()) {
            throw new IllegalStateException("Non-root split requires a parent page");
        }

        PageId parentPageId = parentPageIds.pop();
        Tuple newInternalTuple = TupleSerializer.serializeInternal(
                separatorKey,
                leftChildPageId.blockNumber(),
                indexMetadata
        );

        try (BPlusTreePage parentPage = BPlusTreePage.create(
                bufferPoolManager.writePage(parentPageId),
                indexMetadata
        )) {
            InternalPageEntries entries = internalPageEntries(parentPage);
            int childIndex = entries.children().indexOf(leftChildPageId.blockNumber());
            if (childIndex < 0) {
                throw new IllegalStateException("Parent does not reference child page " + leftChildPageId);
            }

            entries.keys().add(childIndex, separatorKey);
            entries.children().add(childIndex + 1, rightChildPageId.blockNumber());

            if (newInternalTuple.serializedSize() <= parentPage.getRemainingSpace()) {
                parentPage.replaceInternalTuples(
                        entries.children().getLast(),
                        serializeInternalEntries(entries)
                );
                return;
            }

            splitInternalPage(parentPage, entries, parentPageIds);
        }
    }

    private void splitInternalPage(
            BPlusTreePage page,
            InternalPageEntries entries,
            Deque<PageId> parentPageIds
    ) throws Exception {
        int middle = entries.keys().size() / 2;
        BTreeKey promotedKey = entries.keys().get(middle);

        InternalPageEntries leftEntries = new InternalPageEntries(
                new ArrayList<>(entries.keys().subList(0, middle)),
                new ArrayList<>(entries.children().subList(0, middle + 1))
        );
        InternalPageEntries rightEntries = new InternalPageEntries(
                new ArrayList<>(entries.keys().subList(middle + 1, entries.keys().size())),
                new ArrayList<>(entries.children().subList(middle + 1, entries.children().size()))
        );

        if (isRootPage(page)) {
            splitRootInternal(promotedKey, leftEntries, rightEntries, page);
            return;
        }

        page.replaceInternalTuples(
                leftEntries.children().getLast(),
                serializeInternalEntries(leftEntries)
        );

        PageId rightPageId = allocatePageId();
        try (BPlusTreePage rightPage = BPlusTreePage.initializeInternal(
                bufferPoolManager.allocatePage(rightPageId),
                indexMetadata,
                rightEntries.children().getLast()
        )) {
            rightPage.replaceInternalTuples(
                    rightEntries.children().getLast(),
                    serializeInternalEntries(rightEntries)
            );
        }

        insertIntoParent(page.getPageId(), promotedKey, rightPageId, parentPageIds);
    }

    private void splitRootInternal(
            BTreeKey promotedKey,
            InternalPageEntries leftEntries,
            InternalPageEntries rightEntries,
            BPlusTreePage rootPage
    ) throws Exception {
        PageId leftPageId = allocatePageId();
        try (BPlusTreePage leftPage = BPlusTreePage.initializeInternal(
                bufferPoolManager.allocatePage(leftPageId),
                indexMetadata,
                leftEntries.children().getLast()
        )) {
            leftPage.replaceInternalTuples(
                    leftEntries.children().getLast(),
                    serializeInternalEntries(leftEntries)
            );
        }

        PageId rightPageId = allocatePageId();
        try (BPlusTreePage rightPage = BPlusTreePage.initializeInternal(
                bufferPoolManager.allocatePage(rightPageId),
                indexMetadata,
                rightEntries.children().getLast()
        )) {
            rightPage.replaceInternalTuples(
                    rightEntries.children().getLast(),
                    serializeInternalEntries(rightEntries)
            );
        }

        Tuple rootTuple = TupleSerializer.serializeInternal(
                promotedKey,
                leftPageId.blockNumber(),
                indexMetadata
        );
        rootPage.replaceInternalTuples(rightPageId.blockNumber(), List.of(rootTuple));
    }

    private InternalPageEntries internalPageEntries(BPlusTreePage page) {
        List<BTreeKey> keys = new ArrayList<>();
        List<Long> children = new ArrayList<>();

        for (Tuple tuple : page.tuples()) {
            tuple.clear();
            children.add(tuple.readLong());
            keys.add(BTreeKey.ofInternal(tuple, indexMetadata));
        }
        children.add(page.getRightmostChildBlockNumber());

        return new InternalPageEntries(keys, children);
    }

    private List<Tuple> serializeInternalEntries(InternalPageEntries entries) {
        List<Tuple> tuples = new ArrayList<>(entries.keys().size());
        for (int i = 0; i < entries.keys().size(); i++) {
            tuples.add(TupleSerializer.serializeInternal(
                    entries.keys().get(i),
                    entries.children().get(i),
                    indexMetadata
            ));
        }
        return tuples;
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

        Comparator<Tuple> leafCmp = leafComparator();
        Tuple lowerTuple = TupleSerializer.serializeLeaf(lower, indexMetadata);
        Tuple upperTuple = TupleSerializer.serializeLeaf(upper, indexMetadata);
        PageId currentPageId = findLeafPageId(lower);
        boolean firstLeaf = true;

        while (currentPageId != null) {
            try (BPlusTreePage leaf = BPlusTreePage.create(
                    bufferPoolManager.readPage(currentPageId),
                    indexMetadata
            )) {
                int startPos = firstLeaf ? leaf.lowerBound(lowerTuple, leafCmp) : 0;

                for (int i = startPos; i < leaf.tupleCount(); i++) {
                    Tuple tuple = leaf.tupleAt(i);
                    if (leafCmp.compare(tuple, upperTuple) > 0) {
                        return results;
                    }
                    results.add(LeafBTreeKey.ofLeaf(tuple, indexMetadata).rid());
                }

                long nextLeafBlockNumber = leaf.getNextLeafBlockNumber();
                currentPageId = nextLeafBlockNumber == BPlusTreePage.NO_NEXT_LEAF
                        ? null
                        : new PageId(indexMetadata.containerId(), nextLeafBlockNumber);
                firstLeaf = false;
            }
        }

        return results;
    }

    private BPlusTreePage findLeafPage(LeafBTreeKey searchKey) throws Exception {
        PageId leafPageId = findLeafPageId(searchKey);
        return BPlusTreePage.create(
                bufferPoolManager.readPage(leafPageId),
                indexMetadata
        );
    }

    private PageId findLeafPageId(LeafBTreeKey searchKey) throws Exception {
        return findLeafPageIdWithParents(searchKey).leafPageId();
    }

    private DescentPath findLeafPageIdWithParents(LeafBTreeKey searchKey) throws Exception {
        PageId currentPageId = new PageId(
                indexMetadata.containerId(),
                ROOT_BLOCK_NUMBER
        );
        Deque<PageId> parentPageIds = new ArrayDeque<>();

        while (true) {
            BPlusTreePage page = BPlusTreePage.create(
                    bufferPoolManager.readPage(currentPageId),
                    indexMetadata
            );

            if (page.isLeaf()) {
                try {
                    return new DescentPath(currentPageId, parentPageIds);
                } finally {
                    page.close();
                }
            }

            PageId childPageId;
            try {
                parentPageIds.push(currentPageId);
                childPageId = findChildPageId(page, searchKey);
            } finally {
                page.close(); // release read latch before descending
            }

            currentPageId = childPageId;
        }
    }

    private record LeafDescent(BPlusTreePage leaf, Deque<PageId> parentPageIds) {}

    private record DescentPath(PageId leafPageId, Deque<PageId> parentPageIds) {}

    private record LeafSplit(List<Tuple> leftTuples, List<Tuple> rightTuples, BTreeKey separatorKey) {}

    private record InternalPageEntries(List<BTreeKey> keys, List<Long> children) {}

    // Routes through an internal page using upperBound on column-only comparison.
    // upperBound gives us the first separator strictly greater than the search key,
    // which is the correct child to descend into.
    private PageId findChildPageId(BPlusTreePage page, LeafBTreeKey searchKey) throws Exception {
        int pos = upperBoundInternal(page, searchKey);

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

    private int upperBoundInternal(BPlusTreePage page, LeafBTreeKey searchKey) {
        int l = 0, r = page.tupleCount();
        while (l < r) {
            int mid = l + (r - l) / 2;
            BTreeKey midKey = BTreeKey.ofInternal(page.tupleAt(mid), indexMetadata);
            if (midKey.compareColumnsTo(searchKey) <= 0) l = mid + 1;
            else r = mid;
        }
        return l;
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
}
