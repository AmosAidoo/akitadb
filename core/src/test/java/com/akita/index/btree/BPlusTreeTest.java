package com.akita.index.btree;

import com.akita.buffer.BufferPoolManager;
import com.akita.buffer.PageId;
import com.akita.buffer.guards.ReadPageGuard;
import com.akita.buffer.guards.WritePageGuard;
import com.akita.catalog.IndexMetadata;
import com.akita.datatype.AkitaType;
import com.akita.datatype.AkitaValue;
import com.akita.datatype.ColumnMetadata;
import com.akita.datatype.Schema;
import com.akita.heap.HeapFileHeader;
import com.akita.heap.ObjectType;
import com.akita.page.RecordId;
import com.akita.page.Tuple;
import com.akita.storage.BlockManager;
import com.akita.storage.ContainerId;
import com.akita.storage.FileChannelBlockManager;
import com.akita.storage.FileChannelContainerManager;
import com.akita.testing.AkitaExtension;
import com.akita.testing.SlottedPageWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(AkitaExtension.class)
class BPlusTreeTest {

    @Test
    void createNewInitializesEmptyLeafRoot(
            BufferPoolManager bpm,
            FileChannelBlockManager bm,
            FileChannelContainerManager cm
    ) throws Exception {
        ContainerId containerId = createIndexContainer(bm, cm);
        IndexMetadata metadata = intIndexMetadata(containerId);
        RecordId rid10 = fakeHeapRid(containerId, 10);

        BPlusTree tree = BPlusTree.createNew(metadata, bpm);
        tree.insert(leafKey(10, rid10));

        assertThat(tree.find(leafKey(10, rid10))).isEqualTo(rid10);
        assertThat(BPlusTreeDotDumper.dump(tree))
                .contains("page_1 [label=\"{page=1 | LEAF | keys: 10}\"]");
    }

    @Test
    void findsKeyInLeafRoot(
            BufferPoolManager bpm,
            FileChannelBlockManager bm,
            FileChannelContainerManager cm
    ) throws Exception {
        ContainerId containerId = createIndexContainer(bm, cm);
        IndexMetadata metadata = intIndexMetadata(containerId);
        RecordId rid10 = fakeHeapRid(containerId, 10);
        RecordId rid20 = fakeHeapRid(containerId, 20);

        writeLeaf(bm, metadata, 1,
                leafKey(10, rid10),
                leafKey(20, rid20)
        );

        BPlusTree tree = BPlusTree.open(metadata, bpm);

        assertThat(tree.find(leafKey(20, rid20))).isEqualTo(rid20);
        assertThat(tree.find(leafKey(30, fakeHeapRid(containerId, 30)))).isNull();
    }

    @Test
    void findsKeyThroughInternalRoot(
            BufferPoolManager bpm,
            FileChannelBlockManager bm,
            FileChannelContainerManager cm
    ) throws Exception {
        ContainerId containerId = createIndexContainer(bm, cm);
        IndexMetadata metadata = intIndexMetadata(containerId);
        RecordId rid5 = fakeHeapRid(containerId, 5);
        RecordId rid10 = fakeHeapRid(containerId, 10);
        RecordId rid20 = fakeHeapRid(containerId, 20);
        RecordId rid30 = fakeHeapRid(containerId, 30);

        writeInternal(bm, metadata, 1, 4,
                internalKey(10, 2),
                internalKey(20, 3)
        );
        writeLeaf(bm, metadata, 2, leafKey(5, rid5));
        writeLeaf(bm, metadata, 3, leafKey(10, rid10), leafKey(15, fakeHeapRid(containerId, 15)));
        writeLeaf(bm, metadata, 4, leafKey(20, rid20), leafKey(30, rid30));

        BPlusTree tree = BPlusTree.open(metadata, bpm);

        assertThat(tree.find(leafKey(5, rid5))).isEqualTo(rid5);
        assertThat(tree.find(leafKey(10, rid10))).isEqualTo(rid10);
        assertThat(tree.find(leafKey(20, rid20))).isEqualTo(rid20);
        assertThat(tree.find(leafKey(30, rid30))).isEqualTo(rid30);
    }

    @Test
    void dumpsTreeAsGraphVizDot(
            BufferPoolManager bpm,
            FileChannelBlockManager bm,
            FileChannelContainerManager cm
    ) throws Exception {
        ContainerId containerId = createIndexContainer(bm, cm);
        IndexMetadata metadata = intIndexMetadata(containerId);

        writeInternal(bm, metadata, 1, 4,
                internalKey(10, 2),
                internalKey(20, 3)
        );
        writeLeaf(bm, metadata, 2, leafKey(5, fakeHeapRid(containerId, 5)));
        writeLeaf(bm, metadata, 3, leafKey(10, fakeHeapRid(containerId, 10)));
        writeLeaf(bm, metadata, 4, leafKey(20, fakeHeapRid(containerId, 20)));

        BPlusTree tree = BPlusTree.open(metadata, bpm);

        assertThat(BPlusTreeDotDumper.dump(tree)).isEqualTo("""
                digraph bplustree {
                  node [shape=record];

                  page_1 [label="{page=1 | INTERNAL | keys: 10, 20}"];
                  page_2 [label="{page=2 | LEAF | keys: 5}"];
                  page_3 [label="{page=3 | LEAF | keys: 10}"];
                  page_4 [label="{page=4 | LEAF | keys: 20}"];

                  page_1 -> page_2;
                  page_1 -> page_3;
                  page_1 -> page_4;
                }
                """);
    }

    @Test
    void insertsIntoNonFullLeafRoot(
            BufferPoolManager bpm,
            FileChannelBlockManager bm,
            FileChannelContainerManager cm
    ) throws Exception {
        ContainerId containerId = createIndexContainer(bm, cm);
        IndexMetadata metadata = intIndexMetadata(containerId);
        RecordId rid10 = fakeHeapRid(containerId, 10);
        RecordId rid20 = fakeHeapRid(containerId, 20);

        writeLeaf(bm, metadata, 1, leafKey(10, rid10));

        BPlusTree tree = BPlusTree.open(metadata, bpm);
        tree.insert(leafKey(20, rid20));

        assertThat(tree.find(leafKey(20, rid20))).isEqualTo(rid20);
        assertThat(BPlusTreeDotDumper.dump(tree)).contains("page_1 [label=\"{page=1 | LEAF | keys: 10, 20}\"]");
    }

    @Test
    void splitsFullLeafRoot(
            BufferPoolManager bpm,
            FileChannelBlockManager bm,
            FileChannelContainerManager cm
    ) throws Exception {
        ContainerId containerId = createIndexContainer(bm, cm);
        IndexMetadata metadata = varcharIndexMetadata(containerId);
        List<LeafBTreeKey> initialKeys = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            initialKeys.add(leafKey(paddedKey(i), fakeHeapRid(containerId, i)));
        }
        LeafBTreeKey insertedKey = leafKey(paddedKey(16), fakeHeapRid(containerId, 16));

        writeLeaf(bm, metadata, 1, initialKeys.toArray(LeafBTreeKey[]::new));

        BPlusTree tree = BPlusTree.open(metadata, bpm);
        tree.insert(insertedKey);

        assertThat(tree.find(initialKeys.getFirst())).isEqualTo(initialKeys.getFirst().rid());
        assertThat(tree.find(insertedKey)).isEqualTo(insertedKey.rid());
        assertThat(tree.find(initialKeys.getLast())).isEqualTo(initialKeys.getLast().rid());
        assertThat(BPlusTreeDotDumper.dump(tree)).contains(
                "page_1 [label=\"{page=1 | INTERNAL | keys: " + paddedKey(8) + "}\"]",
                "page_2 [label=\"{page=2 | LEAF | keys: " + paddedKey(0),
                "page_3 [label=\"{page=3 | LEAF | keys: " + paddedKey(8),
                "page_1 -> page_2;",
                "page_1 -> page_3;"
        );
    }

    @Test
    void initializesAllocatedLeafPage(
            BufferPoolManager bpm,
            FileChannelBlockManager bm,
            FileChannelContainerManager cm
    ) throws Exception {
        ContainerId containerId = createIndexContainer(bm, cm);
        IndexMetadata metadata = intIndexMetadata(containerId);
        PageId pageId = new PageId(containerId, 5);

        WritePageGuard writeGuard = bpm.allocatePage(pageId);
        try (BPlusTreePage page = BPlusTreePage.initializeLeaf(writeGuard, metadata)) {
            assertThat(page.isLeaf()).isTrue();
            assertThat(page.tupleCount()).isZero();
        }

        ReadPageGuard readGuard = bpm.readPage(pageId);
        try (BPlusTreePage page = BPlusTreePage.create(readGuard, metadata)) {
            assertThat(page.isLeaf()).isTrue();
            assertThat(page.tupleCount()).isZero();
        }
    }

    @Test
    void initializesAllocatedInternalPage(
            BufferPoolManager bpm,
            FileChannelBlockManager bm,
            FileChannelContainerManager cm
    ) throws Exception {
        ContainerId containerId = createIndexContainer(bm, cm);
        IndexMetadata metadata = intIndexMetadata(containerId);
        PageId pageId = new PageId(containerId, 5);

        WritePageGuard writeGuard = bpm.allocatePage(pageId);
        try (BPlusTreePage page = BPlusTreePage.initializeInternal(writeGuard, metadata, 42)) {
            assertThat(page.isInternal()).isTrue();
            assertThat(page.getRightmostChildBlockNumber()).isEqualTo(42);
            assertThat(page.tupleCount()).isZero();
        }

        ReadPageGuard readGuard = bpm.readPage(pageId);
        try (BPlusTreePage page = BPlusTreePage.create(readGuard, metadata)) {
            assertThat(page.isInternal()).isTrue();
            assertThat(page.getRightmostChildBlockNumber()).isEqualTo(42);
            assertThat(page.tupleCount()).isZero();
        }
    }

    @Test
    void rewritesLeafTuples(
            BufferPoolManager bpm,
            FileChannelBlockManager bm,
            FileChannelContainerManager cm
    ) throws Exception {
        ContainerId containerId = createIndexContainer(bm, cm);
        IndexMetadata metadata = intIndexMetadata(containerId);
        RecordId rid10 = fakeHeapRid(containerId, 10);
        RecordId rid20 = fakeHeapRid(containerId, 20);
        RecordId rid30 = fakeHeapRid(containerId, 30);
        PageId pageId = new PageId(containerId, 1);

        writeLeaf(bm, metadata, 1,
                leafKey(10, rid10),
                leafKey(20, rid20),
                leafKey(30, rid30)
        );

        WritePageGuard writeGuard = bpm.writePage(pageId);
        try (BPlusTreePage page = BPlusTreePage.create(writeGuard, metadata)) {
            List<Tuple> tuples = page.tuples();
            page.replaceTuples(List.of(tuples.get(1), tuples.get(2)));
            assertThat(page.tupleCount()).isEqualTo(2);
        }

        BPlusTree tree = BPlusTree.open(metadata, bpm);

        assertThat(tree.find(leafKey(10, rid10))).isNull();
        assertThat(tree.find(leafKey(20, rid20))).isEqualTo(rid20);
        assertThat(tree.find(leafKey(30, rid30))).isEqualTo(rid30);
        assertThat(BPlusTreeDotDumper.dump(tree)).contains("page_1 [label=\"{page=1 | LEAF | keys: 20, 30}\"]");
    }

    @Test
    void splitsNonRootLeafIntoNonFullParent(
            BufferPoolManager bpm,
            FileChannelBlockManager bm,
            FileChannelContainerManager cm
    ) throws Exception {
        ContainerId containerId = createIndexContainer(bm, cm);
        IndexMetadata metadata = varcharIndexMetadata(containerId);
        List<LeafBTreeKey> initialKeys = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            initialKeys.add(leafKey(paddedKey(i), fakeHeapRid(containerId, i)));
        }
        LeafBTreeKey insertedKey = leafKey(paddedKey(16), fakeHeapRid(containerId, 16));
        LeafBTreeKey rightTreeKey = leafKey(paddedKey(50), fakeHeapRid(containerId, 50));

        writeInternal(bm, metadata, 1, 3, internalKey(paddedKey(50), 2));
        writeLeaf(bm, metadata, 2, initialKeys.toArray(LeafBTreeKey[]::new));
        writeLeaf(bm, metadata, 3, rightTreeKey);
        writeIndexPageDirectory(bm, containerId, 1, 2, 3);

        BPlusTree tree = BPlusTree.open(metadata, bpm);
        tree.insert(insertedKey);

        assertThat(tree.find(initialKeys.getFirst())).isEqualTo(initialKeys.getFirst().rid());
        assertThat(tree.find(insertedKey)).isEqualTo(insertedKey.rid());
        assertThat(tree.find(initialKeys.getLast())).isEqualTo(initialKeys.getLast().rid());
        assertThat(tree.find(rightTreeKey)).isEqualTo(rightTreeKey.rid());
        assertThat(BPlusTreeDotDumper.dump(tree)).contains(
                "page_1 [label=\"{page=1 | INTERNAL | keys: " + paddedKey(8) + ", " + paddedKey(50) + "}\"]",
                "page_1 -> page_2;",
                "page_1 -> page_4;",
                "page_1 -> page_3;",
                "page_4 [label=\"{page=4 | LEAF | keys: " + paddedKey(8)
        );
    }

    @Test
    void splitsFullRootInternalWhenLeafPromotesIntoIt(
            BufferPoolManager bpm,
            FileChannelBlockManager bm,
            FileChannelContainerManager cm
    ) throws Exception {
        ContainerId containerId = createIndexContainer(bm, cm);
        IndexMetadata metadata = varcharIndexMetadata(containerId);
        List<LeafBTreeKey> leftmostKeys = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            leftmostKeys.add(leafKey(paddedKey(i), fakeHeapRid(containerId, i)));
        }
        LeafBTreeKey insertedKey = leafKey(paddedKey(16), fakeHeapRid(containerId, 16));
        List<InternalEntry> rootEntries = new ArrayList<>();
        List<Long> existingBlocks = new ArrayList<>();
        existingBlocks.add(1L);

        for (int i = 0; i < 16; i++) {
            long childBlockNumber = i + 2L;
            rootEntries.add(internalKey(paddedKey(50 + i), childBlockNumber));
            existingBlocks.add(childBlockNumber);
        }
        existingBlocks.add(18L);

        writeInternal(bm, metadata, 1, 18, rootEntries.toArray(InternalEntry[]::new));
        writeLeaf(bm, metadata, 2, leftmostKeys.toArray(LeafBTreeKey[]::new));
        for (int i = 0; i < 16; i++) {
            writeLeaf(bm, metadata, i + 3L, leafKey(paddedKey(50 + i), fakeHeapRid(containerId, 50 + i)));
        }
        writeIndexPageDirectory(
                bm,
                containerId,
                existingBlocks.stream().mapToLong(Long::longValue).toArray()
        );

        BPlusTree tree = BPlusTree.open(metadata, bpm);
        tree.insert(insertedKey);

        assertThat(tree.find(leftmostKeys.getFirst())).isEqualTo(leftmostKeys.getFirst().rid());
        assertThat(tree.find(insertedKey)).isEqualTo(insertedKey.rid());
        assertThat(tree.find(leafKey(paddedKey(65), fakeHeapRid(containerId, 65))))
                .isEqualTo(fakeHeapRid(containerId, 65));
        assertThat(BPlusTreeDotDumper.dump(tree)).contains(
                "page_1 [label=\"{page=1 | INTERNAL | keys: " + paddedKey(57) + "}\"]",
                "page_20 [label=\"{page=20 | INTERNAL | keys: " + paddedKey(8),
                "page_21 [label=\"{page=21 | INTERNAL | keys: " + paddedKey(58),
                "page_19 [label=\"{page=19 | LEAF | keys: " + paddedKey(8)
        );
    }

    private static ContainerId createIndexContainer(
            FileChannelBlockManager bm,
            FileChannelContainerManager cm
    ) throws Exception {
        ContainerId containerId = cm.createContainer();
        ByteBuffer firstPage = ByteBuffer.allocate(BlockManager.BLOCK_SIZE);
        HeapFileHeader.write(firstPage, ObjectType.INDEX);
        firstPage.putShort((short) 0); // page-directory slots
        firstPage.putShort((short) 0); // no next page directory
        bm.writeBlock(containerId, 0, firstPage);
        return containerId;
    }

    private static IndexMetadata intIndexMetadata(ContainerId containerId) {
        return new IndexMetadata(
                "idx_test_key",
                "test",
                new Schema(List.of(new ColumnMetadata("key", new AkitaType.Integer(), 0, false))),
                containerId,
                true
        );
    }

    private static IndexMetadata varcharIndexMetadata(ContainerId containerId) {
        return new IndexMetadata(
                "idx_test_key",
                "test",
                new Schema(List.of(new ColumnMetadata("key", new AkitaType.Varchar(4096), 0, false))),
                containerId,
                true
        );
    }

    private static void writeLeaf(
            FileChannelBlockManager bm,
            IndexMetadata metadata,
            long blockNumber,
            LeafBTreeKey... keys
    ) throws Exception {
        SlottedPageWriter writer = SlottedPageWriter.create(bm);
        for (LeafBTreeKey key : keys) {
            writer.addTuple(TupleSerializer.serializeLeaf(key, metadata));
        }
        writer.writeTo(new PageId(metadata.containerId(), blockNumber), leafHeader());
    }

    private static void writeInternal(
            FileChannelBlockManager bm,
            IndexMetadata metadata,
            long blockNumber,
            long rightmostChildBlockNumber,
            InternalEntry... entries
    ) throws Exception {
        SlottedPageWriter writer = SlottedPageWriter.create(bm);
        for (InternalEntry entry : entries) {
            writer.addTuple(TupleSerializer.serializeInternal(entry.key(), entry.leftChildBlockNumber(), metadata));
        }
        writer.writeTo(new PageId(metadata.containerId(), blockNumber), internalHeader(rightmostChildBlockNumber));
    }

    private static void writeIndexPageDirectory(
            FileChannelBlockManager bm,
            ContainerId containerId,
            long... blockNumbers
    ) throws Exception {
        ByteBuffer prefixHeader = ByteBuffer.allocate(HeapFileHeader.SIZE);
        HeapFileHeader.write(prefixHeader, ObjectType.INDEX);
        ByteBuffer additionalHeaders = ByteBuffer.allocate(Short.BYTES);
        additionalHeaders.putShort((short) 0);

        SlottedPageWriter writer = SlottedPageWriter.create(bm);
        for (long blockNumber : blockNumbers) {
            writer.addPageDirectoryTuple(blockNumber, 0);
        }
        writer.writeTo(new PageId(containerId, 0), prefixHeader, additionalHeaders);
    }

    private static ByteBuffer leafHeader() {
        ByteBuffer header = ByteBuffer.allocate(Byte.BYTES);
        header.put((byte) 2);
        return header;
    }

    private static ByteBuffer internalHeader(long rightmostChildBlockNumber) {
        ByteBuffer header = ByteBuffer.allocate(Byte.BYTES + Long.BYTES);
        header.put((byte) 1);
        header.putLong(rightmostChildBlockNumber);
        return header;
    }

    private static LeafBTreeKey leafKey(int value, RecordId rid) {
        return new LeafBTreeKey(List.of(new AkitaValue.IntVal(value)), rid);
    }

    private static LeafBTreeKey leafKey(String value, RecordId rid) {
        return new LeafBTreeKey(List.of(new AkitaValue.VarcharVal(value)), rid);
    }

    private static String paddedKey(int value) {
        return String.format("%02d", value) + "x".repeat(470);
    }

    private static InternalEntry internalKey(int value, long leftChildBlockNumber) {
        return new InternalEntry(new BTreeKey(List.of(new AkitaValue.IntVal(value))), leftChildBlockNumber);
    }

    private static InternalEntry internalKey(String value, long leftChildBlockNumber) {
        return new InternalEntry(new BTreeKey(List.of(new AkitaValue.VarcharVal(value))), leftChildBlockNumber);
    }

    private static RecordId fakeHeapRid(ContainerId containerId, int value) {
        return new RecordId(new PageId(containerId, 100 + value), (short) value);
    }

    private record InternalEntry(BTreeKey key, long leftChildBlockNumber) {}
}
