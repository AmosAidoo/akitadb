package com.akita.index.btree;

import com.akita.buffer.BufferPoolManager;
import com.akita.buffer.PageId;
import com.akita.catalog.IndexMetadata;
import com.akita.datatype.AkitaValue;
import com.akita.page.Tuple;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

public final class BPlusTreeDotDumper {
    private static final long ROOT_BLOCK_NUMBER = 1;

    private BPlusTreeDotDumper() {}

    public static String dump(BPlusTree tree) throws Exception {
        return dump(tree.indexMetadata(), tree.bufferPoolManager());
    }

    static String dump(IndexMetadata indexMetadata, BufferPoolManager bufferPoolManager) throws Exception {
        StringBuilder dot = new StringBuilder();
        dot.append("digraph bplustree {\n");
        dot.append("  node [shape=record];\n");
        dot.append("\n");

        Queue<Long> pending = new ArrayDeque<>();
        Set<Long> visited = new LinkedHashSet<>();
        List<String> edges = new ArrayList<>();
        pending.add(ROOT_BLOCK_NUMBER);

        while (!pending.isEmpty()) {
            long blockNumber = pending.remove();
            if (!visited.add(blockNumber)) {
                continue;
            }

            PageId pageId = new PageId(indexMetadata.containerId(), blockNumber);
            try (BPlusTreePage page = BPlusTreePage.create(bufferPoolManager.readPage(pageId), indexMetadata)) {
                dot.append("  page_")
                        .append(blockNumber)
                        .append(" [label=\"")
                        .append(labelFor(blockNumber, page, indexMetadata))
                        .append("\"];\n");

                if (page.isInternal()) {
                    for (Long childBlockNumber : childBlockNumbers(page)) {
                        edges.add("  page_" + blockNumber + " -> page_" + childBlockNumber + ";");
                        if (!visited.contains(childBlockNumber)) {
                            pending.add(childBlockNumber);
                        }
                    }
                }
            }
        }

        if (!edges.isEmpty()) {
            dot.append("\n");
            for (String edge : edges) {
                dot.append(edge).append("\n");
            }
        }

        dot.append("}\n");
        return dot.toString();
    }

    private static String labelFor(long blockNumber, BPlusTreePage page, IndexMetadata indexMetadata) {
        return "{page=" + blockNumber + " | " + pageType(page) + " | keys: " + keys(page, indexMetadata) + "}";
    }

    private static String pageType(BPlusTreePage page) {
        if (page.isInternal()) {
            return "INTERNAL";
        }
        if (page.isLeaf()) {
            return "LEAF";
        }
        return "INVALID";
    }

    private static String keys(BPlusTreePage page, IndexMetadata indexMetadata) {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < page.tupleCount(); i++) {
            Tuple tuple = page.tupleAt(i);
            BTreeKey key = page.isInternal()
                    ? BTreeKey.ofInternal(tuple, indexMetadata)
                    : new BTreeKey(LeafBTreeKey.ofLeaf(tuple, indexMetadata).columns());
            keys.add(formatColumns(key.columns()));
        }
        return String.join(", ", keys);
    }

    private static List<Long> childBlockNumbers(BPlusTreePage page) {
        List<Long> children = new ArrayList<>();
        for (int i = 0; i < page.tupleCount(); i++) {
            Tuple tuple = page.tupleAt(i);
            tuple.clear();
            children.add(tuple.readLong());
        }
        children.add(page.getRightmostChildBlockNumber());
        return children;
    }

    private static String formatColumns(List<AkitaValue> columns) {
        List<String> values = new ArrayList<>(columns.size());
        for (AkitaValue column : columns) {
            values.add(formatValue(column));
        }
        return String.join("/", values);
    }

    private static String formatValue(AkitaValue value) {
        return switch (value) {
            case AkitaValue.IntVal(var v) -> Integer.toString(v);
            case AkitaValue.BigIntVal(var v) -> Long.toString(v);
            case AkitaValue.DoubleVal(var v) -> Double.toString(v);
            case AkitaValue.BoolVal(var v) -> Boolean.toString(v);
            case AkitaValue.VarcharVal(var v) -> escape(v);
            case AkitaValue.Null ignored -> "NULL";
        };
    }

    private static String escape(String value) {
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("{", "\\{")
                .replace("}", "\\}")
                .replace("|", "\\|");
    }
}
