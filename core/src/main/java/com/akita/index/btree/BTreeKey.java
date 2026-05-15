package com.akita.index.btree;

import com.akita.catalog.IndexMetadata;
import com.akita.datatype.AkitaValue;
import com.akita.datatype.ColumnMetadata;
import com.akita.page.Tuple;

import java.util.ArrayList;
import java.util.List;

public record BTreeKey(List<AkitaValue> columns) implements BTreeKeyComparable, Comparable<BTreeKey> {

    @Override
    public int compareTo(BTreeKey o) {
        return compareColumnsTo(o);
    }

    // Internal tuple layout: | left_child_block_number (long) | indexed_cols... |
    public static BTreeKey ofInternal(Tuple tuple, IndexMetadata meta) {
        tuple.clear();
        tuple.readLong(); // skip left_child_block_number
        List<AkitaValue> columns = readColumns(tuple, meta);
        return new BTreeKey(List.copyOf(columns));
    }

    static List<AkitaValue> readColumns(Tuple tuple, IndexMetadata meta) {
        List<AkitaValue> columns = new ArrayList<>(meta.schema().columns().size());
        for (ColumnMetadata col : meta.schema().columns()) {
            columns.add(TupleDeserializer.readValue(tuple, col));
        }
        return columns;
    }
}