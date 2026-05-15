package com.akita.index.btree;

import com.akita.buffer.PageId;
import com.akita.catalog.IndexMetadata;
import com.akita.datatype.AkitaValue;
import com.akita.page.RecordId;
import com.akita.page.Tuple;
import com.akita.storage.ContainerId;

import java.util.List;
import java.util.UUID;

public record LeafBTreeKey(List<AkitaValue> columns, RecordId rid)
        implements BTreeKeyComparable, Comparable<LeafBTreeKey> {

    public static LeafBTreeKey withMinRid(List<AkitaValue> columns) {
        RecordId minRid = new RecordId(
                new PageId(ContainerId.MIN, Long.MIN_VALUE),
                Short.MIN_VALUE
        );
        return new LeafBTreeKey(columns, minRid);
    }

    public static LeafBTreeKey withMaxRid(List<AkitaValue> columns) {
        RecordId maxRid = new RecordId(
                new PageId(ContainerId.MAX, Long.MAX_VALUE),
                Short.MAX_VALUE
        );
        return new LeafBTreeKey(columns, maxRid);
    }

    @Override
    public int compareTo(LeafBTreeKey o) {
        int res = compareColumnsTo(o);
        return res != 0 ? res : rid.compareTo(o.rid);
    }

    // Leaf tuple layout:
    // | indexed_cols... | containerId MSB (long) | containerId LSB (long) | blockNumber (long) | slotNumber (short) |
    public static LeafBTreeKey ofLeaf(Tuple tuple, IndexMetadata meta) {
        tuple.clear();
        List<AkitaValue> columns = BTreeKey.readColumns(tuple, meta);

        long msb         = tuple.readLong();
        long lsb         = tuple.readLong();
        long blockNumber = tuple.readLong();
        short slotNumber = tuple.readShort();

        RecordId rid = new RecordId(
                new PageId(ContainerId.fromUUID(new UUID(msb, lsb)), blockNumber),
                slotNumber
        );

        return new LeafBTreeKey(List.copyOf(columns), rid);
    }
}