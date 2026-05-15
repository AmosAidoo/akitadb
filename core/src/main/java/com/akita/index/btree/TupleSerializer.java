package com.akita.index.btree;

import com.akita.catalog.IndexMetadata;
import com.akita.datatype.AkitaType;
import com.akita.datatype.AkitaValue;
import com.akita.datatype.ColumnMetadata;
import com.akita.page.RecordId;
import com.akita.page.Tuple;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

class TupleSerializer {

    private TupleSerializer() {}

    // Leaf tuple layout:
    // | indexed_cols... | containerId MSB (long) | containerId LSB (long) | blockNumber (long) | slotNumber (short) |
    static Tuple serializeLeaf(LeafBTreeKey key, IndexMetadata meta) {
        int capacity = computeColumnsSize(key, meta) + ridSize();
        Tuple.Builder builder = Tuple.builder(capacity);
        writeColumns(builder, key, meta);
        writeRid(builder, key.rid());
        return builder.build();
    }

    // Internal tuple layout:
    // | left_child_block_number (long) | indexed_cols... |
    static Tuple serializeInternal(BTreeKey key, long leftChildBlockNumber, IndexMetadata meta) {
        int capacity = Long.BYTES + computeColumnsSize(key, meta);
        Tuple.Builder builder = Tuple.builder(capacity);
        builder.writeLong(leftChildBlockNumber);
        writeColumns(builder, key, meta);
        return builder.build();
    }

    private static void writeColumns(Tuple.Builder builder, BTreeKeyComparable key, IndexMetadata meta) {
        for (int i = 0; i < meta.schema().columns().size(); i++) {
            ColumnMetadata col = meta.schema().columns().get(i);
            AkitaValue value   = key.columns().get(i);
            writeValue(builder, col, value);
        }
    }

    private static void writeValue(Tuple.Builder builder, ColumnMetadata col, AkitaValue value) {
        if (col.nullable()) {
            builder.writeByte(value instanceof AkitaValue.Null ? (byte) 0 : (byte) 1);
        }

        if (value instanceof AkitaValue.Null) return;

        switch (value) {
            case AkitaValue.IntVal(var v)     -> builder.writeInt(v);
            case AkitaValue.BigIntVal(var v)  -> builder.writeLong(v);
            case AkitaValue.DoubleVal(var v)  -> builder.writeDouble(v);
            case AkitaValue.BoolVal(var v)    -> builder.writeBoolean(v);
            case AkitaValue.VarcharVal(var v) -> builder.writeVarchar(v);
            default -> throw new IllegalStateException("Unexpected value: " + value);
        }
    }

    private static void writeRid(Tuple.Builder builder, RecordId rid) {
        UUID uuid = rid.pageId().containerId().getValue();
        builder.writeLong(uuid.getMostSignificantBits());
        builder.writeLong(uuid.getLeastSignificantBits());
        builder.writeLong(rid.pageId().blockNumber());
        builder.writeShort(rid.slotNumber());
    }

    private static int computeColumnsSize(BTreeKeyComparable key, IndexMetadata meta) {
        int size = 0;
        for (int i = 0; i < meta.schema().columns().size(); i++) {
            ColumnMetadata col = meta.schema().columns().get(i);
            AkitaValue value   = key.columns().get(i);
            if (col.nullable()) size += Byte.BYTES;
            size += valueSize(col, value);
        }
        return size;
    }

    private static int valueSize(ColumnMetadata col, AkitaValue value) {
        if (value instanceof AkitaValue.Null) return 0;
        return switch (col.type()) {
            case AkitaType.Integer ignored -> Integer.BYTES;
            case AkitaType.BigInt ignored  -> Long.BYTES;
            case AkitaType.Double ignored  -> Double.BYTES;
            case AkitaType.Boolean ignored    -> Byte.BYTES;
            case AkitaType.Varchar ignored -> Integer.BYTES + ((AkitaValue.VarcharVal) value).value()
                    .getBytes(StandardCharsets.UTF_8).length;
        };
    }

    private static int ridSize() {
        // containerId MSB + LSB + blockNumber + slotNumber
        return Long.BYTES + Long.BYTES + Long.BYTES + Short.BYTES;
    }

    public static int computeLeafSize(LeafBTreeKey key, IndexMetadata meta) {
        return computeColumnsSize(key, meta) + ridSize();
    }
}