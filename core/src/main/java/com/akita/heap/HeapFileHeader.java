package com.akita.heap;

import java.nio.ByteBuffer;

public class HeapFileHeader {
    public static final int SIZE = Integer.BYTES;

    ObjectType type;

    private HeapFileHeader(ObjectType type) {
        this.type = type;
    }

    public static HeapFileHeader parse(ByteBuffer data) {
        int typeCode = data.getInt();
        ObjectType type = switch (typeCode) {
            case 0 -> ObjectType.TABLE;
            case 1 -> ObjectType.INDEX;
            default -> throw new IllegalArgumentException("Unknown object type: " + typeCode);
        };
        return new HeapFileHeader(type);
    }

    public static void write(ByteBuffer data, ObjectType type) {
        data.putInt(switch (type) {
            case TABLE -> 0;
            case INDEX -> 1;
        });
    }
}
