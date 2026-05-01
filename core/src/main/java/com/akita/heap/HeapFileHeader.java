package com.akita.heap;

import java.nio.ByteBuffer;

public class HeapFileHeader {
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
}
