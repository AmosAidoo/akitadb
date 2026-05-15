package com.akita.page;

public class Slot implements Comparable<Slot> {
    public static final int SERIALIZED_SIZE = 4;
    private short offset;
    private short length;

    private Slot(short offset, short length) {
        this.offset = offset;
        this.length = length;
    }

    public static Slot create(short offset, short length) {
        return new Slot(offset, length);
    }

    public short getOffset() {
        return offset;
    }

    public short getLength() {
        return length;
    }

    @Override
    public int compareTo(Slot o) {
        return Short.compare(offset, o.offset);
    }
}
