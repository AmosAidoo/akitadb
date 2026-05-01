package com.akita.page;

public class Slot {
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
}
