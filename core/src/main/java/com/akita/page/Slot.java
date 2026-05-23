package com.akita.page;

public class Slot implements Comparable<Slot> {
    public static final int SERIALIZED_SIZE = 4;
    private final short index;
    private short offset;
    private short length;

    private Slot(short index, short offset, short length) {
        this.index = index;
        this.offset = offset;
        this.length = length;
    }

    public static Slot create(short index, short offset, short length) {
        return new Slot(index, offset, length);
    }

    public short getIndex() {
        return index;
    }

    public short getOffset() {
        return offset;
    }

    public short getLength() {
        return length;
    }

    public byte[] getBytes() {
        byte[] bytes = new byte[SERIALIZED_SIZE];
        bytes[0] = (byte) ((offset >> 8) & 0xff);
        bytes[1] = (byte) ((offset) & 0xff);
        bytes[2] = (byte) ((length >> 8) & 0xff);
        bytes[3] = (byte) (length & 0xff);
        return bytes;
    }

    @Override
    public int compareTo(Slot o) {
        return Short.compare(offset, o.offset);
    }
}
