package com.akita.page;

import java.nio.ByteBuffer;

public class Tuple {
    private final ByteBuffer buffer;

    public Tuple(byte[] data) {
        this.buffer = ByteBuffer.wrap(data);
    }

    public Tuple(ByteBuffer buffer) {
        ByteBuffer duplicate = buffer.duplicate();
        duplicate.clear();
        this.buffer = duplicate;
    }

    public int size() {
        return buffer.capacity();
    }

    public int serializedSize() {
        return buffer.capacity() + Slot.SERIALIZED_SIZE;
    }

    public ByteBuffer getBuffer() {
        buffer.clear();
        return buffer;
    }

    // -- Relative reads (advance the cursor) --

    public byte readByte() {
        return buffer.get();
    }

    public short readShort() {
        return buffer.getShort();
    }

    public int readInt() {
        return buffer.getInt();
    }

    public long readLong() {
        return buffer.getLong();
    }

    public float readFloat() {
        return buffer.getFloat();
    }

    public double readDouble() {
        return buffer.getDouble();
    }

    public boolean readBoolean() {
        return buffer.get() != 0;
    }

    // -- Cursor control --

    public int position() {
        return buffer.position();
    }

    public void clear() {
        buffer.clear();
    }

    public Tuple seek(int position) {
        buffer.position(position);
        return this;
    }

    public int remaining() {
        return buffer.remaining();
    }
}