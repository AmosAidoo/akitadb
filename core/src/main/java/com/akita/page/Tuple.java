package com.akita.page;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Tuple {
    private final ByteBuffer buffer;

    public Tuple(byte[] data) {
        // Own a copy — don't hold a reference into the buffer pool's memory
        this.buffer = ByteBuffer.wrap(data.clone());
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

    // -- Raw access --

    public byte[] toBytes() {
        return buffer.array().clone();
    }
}