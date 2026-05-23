package com.akita.page;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

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

    public byte readByte() { return buffer.get(); }
    public short readShort() { return buffer.getShort(); }
    public int readInt() { return buffer.getInt(); }
    public long readLong() { return buffer.getLong(); }
    public float readFloat() { return buffer.getFloat(); }
    public double readDouble() { return buffer.getDouble(); }
    public boolean readBoolean() { return buffer.get() != 0; }
    public void readBytes(byte[] dst) { buffer.get(dst); }

    // -- Cursor control --

    public int position() { return buffer.position(); }
    public void clear() { buffer.clear(); }
    public Tuple seek(int position) { buffer.position(position); return this; }
    public int remaining() { return buffer.remaining(); }

    // -- Builder --

    public static Builder builder(int capacity) {
        return new Builder(capacity);
    }

    public static class Builder {
        private final ByteBuffer buffer;

        private Builder(int capacity) {
            this.buffer = ByteBuffer.allocate(capacity);
        }

        public Builder writeByte(byte value) { buffer.put(value); return this; }
        public Builder writeShort(short value) { buffer.putShort(value); return this; }
        public Builder writeInt(int value) { buffer.putInt(value); return this; }
        public Builder writeLong(long value) { buffer.putLong(value); return this; }
        public Builder writeFloat(float value) { buffer.putFloat(value); return this; }
        public Builder writeDouble(double value) { buffer.putDouble(value); return this; }
        public Builder writeBoolean(boolean value) { buffer.put(value ? (byte) 1 : (byte) 0); return this; }
        public Builder writeBytes(byte[] value) { buffer.put(value); return this; }
        public Builder writeVarchar(String value) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            buffer.putInt(bytes.length);
            buffer.put(bytes);
            return this;
        }

        public Tuple build() {
            buffer.flip();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return new Tuple(bytes);
        }
    }
}