package com.akita.page;

import com.akita.storage.BlockManager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * This is a generic slotted page implementation.
 * More specialized slotted pages will extend it and add additional features
 */
public abstract class SlottedPage {
    protected PageHeader pageHeader;

    public List<Slot> getSlots() {
        return slots;
    }

    protected List<Slot> slots;
    protected ByteBuffer data;

    public final void parsePage(ByteBuffer data) {
        this.pageHeader = parseBaseHeader(data);
        parseExtendedHeader(data);
        parseAndSetSlots(data);
        setData(data);
    }

    private PageHeader parseBaseHeader(ByteBuffer data) {
        return PageHeader.parse(data);
    }

    private void parseAndSetSlots(ByteBuffer data) {
        List<Slot> slots = new ArrayList<>();
        for (short i = 0; i < this.pageHeader.getNumberOfSlots(); i++) {
            short offset = data.getShort();
            short length = data.getShort();
            slots.add(Slot.create(i, offset, length));
        }
        this.slots = slots;
    }

    private void setData(ByteBuffer data) {
        data.clear();
        this.data = data;
    }

    protected int headerSize() {
        return PageHeader.SIZE;
    }

    protected int slotDirectoryOffset(int slotIndex) {
        return headerSize() + (slotIndex * Slot.SERIALIZED_SIZE);
    }

    protected void parseExtendedHeader(ByteBuffer data) {
        // no-op by default; subclasses override to read their extra fields
    }

    protected Tuple getTuple(short slotOffset) {
        Slot slot = slots.stream().filter(s -> s.getOffset() == slotOffset).findFirst().orElse(null);
        if (slot == null) {
            throw new IllegalArgumentException(slotOffset + " is not a valid slot");
        }
        byte[] tupleBytes = new byte[slot.getLength()];
        data.get(slot.getOffset(), tupleBytes);
        return new Tuple(tupleBytes);
    }

    protected Tuple getTuple(Slot slot) {
        byte[] tupleBytes = new byte[slot.getLength()];
        data.get(slot.getOffset(), tupleBytes);
        return new Tuple(tupleBytes);
    }

    protected int lowestTupleOffset() {
        return slots.stream()
                .mapToInt(Slot::getOffset)
                .min()
                .orElse(BlockManager.BLOCK_SIZE);
    }

    protected Slot insertTupleRaw(Tuple tuple) {
        int lastOffsetBase = lowestTupleOffset();
        Slot newSlot = Slot.create((short) slots.size(), (short) (lastOffsetBase - tuple.size()), (short) tuple.size());
        slots.add(newSlot);
        pageHeader.setNumberOfSlots((short) slots.size());
        data.putShort(PageHeader.NUMBER_OF_SLOTS_OFFSET, pageHeader.getNumberOfSlots());
        data.put(slotDirectoryOffset(slots.size() - 1), newSlot.getBytes());
        data.put(newSlot.getOffset(), tuple.getBuffer().array());
        return newSlot;
    }

    protected void clearTuples() {
        slots.clear();
        pageHeader.setNumberOfSlots((short) 0);
        data.putShort(PageHeader.NUMBER_OF_SLOTS_OFFSET, pageHeader.getNumberOfSlots());
    }

    protected Tuple getTupleBySlotIndex(int slotIndex) {
        Slot slot = slots.get(slotIndex);
        if (slot == null) {
            throw new IllegalArgumentException(slotIndex + " is not a valid slot index");
        }
        byte[] tupleBytes = new byte[slot.getLength()];
        data.get(slot.getOffset(), tupleBytes);
        return new Tuple(tupleBytes);
    }

    public int getFreeSpace() {
        int endOfSlotArray = headerSize() + (slots.size() * Slot.SERIALIZED_SIZE);
        return lowestTupleOffset() - endOfSlotArray;
    }

    /**
     * Replaces the tuple at the given slot with a new one of equal size.
     */
    public void updateTuple(Slot slot, Tuple tuple) {
        if (tuple.size() != slot.getLength()) {
            throw new IllegalArgumentException("In-place update requires equal-size tuple");
        }
        data.put(slot.getOffset(), tuple.getBuffer().array());
    }
}
