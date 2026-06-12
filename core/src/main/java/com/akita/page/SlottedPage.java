package com.akita.page;

import com.akita.storage.Storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * This is a generic slotted page implementation.
 * More specialized slotted pages will extend it and add additional features
 */
public abstract class SlottedPage {
    protected PageHeader pageHeader;

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
            slots.add(Slot.create(offset, length));
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

    protected int numberOfSlotsOffset() {
        return PageHeader.NUMBER_OF_SLOTS_OFFSET;
    }

    protected int slotDirectoryOffset(int slotIndex) {
        return headerSize() + (slotIndex * Slot.SERIALIZED_SIZE);
    }

    protected void parseExtendedHeader(ByteBuffer data) {
        // no-op by default; subclasses override to read their extra fields
    }

    public List<Slot> slots() {
        return Collections.unmodifiableList(slots);
    }

    public int tupleCount() {
        return slots.size();
    }

    public Slot slotAt(int slotIndex) {
        validateSlotIndex(slotIndex);
        return slots.get(slotIndex);
    }

    public Tuple tupleAt(int slotIndex) {
        return tupleFor(slotAt(slotIndex));
    }

    protected int lowestTupleOffset() {
        return slots.stream()
                .mapToInt(Slot::getOffset)
                .min()
                .orElse(Storage.PAGE_SIZE);
    }

    protected short appendTuple(Tuple tuple) {
        validateAppend(tuple);
        short slotIndex = appendSlotMetadata(tuple, true);
        Slot newSlot = slots.get(slotIndex);
        writeSlot(slotIndex);
        data.put(newSlot.getOffset(), tuple.getBuffer().array());
        return slotIndex;
    }

    protected short cacheAppendedTuple(Tuple tuple) {
        validateAppend(tuple);
        return appendSlotMetadata(tuple, false);
    }

    private short appendSlotMetadata(Tuple tuple, boolean writeHeader) {
        int lastOffsetBase = lowestTupleOffset();
        short slotIndex = (short) slots.size();
        Slot newSlot = Slot.create((short) (lastOffsetBase - tuple.size()), (short) tuple.size());
        slots.add(newSlot);
        pageHeader.setNumberOfSlots((short) slots.size());
        if (writeHeader) {
            data.putShort(numberOfSlotsOffset(), pageHeader.getNumberOfSlots());
        }
        return slotIndex;
    }

    protected void replaceTuplesRaw(List<Tuple> tuples) {
        clearTuples();
        for (Tuple tuple : tuples) {
            appendTuple(tuple);
        }
    }

    protected void sortSlotsByTuple(Comparator<Tuple> comparator) {
        slots.sort((s1, s2) -> comparator.compare(tupleFor(s1), tupleFor(s2)));
        writeSlots();
    }

    private void clearTuples() {
        slots.clear();
        pageHeader.setNumberOfSlots((short) 0);
        data.putShort(numberOfSlotsOffset(), pageHeader.getNumberOfSlots());
    }

    private Tuple tupleFor(Slot slot) {
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
    public void updateTuple(int slotIndex, Tuple tuple) {
        Slot slot = slotAt(slotIndex);
        if (tuple.size() != slot.getLength()) {
            throw new IllegalArgumentException("In-place update requires equal-size tuple");
        }
        data.put(slot.getOffset(), tuple.getBuffer().array());
    }

    private void validateSlotIndex(int slotIndex) {
        if (slotIndex < 0 || slotIndex >= slots.size()) {
            throw new IllegalArgumentException(slotIndex + " is not a valid slot index");
        }
    }

    private void validateAppend(Tuple tuple) {
        if (slots.size() >= Short.MAX_VALUE) {
            throw new IllegalStateException("Slotted page cannot contain more than " + Short.MAX_VALUE + " tuples");
        }
        if (tuple.serializedSize() > getFreeSpace()) {
            throw new IllegalArgumentException("Tuple requires " + tuple.serializedSize()
                    + " bytes but page has only " + getFreeSpace() + " bytes free");
        }
    }

    private void writeSlots() {
        for (int i = 0; i < slots.size(); i++) {
            writeSlot(i);
        }
    }

    private void writeSlot(int slotIndex) {
        data.put(slotDirectoryOffset(slotIndex), slots.get(slotIndex).getBytes());
    }
}
