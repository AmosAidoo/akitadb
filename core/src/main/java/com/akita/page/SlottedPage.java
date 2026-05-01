package com.akita.page;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * This is a generic slotted page implementation.
 * More specialized slotted pages will extend it and add additional features
 */
public abstract class SlottedPage {
    protected PageHeader pageHeader;
    protected List<Slot> slots;
    protected ByteBuffer data;

    protected final void parsePage(ByteBuffer data) {
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
        for (short i = 0; i < this.pageHeader.numberOfSlots; i++) {
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

    protected void parseExtendedHeader(ByteBuffer data) {
        // no-op by default; subclasses override to read their extra fields
    }

    public Tuple getTuple(Slot slot) {
        byte[] tupleBytes = new byte[slot.getLength()];
        data.get(slot.getOffset(), tupleBytes);
        return new Tuple(tupleBytes);
    }

    public ByteBuffer toByteBuffer() {
        return null;
    }
}
