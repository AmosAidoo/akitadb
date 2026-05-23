package com.akita.page;

import java.nio.ByteBuffer;

public class PageHeader {
    public static final int SIZE = Short.BYTES;
    public static final int NUMBER_OF_SLOTS_OFFSET = 0;

    private short numberOfSlots;
    // PageType pageType;

    private PageHeader(short numberOfSlots) {
        this.numberOfSlots = numberOfSlots;
    }

    public static PageHeader parse(ByteBuffer data) {
        short numberOfSlots = data.getShort();
        return new PageHeader(numberOfSlots);
    }

    public short getNumberOfSlots() {
        return numberOfSlots;
    }

    public void setNumberOfSlots(short numberOfSlots) {
        this.numberOfSlots = numberOfSlots;
    }
}
