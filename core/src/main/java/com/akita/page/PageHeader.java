package com.akita.page;

import java.nio.ByteBuffer;

public class PageHeader {
    public static final int SIZE = Short.BYTES;

    short numberOfSlots;
    // PageType pageType;

    private PageHeader(short numberOfSlots) {
        this.numberOfSlots = numberOfSlots;
    }

    public static PageHeader parse(ByteBuffer data) {
        short numberOfSlots = data.getShort();
        return new PageHeader(numberOfSlots);
    }
}
