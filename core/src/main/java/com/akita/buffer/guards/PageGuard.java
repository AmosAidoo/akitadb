package com.akita.buffer.guards;

import com.akita.buffer.PageId;

import java.nio.ByteBuffer;

public interface PageGuard extends AutoCloseable {

    PageId getPageId();

    ByteBuffer getData();
}
