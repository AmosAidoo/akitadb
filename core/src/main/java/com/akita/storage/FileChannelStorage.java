package com.akita.storage;

import com.akita.buffer.PageId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class FileChannelStorage implements Storage {
    private final Path basePath;

    private FileChannelStorage(Path basePath) {
        this.basePath = basePath;
    }

    public static FileChannelStorage open(Path basePath) throws IOException {
        Files.createDirectories(basePath);
        return new FileChannelStorage(basePath);
    }

    @Override
    public ContainerId createContainer() throws IOException {
        ContainerId containerId = ContainerId.generate();
        try (FileChannel ignored = FileChannel.open(
                path(containerId),
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE_NEW
        )) {
            return containerId;
        }
    }

    @Override
    public void allocate(PageId pageId) throws IOException {
        try (FileChannel channel = FileChannel.open(
                path(pageId.containerId()),
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE
        )) {
            allocate(channel, pageId.blockNumber());
        }
    }

    @Override
    public ByteBuffer read(PageId pageId) throws IOException {
        ByteBuffer page = ByteBuffer.allocate(PAGE_SIZE);
        try (FileChannel channel = FileChannel.open(path(pageId.containerId()), StandardOpenOption.READ)) {
            readFully(channel, page, pageId.blockNumber() * PAGE_SIZE);
        }
        page.clear();
        return page;
    }

    @Override
    public void write(PageId pageId, ByteBuffer page) throws IOException {
        ByteBuffer toWrite = page.duplicate();
        toWrite.clear();
        validatePageBuffer(toWrite);

        try (FileChannel channel = FileChannel.open(
                path(pageId.containerId()),
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE
        )) {
            allocate(channel, pageId.blockNumber());
            writeFully(channel, toWrite, pageId.blockNumber() * PAGE_SIZE);
        }
    }

    @Override
    public long size(ContainerId containerId) throws IOException {
        try (FileChannel channel = FileChannel.open(path(containerId), StandardOpenOption.READ)) {
            return channel.size();
        }
    }

    private Path path(ContainerId containerId) {
        return basePath.resolve(containerId.toString());
    }

    private void allocate(FileChannel channel, long blockNumber) throws IOException {
        long numberOfPages = channel.size() / PAGE_SIZE;
        ByteBuffer zeroPage = ByteBuffer.allocate(PAGE_SIZE);
        while (numberOfPages <= blockNumber) {
            writeFully(channel, zeroPage, numberOfPages * PAGE_SIZE);
            zeroPage.clear();
            numberOfPages++;
        }
    }

    private void validatePageBuffer(ByteBuffer page) {
        if (page.capacity() != PAGE_SIZE) {
            throw new IllegalArgumentException("Page buffer must be exactly PAGE_SIZE bytes");
        }
    }

    private static void readFully(FileChannel channel, ByteBuffer page, long offset) throws IOException {
        int bytesRead;
        do {
            bytesRead = channel.read(page, offset);
            offset += Math.max(bytesRead, 0);
        } while (bytesRead != -1 && page.hasRemaining());
    }

    private static void writeFully(FileChannel channel, ByteBuffer page, long offset) throws IOException {
        while (page.hasRemaining()) {
            int bytesWritten = channel.write(page, offset);
            offset += bytesWritten;
        }
    }
}
