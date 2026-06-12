package com.akita.heap;

import com.akita.buffer.BufferPoolManager;
import com.akita.page.PageDirectory;
import com.akita.page.Tuple;

import java.util.Optional;

public class HeapFileScan implements AutoCloseable {
    private final BufferPoolManager bufferPoolManager;
    private PageDirectory currentDirectory;
    private int directorySlotIndex;
    private HeapPage currentHeapPage;
    private int currentTupleCount;
    private int tupleSlotIndex;

    HeapFileScan(PageDirectory firstDirectory, BufferPoolManager bufferPoolManager) {
        this.currentDirectory = firstDirectory;
        this.bufferPoolManager = bufferPoolManager;
    }

    public Optional<Tuple> next() throws Exception {
        while (true) {
            if (currentHeapPage != null && tupleSlotIndex < currentTupleCount) {
                return Optional.of(currentHeapPage.getTuple((short) tupleSlotIndex++));
            }

            closeCurrentHeapPage();
            if (!openNextHeapPage()) {
                return Optional.empty();
            }
        }
    }

    private boolean openNextHeapPage() throws Exception {
        while (currentDirectory != null) {
            if (directorySlotIndex < currentDirectory.tupleCount()) {
                currentHeapPage = HeapPage.create(
                        bufferPoolManager.readPage(currentDirectory.dataPageId(directorySlotIndex++))
                );
                currentTupleCount = currentHeapPage.tupleCount();
                tupleSlotIndex = 0;
                return true;
            }

            currentDirectory = currentDirectory.next();
            directorySlotIndex = 0;
        }
        return false;
    }

    private void closeCurrentHeapPage() throws Exception {
        if (currentHeapPage != null) {
            currentHeapPage.close();
            currentHeapPage = null;
            currentTupleCount = 0;
            tupleSlotIndex = 0;
        }
    }

    @Override
    public void close() throws Exception {
        closeCurrentHeapPage();
    }
}
