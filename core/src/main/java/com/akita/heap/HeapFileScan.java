package com.akita.heap;

import com.akita.buffer.BufferPoolManager;
import com.akita.page.PageDirectory;
import com.akita.page.Slot;
import com.akita.page.Tuple;

import java.util.List;
import java.util.Optional;

public class HeapFileScan implements AutoCloseable {
    private final BufferPoolManager bufferPoolManager;
    private PageDirectory currentDirectory;
    private int directorySlotIndex;
    private HeapPage currentHeapPage;
    private List<Slot> currentTupleSlots = List.of();
    private int tupleSlotIndex;

    HeapFileScan(PageDirectory firstDirectory, BufferPoolManager bufferPoolManager) {
        this.currentDirectory = firstDirectory;
        this.bufferPoolManager = bufferPoolManager;
    }

    public Optional<Tuple> next() throws Exception {
        while (true) {
            if (currentHeapPage != null && tupleSlotIndex < currentTupleSlots.size()) {
                Slot slot = currentTupleSlots.get(tupleSlotIndex++);
                return Optional.of(currentHeapPage.getTuple(slot.getOffset()));
            }

            closeCurrentHeapPage();
            if (!openNextHeapPage()) {
                return Optional.empty();
            }
        }
    }

    private boolean openNextHeapPage() throws Exception {
        while (currentDirectory != null) {
            List<Slot> directorySlots = currentDirectory.getSlots();
            if (directorySlotIndex < directorySlots.size()) {
                Slot directorySlot = directorySlots.get(directorySlotIndex++);
                currentHeapPage = HeapPage.create(
                        bufferPoolManager.readPage(currentDirectory.dataPageId(directorySlot))
                );
                currentTupleSlots = currentHeapPage.getSlots();
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
            currentTupleSlots = List.of();
            tupleSlotIndex = 0;
        }
    }

    @Override
    public void close() throws Exception {
        closeCurrentHeapPage();
    }
}
