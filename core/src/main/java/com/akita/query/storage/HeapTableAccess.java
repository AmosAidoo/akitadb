package com.akita.query.storage;

import com.akita.buffer.BufferPoolManager;
import com.akita.catalog.TableMetadata;
import com.akita.heap.HeapFile;
import com.akita.heap.HeapFileScan;
import com.akita.page.RecordId;
import com.akita.page.Tuple;

public class HeapTableAccess implements TableAccess {
    private final BufferPoolManager bufferPoolManager;

    public HeapTableAccess(BufferPoolManager bufferPoolManager) {
        this.bufferPoolManager = bufferPoolManager;
    }

    @Override
    public HeapFileScan scan(TableMetadata table) throws Exception {
        return HeapFile.open(table.containerId(), bufferPoolManager).scanTuples();
    }

    @Override
    public RecordId insert(TableMetadata table, Tuple tuple) throws Exception {
        return HeapFile.open(table.containerId(), bufferPoolManager).insertTuple(tuple);
    }
}
