package com.akita.query.storage;

import com.akita.catalog.TableMetadata;
import com.akita.heap.HeapFileScan;
import com.akita.page.RecordId;
import com.akita.page.Tuple;

public interface TableAccess {
    HeapFileScan scan(TableMetadata table) throws Exception;

    RecordId insert(TableMetadata table, Tuple tuple) throws Exception;
}
