package com.akita.catalog;

import com.akita.datatype.Schema;
import com.akita.storage.ContainerId;

public record IndexMetadata(
        String indexName,
        String tableName,
        Schema schema,
        ContainerId containerId,
        boolean unique
) {
}
