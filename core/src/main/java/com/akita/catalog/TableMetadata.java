package com.akita.catalog;

import com.akita.datatype.Schema;
import com.akita.storage.ContainerId;

public record TableMetadata(
        String tableName,
        ContainerId containerId,
        Schema schema
) {}
