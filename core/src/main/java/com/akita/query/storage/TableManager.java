package com.akita.query.storage;

import com.akita.buffer.BufferPoolManager;
import com.akita.buffer.PageId;
import com.akita.catalog.Catalog;
import com.akita.catalog.TableMetadata;
import com.akita.datatype.AkitaTypes;
import com.akita.datatype.ColumnMetadata;
import com.akita.datatype.Schema;
import com.akita.heap.HeapFileHeader;
import com.akita.heap.ObjectType;
import com.akita.page.PageDirectory;
import com.akita.sql.ast.ColumnDefinition;
import com.akita.sql.ast.QualifiedName;
import com.akita.storage.ContainerId;

import java.util.ArrayList;
import java.util.List;

public class TableManager {
    private final Catalog catalog;
    private final BufferPoolManager bufferPoolManager;

    public TableManager(Catalog catalog, BufferPoolManager bufferPoolManager) {
        this.catalog = catalog;
        this.bufferPoolManager = bufferPoolManager;
    }

    public void createTable(QualifiedName name, List<ColumnDefinition> columns) throws Exception {
        String tableName = tableName(name);
        if (catalog.getTable(tableName) != null) {
            throw new IllegalArgumentException("table already exists: " + tableName);
        }

        ContainerId containerId = ContainerId.generate();
        PageId headerPageId = new PageId(containerId, PageDirectory.FIRST_PAGE_DIRECTORY_NUMBER);
        try (var page = bufferPoolManager.allocatePage(headerPageId)) {
            HeapFileHeader.write(page.getData(), ObjectType.TABLE);
            page.getData().putShort((short) 0);
        }
        bufferPoolManager.flushPage(headerPageId);

        catalog.createTable(new TableMetadata(tableName, containerId, schema(columns)));
    }

    private static Schema schema(List<ColumnDefinition> columns) {
        List<ColumnMetadata> metadata = new ArrayList<>();
        for (int ordinal = 0; ordinal < columns.size(); ordinal++) {
            ColumnDefinition column = columns.get(ordinal);
            metadata.add(new ColumnMetadata(column.name(), AkitaTypes.fromSqlType(column.type()), ordinal, column.nullable()));
        }
        return new Schema(metadata);
    }

    private static String tableName(QualifiedName name) {
        List<String> parts = name.parts();
        if (parts.size() != 1) {
            throw new IllegalArgumentException("Table names must be unqualified: " + String.join(".", parts));
        }
        return parts.getFirst();
    }
}
