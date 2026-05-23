package com.akita.catalog;

public interface Catalog {

    void createTable(TableMetadata table);

    void dropTable(TableMetadata table);

    TableMetadata getTable(String tableName);
}
