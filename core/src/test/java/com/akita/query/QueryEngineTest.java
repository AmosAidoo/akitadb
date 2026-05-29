package com.akita.query;

import com.akita.buffer.BufferPoolManager;
import com.akita.buffer.PageId;
import com.akita.catalog.Catalog;
import com.akita.catalog.JsonCatalog;
import com.akita.catalog.TableMetadata;
import com.akita.datatype.AkitaType;
import com.akita.datatype.AkitaValue;
import com.akita.datatype.ColumnMetadata;
import com.akita.datatype.Schema;
import com.akita.heap.HeapFile;
import com.akita.heap.HeapFileHeader;
import com.akita.heap.ObjectType;
import com.akita.page.PageDirectory;
import com.akita.page.Tuple;
import com.akita.page.PageHeader;
import com.akita.query.execution.Row;
import com.akita.query.execution.RowTupleCodec;
import com.akita.storage.BlockManager;
import com.akita.storage.ContainerId;
import com.akita.storage.FileChannelBlockManager;
import com.akita.storage.FileChannelContainerManager;
import com.akita.testing.AkitaExtension;
import com.akita.testing.ContainerFixture;
import com.akita.testing.SlottedPageWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.ByteBuffer;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(AkitaExtension.class)
class QueryEngineTest {

    @Test
    void createsTableMetadataAndInitialHeapPage(
            BufferPoolManager bpm,
            FileChannelBlockManager bm
    ) throws Exception {
        Catalog catalog = catalog();

        QueryResult result = new QueryEngine(catalog, bpm).execute("""
                CREATE TABLE users (
                    id INTEGER NOT NULL,
                    name VARCHAR(32),
                    active BOOLEAN
                )
                """);

        assertThat(result.schema().columns()).isEmpty();
        assertThat(result.rows()).isEmpty();

        TableMetadata table = catalog.getTable("users");
        assertThat(table).isNotNull();
        assertThat(table.schema().columns()).containsExactly(
                new ColumnMetadata("id", new AkitaType.Integer(), 0, false),
                new ColumnMetadata("name", new AkitaType.Varchar(32), 1, true),
                new ColumnMetadata("active", new AkitaType.Boolean(), 2, true)
        );

        ByteBuffer page = ByteBuffer.allocate(BlockManager.BLOCK_SIZE);
        bm.readBlock(table.containerId(), PageDirectory.FIRST_PAGE_DIRECTORY_NUMBER, page);
        page.clear();
        assertThat(page.getInt()).isEqualTo(0);
        assertThat(page.getShort()).isEqualTo((short) 0);
    }

    @Test
    void executesSingleTableSelectEndToEnd(
            BufferPoolManager bpm,
            FileChannelBlockManager bm,
            FileChannelContainerManager cm
    ) throws Exception {
        Schema schema = usersSchema();
        ContainerId containerId = ContainerFixture.create(bm, cm).createTable();
        writeRows(bpm, bm, containerId, schema, List.of(
                Row.of(new AkitaValue.IntVal(1), new AkitaValue.VarcharVal("Ada"), new AkitaValue.IntVal(42)),
                Row.of(new AkitaValue.IntVal(2), new AkitaValue.VarcharVal("Grace"), new AkitaValue.IntVal(17)),
                Row.of(new AkitaValue.IntVal(3), new AkitaValue.VarcharVal("Edsger"), new AkitaValue.IntVal(32))
        ));

        Catalog catalog = catalog();
        catalog.createTable(new TableMetadata("users", containerId, schema));

        QueryResult result = new QueryEngine(catalog, bpm)
                .execute("SELECT id, name FROM users WHERE age > 18");

        assertThat(result.schema().columns())
                .extracting(ColumnMetadata::name)
                .containsExactly("id", "name");
        assertThat(result.rows()).containsExactly(
                Row.of(new AkitaValue.IntVal(1), new AkitaValue.VarcharVal("Ada")),
                Row.of(new AkitaValue.IntVal(3), new AkitaValue.VarcharVal("Edsger"))
        );
    }

    @Test
    void queryReturnsCursorForIteratorStyleRowAccess(
            BufferPoolManager bpm,
            FileChannelBlockManager bm,
            FileChannelContainerManager cm
    ) throws Exception {
        Schema schema = usersSchema();
        ContainerId containerId = ContainerFixture.create(bm, cm).createTable();
        writeRows(bpm, bm, containerId, schema, List.of(
                Row.of(new AkitaValue.IntVal(1), new AkitaValue.VarcharVal("Ada"), new AkitaValue.IntVal(42)),
                Row.of(new AkitaValue.IntVal(2), new AkitaValue.VarcharVal("Grace"), new AkitaValue.IntVal(17))
        ));

        Catalog catalog = catalog();
        catalog.createTable(new TableMetadata("users", containerId, schema));

        try (QueryCursor cursor = new QueryEngine(catalog, bpm).query("SELECT name FROM users")) {
            assertThat(cursor.schema().columns())
                    .extracting(ColumnMetadata::name)
                    .containsExactly("name");
            assertThat(cursor.next()).contains(Row.of(new AkitaValue.VarcharVal("Ada")));
            assertThat(cursor.next()).contains(Row.of(new AkitaValue.VarcharVal("Grace")));
            assertThat(cursor.next()).isEmpty();
        }
    }

    @Test
    void executesInsertEndToEnd(BufferPoolManager bpm) {
        Catalog catalog = catalog();
        QueryEngine queryEngine = new QueryEngine(catalog, bpm);
        queryEngine.execute("""
                CREATE TABLE users (
                    id INTEGER NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    age INTEGER NOT NULL
                )
                """);

        QueryResult insertResult = queryEngine.execute("""
                INSERT INTO users (name, age, id)
                VALUES ('Ada', 42, 1), ('Grace', 17, 2)
                """);

        assertThat(insertResult.schema().columns()).isEmpty();
        assertThat(insertResult.rows()).isEmpty();
        QueryResult selectResult = queryEngine.execute("SELECT id, name FROM users WHERE age > 18");
        assertThat(selectResult.rows()).containsExactly(
                Row.of(new AkitaValue.IntVal(1), new AkitaValue.VarcharVal("Ada"))
        );
    }

    @Test
    void surfacesInsertTypeMismatchAsBindError(BufferPoolManager bpm) {
        QueryEngine queryEngine = new QueryEngine(catalog(), bpm);
        queryEngine.execute("CREATE TABLE users (id INTEGER NOT NULL, name VARCHAR(8) NOT NULL)");

        assertThatThrownBy(() -> queryEngine.execute("INSERT INTO users VALUES ('not-int', 'Ada')"))
                .isInstanceOfSatisfying(QueryException.class, exception -> {
                    assertThat(exception.kind()).isEqualTo(QueryException.Kind.BIND);
                    assertThat(exception).hasMessageContaining("Value for column id");
                });
    }

    @Test
    void surfacesUnknownTableAsBindError(BufferPoolManager bpm) {
        QueryEngine queryEngine = new QueryEngine(catalog(), bpm);

        assertThatThrownBy(() -> queryEngine.execute("SELECT id FROM missing"))
                .isInstanceOfSatisfying(QueryException.class, exception -> {
                    assertThat(exception.kind()).isEqualTo(QueryException.Kind.BIND);
                    assertThat(exception).hasMessage("Unknown table: missing");
                });
    }

    @Test
    void surfacesUnknownColumnAsBindError(BufferPoolManager bpm) {
        Catalog catalog = catalog();
        catalog.createTable(new TableMetadata("users", ContainerId.generate(), usersSchema()));
        QueryEngine queryEngine = new QueryEngine(catalog, bpm);

        assertThatThrownBy(() -> queryEngine.execute("SELECT nope FROM users"))
                .isInstanceOfSatisfying(QueryException.class, exception -> {
                    assertThat(exception.kind()).isEqualTo(QueryException.Kind.BIND);
                    assertThat(exception).hasMessageContaining("Unknown column");
                });
    }

    @Test
    void surfacesInvalidSqlAsParseError(BufferPoolManager bpm) {
        QueryEngine queryEngine = new QueryEngine(catalog(), bpm);

        assertThatThrownBy(() -> queryEngine.execute("SELECT FROM users"))
                .isInstanceOfSatisfying(QueryException.class, exception -> {
                    assertThat(exception.kind()).isEqualTo(QueryException.Kind.PARSE);
                    assertThat(exception).hasMessageContaining("Expected expression");
                });
    }

    private static void writeRows(
            BufferPoolManager bpm,
            FileChannelBlockManager bm,
            ContainerId containerId,
            Schema schema,
            List<Row> rows
    ) throws Exception {
        SlottedPageWriter.create(bm)
                .addPageDirectoryTuple(1, BlockManager.BLOCK_SIZE - PageHeader.SIZE)
                .writeTo(new PageId(containerId, 0), tableHeader(), pageDirectoryHeader());

        RowTupleCodec codec = new RowTupleCodec();
        HeapFile heapFile = HeapFile.open(containerId, bpm);
        for (Row row : rows) {
            Tuple tuple = codec.encode(row, schema);
            heapFile.insertTuple(tuple);
        }
    }

    private static Catalog catalog() {
        return new JsonCatalog();
    }

    private static Schema usersSchema() {
        return new Schema(List.of(
                new ColumnMetadata("id", new AkitaType.Integer(), 0, false),
                new ColumnMetadata("name", new AkitaType.Varchar(255), 1, false),
                new ColumnMetadata("age", new AkitaType.Integer(), 2, false)
        ));
    }

    private static ByteBuffer tableHeader() {
        ByteBuffer buffer = ByteBuffer.allocate(HeapFileHeader.SIZE);
        HeapFileHeader.write(buffer, ObjectType.TABLE);
        return buffer;
    }

    private static ByteBuffer pageDirectoryHeader() {
        ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES);
        buffer.putShort((short) 0);
        return buffer;
    }
}
