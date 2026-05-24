package com.akita.query;

import com.akita.buffer.BufferPoolManager;
import com.akita.buffer.PageId;
import com.akita.catalog.JsonCatalog;
import com.akita.catalog.TableMetadata;
import com.akita.datatype.AkitaType;
import com.akita.datatype.AkitaValue;
import com.akita.datatype.ColumnMetadata;
import com.akita.datatype.Schema;
import com.akita.heap.HeapFileHeader;
import com.akita.heap.ObjectType;
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
    void executesSingleTableSelectEndToEnd(
            BufferPoolManager bpm,
            FileChannelBlockManager bm,
            FileChannelContainerManager cm
    ) throws Exception {
        Schema schema = usersSchema();
        ContainerId containerId = ContainerFixture.create(bm, cm).createTable();
        writeRows(bm, containerId, schema, List.of(
                Row.of(new AkitaValue.IntVal(1), new AkitaValue.VarcharVal("Ada"), new AkitaValue.IntVal(42)),
                Row.of(new AkitaValue.IntVal(2), new AkitaValue.VarcharVal("Grace"), new AkitaValue.IntVal(17)),
                Row.of(new AkitaValue.IntVal(3), new AkitaValue.VarcharVal("Edsger"), new AkitaValue.IntVal(32))
        ));

        JsonCatalog catalog = new JsonCatalog();
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
        writeRows(bm, containerId, schema, List.of(
                Row.of(new AkitaValue.IntVal(1), new AkitaValue.VarcharVal("Ada"), new AkitaValue.IntVal(42)),
                Row.of(new AkitaValue.IntVal(2), new AkitaValue.VarcharVal("Grace"), new AkitaValue.IntVal(17))
        ));

        JsonCatalog catalog = new JsonCatalog();
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
    void surfacesUnknownTableAsBindError(BufferPoolManager bpm) {
        QueryEngine queryEngine = new QueryEngine(new JsonCatalog(), bpm);

        assertThatThrownBy(() -> queryEngine.execute("SELECT id FROM missing"))
                .isInstanceOfSatisfying(QueryException.class, exception -> {
                    assertThat(exception.kind()).isEqualTo(QueryException.Kind.BIND);
                    assertThat(exception).hasMessage("Unknown table: missing");
                });
    }

    @Test
    void surfacesUnknownColumnAsBindError(BufferPoolManager bpm) {
        JsonCatalog catalog = new JsonCatalog();
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
        QueryEngine queryEngine = new QueryEngine(new JsonCatalog(), bpm);

        assertThatThrownBy(() -> queryEngine.execute("SELECT FROM users"))
                .isInstanceOfSatisfying(QueryException.class, exception -> {
                    assertThat(exception.kind()).isEqualTo(QueryException.Kind.PARSE);
                    assertThat(exception).hasMessageContaining("Expected expression");
                });
    }

    private static void writeRows(
            FileChannelBlockManager bm,
            ContainerId containerId,
            Schema schema,
            List<Row> rows
    ) throws Exception {
        RowTupleCodec codec = new RowTupleCodec();
        SlottedPageWriter writer = SlottedPageWriter.create(bm);
        for (Row row : rows) {
            writer.addTuple(codec.encode(row, schema));
        }
        writer.writeTo(new PageId(containerId, 1), null);
        SlottedPageWriter.create(bm)
                .addPageDirectoryTuple(1, BlockManager.BLOCK_SIZE - PageHeader.SIZE)
                .writeTo(new PageId(containerId, 0), tableHeader(), pageDirectoryHeader());
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
