package com.akita.query.execution;

import com.akita.buffer.BufferPoolManager;
import com.akita.buffer.PageId;
import com.akita.catalog.TableMetadata;
import com.akita.datatype.AkitaType;
import com.akita.datatype.AkitaValue;
import com.akita.datatype.ColumnMetadata;
import com.akita.datatype.Schema;
import com.akita.heap.HeapFileHeader;
import com.akita.heap.ObjectType;
import com.akita.page.PageHeader;
import com.akita.query.bind.BoundBinaryExpression;
import com.akita.query.bind.BoundColumnReference;
import com.akita.query.bind.BoundLiteral;
import com.akita.query.bind.BoundSelectItem;
import com.akita.query.bind.BoundTable;
import com.akita.query.physical.FilterPlan;
import com.akita.query.physical.PhysicalPlan;
import com.akita.query.physical.ProjectionPlan;
import com.akita.query.physical.SeqScanPlan;
import com.akita.sql.ast.BinaryOperator;
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
import java.util.ArrayDeque;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(AkitaExtension.class)
class ExecutorTest {

    @Test
    void seqScanReturnsAllRowsFromOneTable(
            BufferPoolManager bpm,
            FileChannelBlockManager bm,
            FileChannelContainerManager cm
    ) throws Exception {
        Schema schema = usersSchema();
        ContainerId containerId = ContainerFixture.create(bm, cm).createTable();
        RowTupleCodec codec = new RowTupleCodec();
        List<Row> rows = List.of(
                Row.of(new AkitaValue.IntVal(1), new AkitaValue.VarcharVal("Ada"), new AkitaValue.IntVal(42)),
                Row.of(new AkitaValue.IntVal(2), new AkitaValue.VarcharVal("Grace"), new AkitaValue.IntVal(17))
        );

        SlottedPageWriter.create(bm)
                .addPageDirectoryTuple(1, BlockManager.BLOCK_SIZE - PageHeader.SIZE)
                .writeTo(new PageId(containerId, 0), tableHeader(), pageDirectoryHeader());
        SlottedPageWriter.create(bm)
                .addTuple(codec.encode(rows.get(0), schema))
                .addTuple(codec.encode(rows.get(1), schema))
                .writeTo(new PageId(containerId, 1), null);

        TableMetadata metadata = new TableMetadata("users", containerId, schema);
        SeqScanExecutor executor = new SeqScanExecutor(
                new ExecutionContext(bpm),
                new SeqScanPlan(new BoundTable(metadata, null))
        );

        assertThat(executor.next()).contains(rows.get(0));
        assertThat(executor.next()).contains(rows.get(1));
        assertThat(executor.next()).isEmpty();
    }

    @Test
    void filterSkipsRowsThatDoNotMatchPredicate() throws Exception {
        FilterExecutor executor = new FilterExecutor(
                rows(
                        Row.of(new AkitaValue.IntVal(17)),
                        Row.of(new AkitaValue.IntVal(42))
                ),
                new FilterPlan(inputPlan(usersSchema()), new BoundBinaryExpression(
                        column("age", 0, new AkitaType.Integer()),
                        BinaryOperator.GREATER_THAN,
                        new BoundLiteral(18, new AkitaType.Integer()),
                        new AkitaType.Boolean()
                ))
        );

        assertThat(executor.next()).contains(Row.of(new AkitaValue.IntVal(42)));
        assertThat(executor.next()).isEmpty();
    }

    @Test
    void projectionReturnsOnlyRequestedColumns() throws Exception {
        ProjectionExecutor executor = new ProjectionExecutor(
                rows(Row.of(
                        new AkitaValue.IntVal(1),
                        new AkitaValue.VarcharVal("Ada"),
                        new AkitaValue.IntVal(42)
                )),
                new ProjectionPlan(
                        inputPlan(usersSchema()),
                        List.of(
                                new BoundSelectItem(column("name", 1, new AkitaType.Varchar(255)), "name"),
                                new BoundSelectItem(column("age", 2, new AkitaType.Integer()), "age")
                        ),
                        new Schema(List.of(
                                new ColumnMetadata("name", new AkitaType.Varchar(255), 0, false),
                                new ColumnMetadata("age", new AkitaType.Integer(), 1, false)
                        ))
                )
        );

        assertThat(executor.next()).contains(Row.of(
                new AkitaValue.VarcharVal("Ada"),
                new AkitaValue.IntVal(42)
        ));
        assertThat(executor.next()).isEmpty();
    }

    private static Executor rows(Row... rows) {
        Queue<Row> queue = new ArrayDeque<>(List.of(rows));
        return () -> Optional.ofNullable(queue.poll());
    }

    private static PhysicalPlan inputPlan(Schema schema) {
        TableMetadata metadata = new TableMetadata("input", ContainerId.generate(), schema);
        return new SeqScanPlan(new BoundTable(metadata, null));
    }

    private static BoundColumnReference column(String name, int ordinalPosition, AkitaType type) {
        return new BoundColumnReference(null, name, ordinalPosition, type);
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
