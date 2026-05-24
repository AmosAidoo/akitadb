package com.akita.query.execution;

import com.akita.datatype.AkitaType;
import com.akita.datatype.AkitaValue;
import com.akita.datatype.ColumnMetadata;
import com.akita.datatype.Schema;
import com.akita.page.Tuple;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RowTupleCodecTest {

    @Test
    void decodesTupleUsingSchemaOrdinals() {
        Tuple tuple = Tuple.builder(21)
                .writeInt(1)
                .writeVarchar("Ada")
                .writeInt(42)
                .writeBoolean(true)
                .build();

        Row row = new RowTupleCodec().decode(tuple, usersSchema());

        assertThat(row).containsExactly(
                new AkitaValue.IntVal(1),
                new AkitaValue.VarcharVal("Ada"),
                new AkitaValue.IntVal(42),
                new AkitaValue.BoolVal(true)
        );
    }

    @Test
    void encodesRowsForTupleBasedTests() {
        RowTupleCodec codec = new RowTupleCodec();
        Row row = Row.of(
                new AkitaValue.IntVal(1),
                new AkitaValue.VarcharVal("Ada"),
                new AkitaValue.IntVal(42),
                new AkitaValue.BoolVal(true)
        );

        Row decoded = codec.decode(codec.encode(row, usersSchema()), usersSchema());

        assertThat(decoded.values()).isEqualTo(row.values());
    }

    @Test
    void rejectsTupleNullEncodingForMilestoneOne() {
        Row row = Row.of(
                new AkitaValue.IntVal(1),
                new AkitaValue.VarcharVal("Ada"),
                new AkitaValue.Null(),
                new AkitaValue.BoolVal(true)
        );

        assertThatThrownBy(() -> new RowTupleCodec().encode(row, usersSchema()))
                .isInstanceOf(ExpressionEvaluationException.class)
                .hasMessage("Tuple null encoding is not supported yet");
    }

    private static Schema usersSchema() {
        return new Schema(List.of(
                new ColumnMetadata("id", new AkitaType.Integer(), 0, false),
                new ColumnMetadata("name", new AkitaType.Varchar(255), 1, false),
                new ColumnMetadata("age", new AkitaType.Integer(), 2, true),
                new ColumnMetadata("active", new AkitaType.Boolean(), 3, false)
        ));
    }
}
