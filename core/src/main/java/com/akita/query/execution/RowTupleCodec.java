package com.akita.query.execution;

import com.akita.datatype.AkitaType;
import com.akita.datatype.AkitaValue;
import com.akita.datatype.ColumnMetadata;
import com.akita.datatype.Schema;
import com.akita.page.Tuple;

import java.nio.charset.StandardCharsets;

public class RowTupleCodec {

    public Row decode(Tuple tuple, Schema schema) {
        tuple.clear();
        AkitaValue[] values = new AkitaValue[schema.columns().size()];
        for (ColumnMetadata column : schema.columns()) {
            values[column.ordinalPosition()] = readValue(tuple, column.type());
        }
        return new Row(values);
    }

    public Tuple encode(Row row, Schema schema) {
        Tuple.Builder builder = Tuple.builder(encodedSize(row, schema));
        for (ColumnMetadata column : schema.columns()) {
            writeValue(builder, row.get(column.ordinalPosition()), column.type());
        }
        return builder.build();
    }

    private static AkitaValue readValue(Tuple tuple, AkitaType type) {
        return switch (type) {
            case AkitaType.Integer ignored -> new AkitaValue.IntVal(tuple.readInt());
            case AkitaType.BigInt ignored -> new AkitaValue.BigIntVal(tuple.readLong());
            case AkitaType.Double ignored -> new AkitaValue.DoubleVal(tuple.readDouble());
            case AkitaType.Boolean ignored -> new AkitaValue.BoolVal(tuple.readBoolean());
            case AkitaType.Varchar ignored -> {
                int length = tuple.readInt();
                byte[] bytes = new byte[length];
                tuple.readBytes(bytes);
                yield new AkitaValue.VarcharVal(new String(bytes, StandardCharsets.UTF_8));
            }
        };
    }

    private static void writeValue(Tuple.Builder builder, AkitaValue value, AkitaType type) {
        if (value instanceof AkitaValue.Null) {
            throw new ExpressionEvaluationException("Tuple null encoding is not supported yet");
        }
        switch (type) {
            case AkitaType.Integer ignored when value instanceof AkitaValue.IntVal(var intValue) ->
                    builder.writeInt(intValue);
            case AkitaType.BigInt ignored when value instanceof AkitaValue.BigIntVal(var longValue) ->
                    builder.writeLong(longValue);
            case AkitaType.Double ignored when value instanceof AkitaValue.DoubleVal(var doubleValue) ->
                    builder.writeDouble(doubleValue);
            case AkitaType.Boolean ignored when value instanceof AkitaValue.BoolVal(var boolValue) ->
                    builder.writeBoolean(boolValue);
            case AkitaType.Varchar ignored when value instanceof AkitaValue.VarcharVal(var stringValue) ->
                    builder.writeVarchar(stringValue);
            default -> throw new ExpressionEvaluationException("Row value does not match schema type");
        }
    }

    private static int encodedSize(Row row, Schema schema) {
        int size = 0;
        for (ColumnMetadata column : schema.columns()) {
            size += encodedSize(row.get(column.ordinalPosition()), column.type());
        }
        return size;
    }

    private static int encodedSize(AkitaValue value, AkitaType type) {
        if (value instanceof AkitaValue.Null) {
            throw new ExpressionEvaluationException("Tuple null encoding is not supported yet");
        }
        return switch (type) {
            case AkitaType.Integer ignored when value instanceof AkitaValue.IntVal -> Integer.BYTES;
            case AkitaType.BigInt ignored when value instanceof AkitaValue.BigIntVal -> Long.BYTES;
            case AkitaType.Double ignored when value instanceof AkitaValue.DoubleVal -> Double.BYTES;
            case AkitaType.Boolean ignored when value instanceof AkitaValue.BoolVal -> Byte.BYTES;
            case AkitaType.Varchar ignored when value instanceof AkitaValue.VarcharVal(var stringValue) ->
                    Integer.BYTES + stringValue.getBytes(StandardCharsets.UTF_8).length;
            default -> throw new ExpressionEvaluationException("Row value does not match schema type");
        };
    }
}
