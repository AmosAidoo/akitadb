package com.akita.index.btree;

import com.akita.datatype.AkitaType;
import com.akita.datatype.AkitaValue;
import com.akita.datatype.ColumnMetadata;
import com.akita.page.Tuple;

import java.nio.charset.StandardCharsets;

class TupleDeserializer {

    private TupleDeserializer() {}

    static AkitaValue readValue(Tuple tuple, ColumnMetadata col) {
        if (col.nullable() && tuple.readByte() == 0) {
            return new AkitaValue.Null();
        }

        return switch (col.type()) {
            case AkitaType.Integer ignored  -> new AkitaValue.IntVal(tuple.readInt());
            case AkitaType.BigInt ignored   -> new AkitaValue.BigIntVal(tuple.readLong());
            case AkitaType.Double ignored   -> new AkitaValue.DoubleVal(tuple.readDouble());
            case AkitaType.Boolean ignored  -> new AkitaValue.BoolVal(tuple.readBoolean());
            case AkitaType.Varchar ignored  -> {
                int len = tuple.readInt();
                byte[] bytes = new byte[len];
                tuple.readBytes(bytes);
                yield new AkitaValue.VarcharVal(new String(bytes, StandardCharsets.UTF_8));
            }
        };
    }
}