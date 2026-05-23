package com.akita.datatype;

public sealed interface AkitaValue
        permits AkitaValue.IntVal,
        AkitaValue.BigIntVal,
        AkitaValue.DoubleVal,
        AkitaValue.VarcharVal,
        AkitaValue.BoolVal,
        AkitaValue.Null {

    record IntVal(int value) implements AkitaValue {}
    record BigIntVal(long value) implements AkitaValue {}
    record DoubleVal(double value) implements AkitaValue {}
    record VarcharVal(String value) implements AkitaValue {}
    record BoolVal(boolean value) implements AkitaValue {}
    record Null() implements AkitaValue {}
}
