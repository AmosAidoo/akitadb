package com.akita.datatype;

import java.util.Comparator;

public class AkitaValueComparator implements Comparator<AkitaValue> {

    public static final AkitaValueComparator INSTANCE = new AkitaValueComparator();

    @Override
    public int compare(AkitaValue a, AkitaValue b) {
        if (a instanceof AkitaValue.Null && b instanceof AkitaValue.Null) return 0;
        if (a instanceof AkitaValue.Null) return 1;
        if (b instanceof AkitaValue.Null) return -1;

        return switch (a) {
            case AkitaValue.IntVal(var x) when b instanceof AkitaValue.IntVal(var y) ->
                    Integer.compare(x, y);
            case AkitaValue.IntVal ignored ->
                    throw new IllegalArgumentException("Type mismatch: expected IntVal, got " + b.getClass().getSimpleName());

            case AkitaValue.BigIntVal(var x) when b instanceof AkitaValue.BigIntVal(var y) ->
                    Long.compare(x, y);
            case AkitaValue.BigIntVal ignored ->
                    throw new IllegalArgumentException("Type mismatch: expected BigIntVal, got " + b.getClass().getSimpleName());

            case AkitaValue.DoubleVal(var x) when b instanceof AkitaValue.DoubleVal(var y) ->
                    Double.compare(x, y);
            case AkitaValue.DoubleVal ignored ->
                    throw new IllegalArgumentException("Type mismatch: expected DoubleVal, got " + b.getClass().getSimpleName());

            case AkitaValue.BoolVal(var x) when b instanceof AkitaValue.BoolVal(var y) ->
                    Boolean.compare(x, y);
            case AkitaValue.BoolVal ignored ->
                    throw new IllegalArgumentException("Type mismatch: expected BoolVal, got " + b.getClass().getSimpleName());

            case AkitaValue.VarcharVal(var x) when b instanceof AkitaValue.VarcharVal(var y) ->
                    x.compareTo(y);
            case AkitaValue.VarcharVal ignored ->
                    throw new IllegalArgumentException("Type mismatch: expected VarcharVal, got " + b.getClass().getSimpleName());

            default -> throw new IllegalStateException("Unexpected value: " + a);
        };
    }
}