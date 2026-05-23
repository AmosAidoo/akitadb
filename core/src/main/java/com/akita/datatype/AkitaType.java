package com.akita.datatype;

public sealed interface AkitaType
        permits AkitaType.Integer,
        AkitaType.BigInt,
        AkitaType.Double,
        AkitaType.Varchar,
        AkitaType.Boolean {

    record Integer() implements AkitaType {}
    record BigInt() implements AkitaType {}
    record Double() implements AkitaType {}
    record Varchar(int maxLength) implements AkitaType {}
    record Boolean() implements AkitaType {}
}
