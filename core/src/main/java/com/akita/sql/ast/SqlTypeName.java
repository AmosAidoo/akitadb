package com.akita.sql.ast;

public sealed interface SqlTypeName
        permits SqlTypeName.Integer,
        SqlTypeName.BigInt,
        SqlTypeName.Double,
        SqlTypeName.Boolean,
        SqlTypeName.Varchar {

    record Integer() implements SqlTypeName {}
    record BigInt() implements SqlTypeName {}
    record Double() implements SqlTypeName {}
    record Boolean() implements SqlTypeName {}
    record Varchar(int maxLength) implements SqlTypeName {}
}
