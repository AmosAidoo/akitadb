package com.akita.datatype;

import com.akita.sql.ast.SqlTypeName;

public final class AkitaTypes {
    private AkitaTypes() {
    }

    public static AkitaType fromSqlType(SqlTypeName type) {
        return switch (type) {
            case SqlTypeName.Integer ignored -> new AkitaType.Integer();
            case SqlTypeName.BigInt ignored -> new AkitaType.BigInt();
            case SqlTypeName.Double ignored -> new AkitaType.Double();
            case SqlTypeName.Boolean ignored -> new AkitaType.Boolean();
            case SqlTypeName.Varchar varchar -> new AkitaType.Varchar(varchar.maxLength());
        };
    }

    public static String format(AkitaType type) {
        return switch (type) {
            case AkitaType.Integer ignored -> "INTEGER";
            case AkitaType.BigInt ignored -> "BIGINT";
            case AkitaType.Double ignored -> "DOUBLE";
            case AkitaType.Boolean ignored -> "BOOLEAN";
            case AkitaType.Varchar varchar -> "VARCHAR(" + varchar.maxLength() + ")";
        };
    }

    public static AkitaType parse(String source) {
        switch (source) {
            case "INTEGER" -> {
                return new AkitaType.Integer();
            }
            case "BIGINT" -> {
                return new AkitaType.BigInt();
            }
            case "DOUBLE" -> {
                return new AkitaType.Double();
            }
            case "BOOLEAN" -> {
                return new AkitaType.Boolean();
            }
            default -> {
            }
        }
        if (source.startsWith("VARCHAR(") && source.endsWith(")")) {
            String maxLength = source.substring("VARCHAR(".length(), source.length() - 1);
            if (maxLength.isEmpty() || !maxLength.chars().allMatch(Character::isDigit)) {
                throw new IllegalArgumentException("unknown catalog type: " + source);
            }
            return new AkitaType.Varchar(Integer.parseInt(maxLength));
        }
        throw new IllegalArgumentException("unknown catalog type: " + source);
    }

    public static boolean compatible(AkitaType target, AkitaType source) {
        if (target instanceof AkitaType.Varchar targetVarchar && source instanceof AkitaType.Varchar sourceVarchar) {
            return sourceVarchar.maxLength() <= targetVarchar.maxLength();
        }
        return target.getClass().equals(source.getClass());
    }
}
