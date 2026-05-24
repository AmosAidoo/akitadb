package com.akita.query.bind;

import com.akita.datatype.AkitaType;
import com.akita.sql.ast.BinaryOperator;

public record BoundBinaryExpression(
        BoundExpression left,
        BinaryOperator operator,
        BoundExpression right,
        AkitaType type
) implements BoundExpression {
}
