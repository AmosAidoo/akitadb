package com.akita.query.bind;

import com.akita.datatype.AkitaType;
import com.akita.sql.ast.UnaryOperator;

public record BoundUnaryExpression(
        UnaryOperator operator,
        BoundExpression operand,
        AkitaType type
) implements BoundExpression {
}
