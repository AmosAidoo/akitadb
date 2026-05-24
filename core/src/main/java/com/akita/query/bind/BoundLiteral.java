package com.akita.query.bind;

import com.akita.datatype.AkitaType;

public record BoundLiteral(
        Object value,
        AkitaType type
) implements BoundExpression {
}
