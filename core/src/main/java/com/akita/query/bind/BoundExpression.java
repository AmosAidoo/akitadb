package com.akita.query.bind;

import com.akita.datatype.AkitaType;

public sealed interface BoundExpression
        permits BoundColumnReference, BoundLiteral, BoundBinaryExpression, BoundUnaryExpression {

    AkitaType type();
}
