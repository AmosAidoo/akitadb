package com.akita.query.bind;

public sealed interface BoundStatement permits BoundSelectStatement, BoundInsertStatement {
}
