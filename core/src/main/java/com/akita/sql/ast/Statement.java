package com.akita.sql.ast;

public sealed interface Statement permits SelectStatement, CreateTableStatement {
}
