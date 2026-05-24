package com.akita.sql.parser;

public record Token(
        TokenType type,
        String lexeme,
        int position
) {}
