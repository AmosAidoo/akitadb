package com.akita.sql.parser;

public enum TokenType {
    SELECT,
    CREATE,
    INSERT,
    TABLE,
    INTO,
    VALUES,
    FROM,
    WHERE,
    AND,
    OR,
    NOT,
    TRUE,
    FALSE,
    NULL,

    IDENTIFIER,
    INTEGER,
    STRING,

    COMMA,
    DOT,
    LEFT_PAREN,
    RIGHT_PAREN,
    SEMICOLON,

    EQUAL,
    NOT_EQUAL,
    LESS_THAN,
    LESS_THAN_OR_EQUAL,
    GREATER_THAN,
    GREATER_THAN_OR_EQUAL,
    PLUS,
    MINUS,
    STAR,
    SLASH,

    EOF
}
