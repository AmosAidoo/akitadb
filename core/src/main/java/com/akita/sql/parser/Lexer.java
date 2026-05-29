package com.akita.sql.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Lexer {
    private static final Map<String, TokenType> KEYWORDS = Map.ofEntries(
            Map.entry("SELECT", TokenType.SELECT),
            Map.entry("CREATE", TokenType.CREATE),
            Map.entry("TABLE", TokenType.TABLE),
            Map.entry("FROM", TokenType.FROM),
            Map.entry("WHERE", TokenType.WHERE),
            Map.entry("AND", TokenType.AND),
            Map.entry("OR", TokenType.OR),
            Map.entry("NOT", TokenType.NOT),
            Map.entry("TRUE", TokenType.TRUE),
            Map.entry("FALSE", TokenType.FALSE),
            Map.entry("NULL", TokenType.NULL)
    );

    private final String source;
    private final List<Token> tokens = new ArrayList<>();
    private int current = 0;

    public Lexer(String source) {
        this.source = source;
    }

    public List<Token> tokenize() {
        while (!isAtEnd()) {
            int start = current;
            char character = advance();

            switch (character) {
                case ' ', '\r', '\t', '\n' -> {
                }
                case ',' -> addToken(TokenType.COMMA, start);
                case '.' -> addToken(TokenType.DOT, start);
                case '(' -> addToken(TokenType.LEFT_PAREN, start);
                case ')' -> addToken(TokenType.RIGHT_PAREN, start);
                case ';' -> addToken(TokenType.SEMICOLON, start);
                case '=' -> addToken(TokenType.EQUAL, start);
                case '!' -> addToken(match('=') ? TokenType.NOT_EQUAL : null, start);
                case '<' -> addToken(match('=') ? TokenType.LESS_THAN_OR_EQUAL : match('>') ? TokenType.NOT_EQUAL : TokenType.LESS_THAN, start);
                case '>' -> addToken(match('=') ? TokenType.GREATER_THAN_OR_EQUAL : TokenType.GREATER_THAN, start);
                case '+' -> addToken(TokenType.PLUS, start);
                case '-' -> addToken(TokenType.MINUS, start);
                case '*' -> addToken(TokenType.STAR, start);
                case '/' -> addToken(TokenType.SLASH, start);
                case '\'' -> string(start);
                default -> {
                    if (isDigit(character)) {
                        integer(start);
                    } else if (isIdentifierStart(character)) {
                        identifier(start);
                    } else {
                        throw new ParseException("Unexpected character '" + character + "'", start);
                    }
                }
            }
        }

        tokens.add(new Token(TokenType.EOF, "", current));
        return List.copyOf(tokens);
    }

    private void identifier(int start) {
        while (!isAtEnd() && isIdentifierPart(peek())) {
            advance();
        }

        String lexeme = source.substring(start, current);
        TokenType type = KEYWORDS.getOrDefault(lexeme.toUpperCase(), TokenType.IDENTIFIER);
        tokens.add(new Token(type, lexeme, start));
    }

    private void integer(int start) {
        while (!isAtEnd() && isDigit(peek())) {
            advance();
        }
        tokens.add(new Token(TokenType.INTEGER, source.substring(start, current), start));
    }

    private void string(int start) {
        while (!isAtEnd() && peek() != '\'') {
            advance();
        }

        if (isAtEnd()) {
            throw new ParseException("Unterminated string literal", start);
        }

        advance();
        tokens.add(new Token(TokenType.STRING, source.substring(start + 1, current - 1), start));
    }

    private void addToken(TokenType type, int start) {
        if (type == null) {
            throw new ParseException("Unexpected character '" + source.charAt(start) + "'", start);
        }
        tokens.add(new Token(type, source.substring(start, current), start));
    }

    private char advance() {
        return source.charAt(current++);
    }

    private boolean match(char expected) {
        if (isAtEnd() || source.charAt(current) != expected) {
            return false;
        }
        current++;
        return true;
    }

    private char peek() {
        return source.charAt(current);
    }

    private boolean isAtEnd() {
        return current >= source.length();
    }

    private static boolean isDigit(char character) {
        return character >= '0' && character <= '9';
    }

    private static boolean isIdentifierStart(char character) {
        return Character.isLetter(character) || character == '_';
    }

    private static boolean isIdentifierPart(char character) {
        return Character.isLetterOrDigit(character) || character == '_';
    }
}
