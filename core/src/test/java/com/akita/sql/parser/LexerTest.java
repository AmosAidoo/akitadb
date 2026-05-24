package com.akita.sql.parser;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LexerTest {

    @Test
    void tokenizesSelectFromWhereQuery() {
        List<Token> tokens = new Lexer("SELECT id, name FROM users WHERE age > 18;").tokenize();

        assertThat(tokens)
                .extracting(Token::type)
                .containsExactly(
                        TokenType.SELECT,
                        TokenType.IDENTIFIER,
                        TokenType.COMMA,
                        TokenType.IDENTIFIER,
                        TokenType.FROM,
                        TokenType.IDENTIFIER,
                        TokenType.WHERE,
                        TokenType.IDENTIFIER,
                        TokenType.GREATER_THAN,
                        TokenType.INTEGER,
                        TokenType.SEMICOLON,
                        TokenType.EOF
                );

        assertThat(tokens)
                .extracting(Token::lexeme)
                .containsExactly(
                        "SELECT",
                        "id",
                        ",",
                        "name",
                        "FROM",
                        "users",
                        "WHERE",
                        "age",
                        ">",
                        "18",
                        ";",
                        ""
                );
    }

    @Test
    void tokenizesKeywordsCaseInsensitively() {
        List<Token> tokens = new Lexer("select TRUE false null and OR from where").tokenize();

        assertThat(tokens)
                .extracting(Token::type)
                .containsExactly(
                        TokenType.SELECT,
                        TokenType.TRUE,
                        TokenType.FALSE,
                        TokenType.NULL,
                        TokenType.AND,
                        TokenType.OR,
                        TokenType.FROM,
                        TokenType.WHERE,
                        TokenType.EOF
                );
    }

    @Test
    void tokenizesStringsOperatorsAndPunctuation() {
        List<Token> tokens = new Lexer("a.b != 'akita' AND x <= 10 OR y <> 20 + 1 - 2 * 3 / 4").tokenize();

        assertThat(tokens)
                .extracting(Token::type)
                .containsExactly(
                        TokenType.IDENTIFIER,
                        TokenType.DOT,
                        TokenType.IDENTIFIER,
                        TokenType.NOT_EQUAL,
                        TokenType.STRING,
                        TokenType.AND,
                        TokenType.IDENTIFIER,
                        TokenType.LESS_THAN_OR_EQUAL,
                        TokenType.INTEGER,
                        TokenType.OR,
                        TokenType.IDENTIFIER,
                        TokenType.NOT_EQUAL,
                        TokenType.INTEGER,
                        TokenType.PLUS,
                        TokenType.INTEGER,
                        TokenType.MINUS,
                        TokenType.INTEGER,
                        TokenType.STAR,
                        TokenType.INTEGER,
                        TokenType.SLASH,
                        TokenType.INTEGER,
                        TokenType.EOF
                );

        assertThat(tokens.get(4).lexeme()).isEqualTo("akita");
    }

    @Test
    void tracksTokenPositions() {
        List<Token> tokens = new Lexer("SELECT id").tokenize();

        assertThat(tokens.get(0).position()).isEqualTo(0);
        assertThat(tokens.get(1).position()).isEqualTo(7);
        assertThat(tokens.get(2).position()).isEqualTo(9);
    }

    @Test
    void rejectsUnknownCharacters() {
        assertThatThrownBy(() -> new Lexer("SELECT @").tokenize())
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Unexpected character '@'")
                .hasMessageContaining("position 7");
    }

    @Test
    void rejectsUnterminatedStrings() {
        assertThatThrownBy(() -> new Lexer("SELECT 'akita").tokenize())
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Unterminated string literal")
                .hasMessageContaining("position 7");
    }
}
