package com.akita.sql.parser;

import com.akita.sql.ast.BinaryExpression;
import com.akita.sql.ast.BinaryOperator;
import com.akita.sql.ast.Expression;
import com.akita.sql.ast.IdentifierExpression;
import com.akita.sql.ast.LiteralExpression;
import com.akita.sql.ast.QualifiedName;
import com.akita.sql.ast.SelectItem;
import com.akita.sql.ast.SelectStatement;
import com.akita.sql.ast.Statement;
import com.akita.sql.ast.TableRef;
import com.akita.sql.ast.UnaryExpression;
import com.akita.sql.ast.UnaryOperator;

import java.util.ArrayList;
import java.util.List;

public class Parser {
    private final List<Token> tokens;
    private int current = 0;

    public Parser(String source) {
        this(new Lexer(source).tokenize());
    }

    public Parser(List<Token> tokens) {
        this.tokens = List.copyOf(tokens);
    }

    public Statement parse() {
        Statement statement = statement();
        match(TokenType.SEMICOLON);
        consume(TokenType.EOF, "Expected end of input");
        return statement;
    }

    private Statement statement() {
        if (check(TokenType.SELECT)) {
            return selectStatement();
        }
        throw error(peek(), "Expected SELECT statement");
    }

    private SelectStatement selectStatement() {
        consume(TokenType.SELECT, "Expected SELECT");

        List<SelectItem> selectItems = new ArrayList<>();
        do {
            selectItems.add(new SelectItem(expression()));
        } while (match(TokenType.COMMA));

        consume(TokenType.FROM, "Expected FROM after select list");
        TableRef from = new TableRef(qualifiedName());

        Expression where = null;
        if (match(TokenType.WHERE)) {
            where = expression();
        }

        return new SelectStatement(selectItems, from, where);
    }

    private Expression expression() {
        return or();
    }

    private Expression or() {
        Expression expression = and();
        while (match(TokenType.OR)) {
            expression = new BinaryExpression(expression, BinaryOperator.OR, and());
        }
        return expression;
    }

    private Expression and() {
        Expression expression = comparison();
        while (match(TokenType.AND)) {
            expression = new BinaryExpression(expression, BinaryOperator.AND, comparison());
        }
        return expression;
    }

    private Expression comparison() {
        Expression expression = term();
        while (match(
                TokenType.EQUAL,
                TokenType.NOT_EQUAL,
                TokenType.LESS_THAN,
                TokenType.LESS_THAN_OR_EQUAL,
                TokenType.GREATER_THAN,
                TokenType.GREATER_THAN_OR_EQUAL
        )) {
            Token operator = previous();
            expression = new BinaryExpression(expression, binaryOperator(operator), term());
        }
        return expression;
    }

    private Expression term() {
        Expression expression = factor();
        while (match(TokenType.PLUS, TokenType.MINUS)) {
            Token operator = previous();
            expression = new BinaryExpression(expression, binaryOperator(operator), factor());
        }
        return expression;
    }

    private Expression factor() {
        Expression expression = unary();
        while (match(TokenType.STAR, TokenType.SLASH)) {
            Token operator = previous();
            expression = new BinaryExpression(expression, binaryOperator(operator), unary());
        }
        return expression;
    }

    private Expression unary() {
        if (match(TokenType.PLUS, TokenType.MINUS)) {
            Token operator = previous();
            return new UnaryExpression(unaryOperator(operator), unary());
        }
        return primary();
    }

    private Expression primary() {
        if (match(TokenType.INTEGER)) {
            return LiteralExpression.integer(Integer.parseInt(previous().lexeme()));
        }
        if (match(TokenType.STRING)) {
            return LiteralExpression.string(previous().lexeme());
        }
        if (match(TokenType.TRUE)) {
            return LiteralExpression.bool(true);
        }
        if (match(TokenType.FALSE)) {
            return LiteralExpression.bool(false);
        }
        if (match(TokenType.NULL)) {
            return LiteralExpression.nullLiteral();
        }
        if (match(TokenType.LEFT_PAREN)) {
            Expression expression = expression();
            consume(TokenType.RIGHT_PAREN, "Expected ')' after expression");
            return expression;
        }
        if (check(TokenType.IDENTIFIER)) {
            return new IdentifierExpression(qualifiedName());
        }

        throw error(peek(), "Expected expression");
    }

    private QualifiedName qualifiedName() {
        List<String> parts = new ArrayList<>();
        parts.add(consume(TokenType.IDENTIFIER, "Expected identifier").lexeme());
        while (match(TokenType.DOT)) {
            parts.add(consume(TokenType.IDENTIFIER, "Expected identifier after '.'").lexeme());
        }
        return new QualifiedName(parts);
    }

    private BinaryOperator binaryOperator(Token token) {
        return switch (token.type()) {
            case OR -> BinaryOperator.OR;
            case AND -> BinaryOperator.AND;
            case EQUAL -> BinaryOperator.EQUAL;
            case NOT_EQUAL -> BinaryOperator.NOT_EQUAL;
            case LESS_THAN -> BinaryOperator.LESS_THAN;
            case LESS_THAN_OR_EQUAL -> BinaryOperator.LESS_THAN_OR_EQUAL;
            case GREATER_THAN -> BinaryOperator.GREATER_THAN;
            case GREATER_THAN_OR_EQUAL -> BinaryOperator.GREATER_THAN_OR_EQUAL;
            case PLUS -> BinaryOperator.ADD;
            case MINUS -> BinaryOperator.SUBTRACT;
            case STAR -> BinaryOperator.MULTIPLY;
            case SLASH -> BinaryOperator.DIVIDE;
            default -> throw new IllegalArgumentException("No binary operator for " + token.type());
        };
    }

    private UnaryOperator unaryOperator(Token token) {
        return switch (token.type()) {
            case PLUS -> UnaryOperator.PLUS;
            case MINUS -> UnaryOperator.MINUS;
            default -> throw new IllegalArgumentException("No unary operator for " + token.type());
        };
    }

    private boolean match(TokenType... types) {
        for (TokenType type : types) {
            if (check(type)) {
                advance();
                return true;
            }
        }
        return false;
    }

    private Token consume(TokenType type, String message) {
        if (check(type)) {
            return advance();
        }
        throw error(peek(), message);
    }

    private boolean check(TokenType type) {
        return peek().type() == type;
    }

    private Token advance() {
        if (!isAtEnd()) {
            current++;
        }
        return previous();
    }

    private boolean isAtEnd() {
        return peek().type() == TokenType.EOF;
    }

    private Token peek() {
        return tokens.get(current);
    }

    private Token previous() {
        return tokens.get(current - 1);
    }

    private ParseException error(Token token, String message) {
        return new ParseException(message + ", found " + found(token), token.position());
    }

    private String found(Token token) {
        if (token.type() == TokenType.EOF) {
            return "end of input";
        }
        return "'" + token.lexeme() + "'";
    }
}
