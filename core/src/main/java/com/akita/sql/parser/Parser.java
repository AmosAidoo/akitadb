package com.akita.sql.parser;

import com.akita.sql.ast.BinaryExpression;
import com.akita.sql.ast.BinaryOperator;
import com.akita.sql.ast.ColumnDefinition;
import com.akita.sql.ast.CreateTableStatement;
import com.akita.sql.ast.Expression;
import com.akita.sql.ast.IdentifierExpression;
import com.akita.sql.ast.LiteralExpression;
import com.akita.sql.ast.QualifiedName;
import com.akita.sql.ast.SelectItem;
import com.akita.sql.ast.SelectStatement;
import com.akita.sql.ast.SqlTypeName;
import com.akita.sql.ast.Statement;
import com.akita.sql.ast.TableRef;
import com.akita.sql.ast.UnaryExpression;
import com.akita.sql.ast.UnaryOperator;

import java.util.ArrayList;
import java.util.List;

public class Parser {
    private static final int MIN_BINDING_POWER = 0;
    private static final int LOGICAL_OR_BINDING_POWER = 10;
    private static final int LOGICAL_AND_BINDING_POWER = 20;
    private static final int COMPARISON_BINDING_POWER = 50;
    private static final int SUM_BINDING_POWER = 70;
    private static final int PRODUCT_BINDING_POWER = 80;
    private static final int UNARY_BINDING_POWER = 100;

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
        if (check(TokenType.CREATE)) {
            return createTableStatement();
        }
        throw error(peek(), "Expected SELECT or CREATE statement");
    }

    private CreateTableStatement createTableStatement() {
        consume(TokenType.CREATE, "Expected CREATE");
        consume(TokenType.TABLE, "Expected TABLE after CREATE");
        QualifiedName tableName = qualifiedName();
        consume(TokenType.LEFT_PAREN, "Expected '(' after table name");

        List<ColumnDefinition> columns = new ArrayList<>();
        do {
            columns.add(columnDefinition());
        } while (match(TokenType.COMMA));

        consume(TokenType.RIGHT_PAREN, "Expected ')' after column definitions");
        return new CreateTableStatement(tableName, columns);
    }

    private ColumnDefinition columnDefinition() {
        String name = consume(TokenType.IDENTIFIER, "Expected column name").lexeme();
        SqlTypeName type = typeName();
        boolean nullable = true;
        if (match(TokenType.NOT)) {
            consume(TokenType.NULL, "Expected NULL after NOT");
            nullable = false;
        }
        return new ColumnDefinition(name, type, nullable);
    }

    private SqlTypeName typeName() {
        Token name = consume(TokenType.IDENTIFIER, "Expected type name");
        return switch (name.lexeme().toUpperCase()) {
            case "INTEGER", "INT" -> new SqlTypeName.Integer();
            case "BIGINT" -> new SqlTypeName.BigInt();
            case "DOUBLE" -> new SqlTypeName.Double();
            case "BOOLEAN", "BOOL" -> new SqlTypeName.Boolean();
            case "VARCHAR" -> varcharType();
            default -> throw error(name, "Unknown type: " + name.lexeme());
        };
    }

    private SqlTypeName.Varchar varcharType() {
        consume(TokenType.LEFT_PAREN, "Expected '(' after VARCHAR");
        Token length = consume(TokenType.INTEGER, "Expected VARCHAR length");
        consume(TokenType.RIGHT_PAREN, "Expected ')' after VARCHAR length");
        int maxLength = Integer.parseInt(length.lexeme());
        if (maxLength < 1) {
            throw error(length, "VARCHAR length must be positive");
        }
        return new SqlTypeName.Varchar(maxLength);
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
        return expression(MIN_BINDING_POWER);
    }

    private Expression expression(int minimumBindingPower) {
        Expression left = prefix();

        while (true) {
            Token operator = peek();
            int bindingPower = infixBindingPower(operator.type());
            if (bindingPower <= minimumBindingPower) {
                break;
            }

            advance();
            Expression right = expression(rightBindingPower(operator.type(), bindingPower));
            left = new BinaryExpression(left, binaryOperator(operator), right);
        }

        return left;
    }

    private Expression prefix() {
        if (match(TokenType.PLUS, TokenType.MINUS)) {
            Token operator = previous();
            return new UnaryExpression(unaryOperator(operator), expression(UNARY_BINDING_POWER));
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

    private int infixBindingPower(TokenType type) {
        return switch (type) {
            case OR -> LOGICAL_OR_BINDING_POWER;
            case AND -> LOGICAL_AND_BINDING_POWER;
            case EQUAL,
                 NOT_EQUAL,
                 LESS_THAN,
                 LESS_THAN_OR_EQUAL,
                 GREATER_THAN,
                 GREATER_THAN_OR_EQUAL -> COMPARISON_BINDING_POWER;
            case PLUS, MINUS -> SUM_BINDING_POWER;
            case STAR, SLASH -> PRODUCT_BINDING_POWER;
            default -> MIN_BINDING_POWER;
        };
    }

    private int rightBindingPower(TokenType type, int bindingPower) {
        if (isRightAssociative(type)) {
            return bindingPower - 1;
        }
        return bindingPower;
    }

    private boolean isRightAssociative(TokenType type) {
        return false;
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
