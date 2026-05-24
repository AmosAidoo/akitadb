package com.akita.catalog;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class CatalogJsonParser {
    private CatalogJsonParser() {
    }

    static Object parse(String source) {
        Parser parser = new Parser(source);
        Object value = parser.parseValue();
        parser.skipWhitespace();
        if (!parser.isAtEnd()) {
            throw parser.error("expected end of input");
        }
        return value;
    }

    static String stringify(Object value) {
        StringBuilder json = new StringBuilder();
        writeValue(json, value);
        return json.toString();
    }

    private static void writeValue(StringBuilder json, Object value) {
        switch (value) {
            case null -> json.append("null");
            case String string -> writeString(json, string);
            case Boolean bool -> json.append(bool);
            case Integer number -> json.append(number);
            case Long number -> json.append(number);
            case Double number -> {
                if (!Double.isFinite(number)) {
                    throw new IllegalArgumentException("unsupported JSON number: " + number);
                }
                json.append(number);
            }
            case Map<?, ?> object -> writeObject(json, object);
            case List<?> array -> writeArray(json, array);
            default -> throw new IllegalArgumentException("unsupported JSON value: " + value.getClass().getName());
        }
    }

    private static void writeObject(StringBuilder json, Map<?, ?> object) {
        json.append('{');
        boolean first = true;
        for (Map.Entry<?, ?> entry : object.entrySet()) {
            if (!first) {
                json.append(',');
            }
            first = false;
            writeString(json, (String) entry.getKey());
            json.append(':');
            writeValue(json, entry.getValue());
        }
        json.append('}');
    }

    private static void writeArray(StringBuilder json, List<?> array) {
        json.append('[');
        boolean first = true;
        for (Object item : array) {
            if (!first) {
                json.append(',');
            }
            first = false;
            writeValue(json, item);
        }
        json.append(']');
    }

    private static void writeString(StringBuilder json, String source) {
        json.append('"');
        for (int i = 0; i < source.length(); i++) {
            char c = source.charAt(i);
            switch (c) {
                case '"' -> json.append("\\\"");
                case '\\' -> json.append("\\\\");
                case '\b' -> json.append("\\b");
                case '\f' -> json.append("\\f");
                case '\n' -> json.append("\\n");
                case '\r' -> json.append("\\r");
                case '\t' -> json.append("\\t");
                default -> {
                    if (c < 0x20) {
                        json.append(String.format("\\u%04x", (int) c));
                    } else {
                        json.append(c);
                    }
                }
            }
        }
        json.append('"');
    }

    private static final class Parser {
        private final String source;
        private int position;

        private Parser(String source) {
            this.source = source;
        }

        private Object parseValue() {
            skipWhitespace();
            if (isAtEnd()) {
                throw error("expected JSON value");
            }

            return switch (peek()) {
                case '{' -> parseObject();
                case '[' -> parseArray();
                case '"' -> parseString();
                case 't' -> parseLiteral("true", Boolean.TRUE);
                case 'f' -> parseLiteral("false", Boolean.FALSE);
                case 'n' -> parseLiteral("null", null);
                default -> {
                    if (peek() == '-' || Character.isDigit(peek())) {
                        yield parseNumber();
                    }
                    throw error("expected JSON value");
                }
            };
        }

        private Map<String, Object> parseObject() {
            consume('{');
            Map<String, Object> object = new LinkedHashMap<>();
            skipWhitespace();
            if (match('}')) {
                return object;
            }

            do {
                skipWhitespace();
                if (peek() != '"') {
                    throw error("expected object key");
                }
                String key = parseString();
                skipWhitespace();
                consume(':');
                object.put(key, parseValue());
                skipWhitespace();
            } while (match(','));

            consume('}');
            return object;
        }

        private List<Object> parseArray() {
            consume('[');
            List<Object> array = new ArrayList<>();
            skipWhitespace();
            if (match(']')) {
                return array;
            }

            do {
                array.add(parseValue());
                skipWhitespace();
            } while (match(','));

            consume(']');
            return array;
        }

        private String parseString() {
            consume('"');
            StringBuilder value = new StringBuilder();
            while (!isAtEnd()) {
                char c = advance();
                if (c == '"') {
                    return value.toString();
                }
                if (c == '\\') {
                    value.append(parseEscape());
                } else {
                    if (c < 0x20) {
                        throw error("unescaped control character in string");
                    }
                    value.append(c);
                }
            }
            throw error("unterminated string");
        }

        private char parseEscape() {
            if (isAtEnd()) {
                throw error("unterminated escape sequence");
            }
            return switch (advance()) {
                case '"' -> '"';
                case '\\' -> '\\';
                case '/' -> '/';
                case 'b' -> '\b';
                case 'f' -> '\f';
                case 'n' -> '\n';
                case 'r' -> '\r';
                case 't' -> '\t';
                case 'u' -> parseUnicodeEscape();
                default -> throw error("invalid escape sequence");
            };
        }

        private char parseUnicodeEscape() {
            if (position + 4 > source.length()) {
                throw error("incomplete unicode escape");
            }
            int value = 0;
            for (int i = 0; i < 4; i++) {
                char c = advance();
                int digit = Character.digit(c, 16);
                if (digit < 0) {
                    throw error("invalid unicode escape");
                }
                value = (value << 4) + digit;
            }
            return (char) value;
        }

        private Object parseNumber() {
            int start = position;
            match('-');
            if (match('0')) {
                // Leading zero consumed.
            } else if (isDigitOneToNine(peek())) {
                while (!isAtEnd() && Character.isDigit(peek())) {
                    advance();
                }
            } else {
                throw error("invalid number");
            }

            boolean floatingPoint = false;
            if (match('.')) {
                floatingPoint = true;
                if (isAtEnd() || !Character.isDigit(peek())) {
                    throw error("invalid number");
                }
                while (!isAtEnd() && Character.isDigit(peek())) {
                    advance();
                }
            }

            if (!isAtEnd() && (peek() == 'e' || peek() == 'E')) {
                floatingPoint = true;
                advance();
                if (!isAtEnd() && (peek() == '+' || peek() == '-')) {
                    advance();
                }
                if (isAtEnd() || !Character.isDigit(peek())) {
                    throw error("invalid number");
                }
                while (!isAtEnd() && Character.isDigit(peek())) {
                    advance();
                }
            }

            String number = source.substring(start, position);
            if (floatingPoint) {
                return Double.parseDouble(number);
            }
            return Long.parseLong(number);
        }

        private Object parseLiteral(String literal, Object value) {
            if (!source.startsWith(literal, position)) {
                throw error("expected " + literal);
            }
            position += literal.length();
            return value;
        }

        private void skipWhitespace() {
            while (!isAtEnd()) {
                char c = peek();
                if (c == ' ' || c == '\n' || c == '\r' || c == '\t') {
                    position++;
                } else {
                    return;
                }
            }
        }

        private boolean match(char expected) {
            if (isAtEnd() || peek() != expected) {
                return false;
            }
            position++;
            return true;
        }

        private void consume(char expected) {
            if (!match(expected)) {
                throw error("expected '" + expected + "'");
            }
        }

        private char advance() {
            return source.charAt(position++);
        }

        private char peek() {
            if (isAtEnd()) {
                return '\0';
            }
            return source.charAt(position);
        }

        private boolean isAtEnd() {
            return position >= source.length();
        }

        private static boolean isDigitOneToNine(char c) {
            return c >= '1' && c <= '9';
        }

        private IllegalArgumentException error(String message) {
            return new IllegalArgumentException(message + " at position " + position);
        }
    }
}
