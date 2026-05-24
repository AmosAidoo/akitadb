package com.akita.catalog;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CatalogJsonTest {

    @Test
    void parsesObjectsArraysAndEscapedStrings() {
        Object parsed = CatalogJsonParser.parse("""
                {
                  "name": "line\\nbreak\\u005fcol",
                  "values": [1, 1.25, 1e3, true, false, null, "quote: \\""]
                }
                """);

        Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("name", "line\nbreak_col");
        List<Object> values = new ArrayList<>();
        values.add(1L);
        values.add(1.25);
        values.add(1000.0);
        values.add(true);
        values.add(false);
        values.add(null);
        values.add("quote: \"");
        expected.put("values", values);
        assertThat(parsed).isEqualTo(expected);
    }

    @Test
    void writesEscapedStrings() {
        String json = CatalogJsonParser.stringify(Map.of("name", "line\nbreak_col"));

        assertThat(json).isEqualTo("{\"name\":\"line\\nbreak_col\"}");
    }

    @Test
    void preservesIntegerAndFloatingPointNumberKinds() {
        Object parsed = CatalogJsonParser.parse("[0, -12, 1.25, 1e3]");

        assertThat(parsed).isEqualTo(List.of(0L, -12L, 1.25, 1000.0));
    }

    @Test
    void rejectsTrailingInput() {
        assertThatThrownBy(() -> CatalogJsonParser.parse("{} true"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expected end of input");
    }
}
