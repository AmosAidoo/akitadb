package com.akita.datatype;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SchemaTest {

    @Test
    void defensivelyCopiesColumns() {
        ColumnMetadata id = new ColumnMetadata("id", new AkitaType.Integer(), 0, false);
        List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(id);

        Schema schema = new Schema(columns);
        columns.clear();

        assertThat(schema.columns()).containsExactly(id);
        assertThatThrownBy(() -> schema.columns().add(new ColumnMetadata("name", new AkitaType.Varchar(8), 1, true)))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
