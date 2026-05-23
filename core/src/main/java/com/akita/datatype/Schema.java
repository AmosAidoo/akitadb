package com.akita.datatype;

import java.util.List;

public record Schema(List<ColumnMetadata> columns) {}
