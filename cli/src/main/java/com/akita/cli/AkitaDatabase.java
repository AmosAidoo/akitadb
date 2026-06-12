package com.akita.cli;

import com.akita.AkitaEngine;
import com.akita.query.QueryEngine;

import java.nio.file.Path;

public final class AkitaDatabase implements AutoCloseable {
    static final String CATALOG_FILE = AkitaEngine.CATALOG_FILE;

    private final AkitaEngine engine;

    private AkitaDatabase(AkitaEngine engine) {
        this.engine = engine;
    }

    public static AkitaDatabase open(Path dataDirectory) {
        return new AkitaDatabase(AkitaEngine.open(dataDirectory));
    }

    public Path dataDirectory() {
        return engine.dataDirectory();
    }

    public QueryEngine queryEngine() {
        return engine.queryEngine();
    }

    @Override
    public void close() {
        engine.close();
    }
}
