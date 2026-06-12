package com.akita;

import com.akita.buffer.BufferPoolManager;
import com.akita.buffer.FCFSDiskScheduler;
import com.akita.buffer.Frame;
import com.akita.buffer.FrameId;
import com.akita.buffer.replacers.arc.ArcReplacer;
import com.akita.catalog.JsonCatalog;
import com.akita.query.QueryEngine;
import com.akita.query.storage.HeapTableAccess;
import com.akita.query.storage.TableManager;
import com.akita.storage.FileChannelStorage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class AkitaEngine implements AutoCloseable {
    public static final String CATALOG_FILE = "catalog.json";
    private static final String EMPTY_CATALOG = "{\"tables\":[]}\n";

    private final Path dataDirectory;
    private final ExecutorService executor;
    private final BufferPoolManager bufferPoolManager;
    private final QueryEngine queryEngine;

    private AkitaEngine(
            Path dataDirectory,
            ExecutorService executor,
            BufferPoolManager bufferPoolManager,
            QueryEngine queryEngine
    ) {
        this.dataDirectory = dataDirectory;
        this.executor = executor;
        this.bufferPoolManager = bufferPoolManager;
        this.queryEngine = queryEngine;
    }

    public static AkitaEngine open(Path dataDirectory) {
        return open(dataDirectory, EngineOptions.defaults());
    }

    public static AkitaEngine open(Path dataDirectory, EngineOptions options) {
        try {
            Files.createDirectories(dataDirectory);
            Path catalogPath = dataDirectory.resolve(CATALOG_FILE);
            if (Files.notExists(catalogPath)) {
                Files.writeString(catalogPath, EMPTY_CATALOG);
            }

            FileChannelStorage storage = FileChannelStorage.open(dataDirectory);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            BufferPoolManager bufferPoolManager = BufferPoolManager.create(
                    FCFSDiskScheduler.create(executor, storage),
                    ArcReplacer.create(options.bufferFrameCount()),
                    frames(options.bufferFrameCount()),
                    new HashMap<>()
            );
            JsonCatalog catalog = new JsonCatalog(catalogPath);
            QueryEngine queryEngine = new QueryEngine(
                    catalog,
                    new TableManager(catalog, bufferPoolManager),
                    new HeapTableAccess(bufferPoolManager)
            );
            return new AkitaEngine(dataDirectory, executor, bufferPoolManager, queryEngine);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to open Akita database at " + dataDirectory, e);
        } catch (RuntimeException e) {
            throw new IllegalStateException("failed to open Akita database at " + dataDirectory + ": " + e.getMessage(), e);
        }
    }

    public Path dataDirectory() {
        return dataDirectory;
    }

    public QueryEngine queryEngine() {
        return queryEngine;
    }

    @Override
    public void close() {
        bufferPoolManager.flushAllPages();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static Map<FrameId, Frame> frames(int count) {
        Map<FrameId, Frame> frames = new HashMap<>();
        for (int i = 0; i < count; i++) {
            FrameId frameId = new FrameId(i);
            frames.put(frameId, Frame.create(frameId));
        }
        return frames;
    }
}
