package com.akita.testing;

import com.akita.buffer.*;
import com.akita.buffer.replacers.arc.ArcReplacer;
import com.akita.storage.*;
import org.junit.jupiter.api.extension.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AkitaExtension implements BeforeEachCallback, AfterEachCallback, ParameterResolver {
    private static final int DEFAULT_FRAMES = 10;
    private static final String KEY = "akita";

    record Context(
            Path tempDir,
            FileChannelVFS vfs,
            FileChannelBlockManager blockManager,
            FileChannelContainerManager containerManager,
            ExecutorService executor,
            BufferPoolManager bpm
    ) {}

    @Override
    public void beforeEach(ExtensionContext ctx) throws Exception {
        Path tempDir = Files.createTempDirectory("akita-test-");
        FileChannelVFS vfs = FileChannelVFS.create(tempDir);
        FileChannelBlockManager bm = FileChannelBlockManager.create(vfs);
        FileChannelContainerManager cm = FileChannelContainerManager.create(vfs);
        ExecutorService executor = Executors.newSingleThreadExecutor();

        Map<FrameId, Frame> frames = new HashMap<>();
        for (int i = 0; i < DEFAULT_FRAMES; i++) {
            FrameId id = new FrameId(i);
            frames.put(id, Frame.create(id));
        }

        BufferPoolManager bpm = BufferPoolManager.create(
                FCFSDiskScheduler.create(executor, bm),
                ArcReplacer.create(DEFAULT_FRAMES),
                frames,
                new HashMap<>()
        );

        ctx.getStore(ExtensionContext.Namespace.create(KEY))
                .put(KEY, new Context(tempDir, vfs, bm, cm, executor, bpm));
    }

    @Override
    public void afterEach(ExtensionContext ctx) throws Exception {
        Context c = get(ctx);
        c.executor().shutdown();
        c.executor().awaitTermination(5, TimeUnit.SECONDS);
        // temp dir cleanup: delete recursively
        try (var walk = Files.walk(c.tempDir())) {
            walk.sorted(java.util.Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(java.io.File::delete);
        }
    }

    @Override
    public boolean supportsParameter(ParameterContext param, ExtensionContext ctx) {
        Class<?> type = param.getParameter().getType();
        return type == BufferPoolManager.class
                || type == FileChannelBlockManager.class
                || type == FileChannelContainerManager.class;
    }

    @Override
    public Object resolveParameter(ParameterContext param, ExtensionContext ctx) {
        Context c = get(ctx);
        Class<?> type = param.getParameter().getType();
        if (type == BufferPoolManager.class)           return c.bpm();
        if (type == FileChannelBlockManager.class)     return c.blockManager();
        if (type == FileChannelContainerManager.class) return c.containerManager();
        throw new IllegalArgumentException("Unknown parameter type: " + type);
    }

    private Context get(ExtensionContext ctx) {
        return (Context) ctx.getStore(ExtensionContext.Namespace.create(KEY)).get(KEY);
    }
}