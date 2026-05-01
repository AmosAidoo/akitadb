package com.akita.buffer;

import com.akita.buffer.guards.ReadPageGuard;
import com.akita.buffer.guards.WritePageGuard;
import com.akita.buffer.replacers.arc.ArcReplacer;
import com.akita.storage.*;
import com.akita.testing.AkitaExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;


import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(AkitaExtension.class)
class BufferPoolManagerTest {

    @Test
    void veryBasicTest(
            BufferPoolManager bpm,
            FileChannelContainerManager cm
    ) throws Exception {

        ContainerId containerId = cm.createContainer();
        PageId pageId = new PageId(containerId, 0);
        final String expected = "Hello world";

        // Check WritePageGuard for basic functionality
        try (WritePageGuard writePageGuard = bpm.writePage(pageId)) {
            writePageGuard.getData().put(expected.getBytes(StandardCharsets.UTF_8));

            byte[] dst = new byte[expected.length()];
            writePageGuard.getData().get(dst);
            assertThat(new String(dst, StandardCharsets.UTF_8)).isEqualTo(expected);
        }

        // Check ReadPageGuard for basic functionality
        try (ReadPageGuard readPageGuard = bpm.readPage(pageId)) {
            byte[] dst = new byte[expected.length()];
            readPageGuard.getData().get(dst);
            assertThat(new String(dst, StandardCharsets.UTF_8)).isEqualTo(expected);
        }

        // Check ReadPageGuard for basic functionality again
        try (ReadPageGuard readPageGuard = bpm.readPage(pageId)) {
            byte[] dst = new byte[expected.length()];
            readPageGuard.getData().get(dst);
            assertThat(new String(dst, StandardCharsets.UTF_8)).isEqualTo(expected);
        }

        assertThat(bpm.deletePage(pageId)).isTrue();
    }
}