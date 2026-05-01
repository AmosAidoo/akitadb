package com.akita.storage;

import com.akita.storage.*;
import com.akita.testing.AkitaExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(AkitaExtension.class)
class FileChannelBlockManagerTest {
    @Test
    void allocatesCorrectBlockSize(
            FileChannelBlockManager bm,
            FileChannelContainerManager cm,
            FileChannelVFS vfs  // you'll need to expose this from the extension
    ) throws Exception {
        ContainerId containerId = cm.createContainer();
        VFSFile file = vfs.open(containerId);

        bm.allocateBlock(containerId, 0);
        assertThat(file.size()).isEqualTo(BlockManager.BLOCK_SIZE);

        bm.allocateBlock(containerId, 9);
        assertThat(file.size()).isEqualTo(10L * BlockManager.BLOCK_SIZE);
    }

//    @Test
//    void writesBlockToDiskWhenContainerExists() {}
//
//    @Test
//    void readsBlockFromDiskWhenContainerAndBlockExists() {}
//
//    @Test
//    void allocatesBlockImplicitlyWhenBlockDoesntExist() {}
}