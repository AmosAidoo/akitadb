package com.akita.storage;

import java.io.IOException;

public class FileChannelContainerManager implements ContainerManager {
    private final FileChannelVFS vfs;

    private FileChannelContainerManager(FileChannelVFS vfs) {
        this.vfs = vfs;
    }

    public static FileChannelContainerManager create(FileChannelVFS vfs) {
        return new FileChannelContainerManager(vfs);
    }

    @Override
    public ContainerId createContainer() throws IOException {
        ContainerId containerId = ContainerId.generate();
        vfs.open(containerId, OpenMode.READ, OpenMode.WRITE, OpenMode.CREATE);
        return containerId;
    }
}
