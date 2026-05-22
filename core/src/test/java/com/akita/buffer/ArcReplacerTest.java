package com.akita.buffer;

import com.akita.buffer.FrameId;
import com.akita.buffer.PageId;
import com.akita.buffer.replacers.Replacer;
import com.akita.buffer.replacers.arc.ArcReplacer;
import org.junit.jupiter.api.Test;
import com.akita.storage.ContainerId;

import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

class ArcReplacerTest {

    @Test
    void sampleWorkflowOnArcReplacerOfSizeSeven() {
        // This sample test was adapted from bustub:
        // https://github.com/cmu-db/bustub/blob/master/test/buffer/arc_replacer_test.cpp#L23-L103
        Replacer replacer = ArcReplacer.create(7);
        ContainerId containerId = containerId(1);

        replacer.recordAccess(new FrameId(1), new PageId(containerId, 1));
        replacer.recordAccess(new FrameId(2), new PageId(containerId, 2));
        replacer.recordAccess(new FrameId(3), new PageId(containerId, 3));
        replacer.recordAccess(new FrameId(4), new PageId(containerId, 4));
        replacer.recordAccess(new FrameId(5), new PageId(containerId, 5));
        replacer.recordAccess(new FrameId(6), new PageId(containerId, 6));
        replacer.setEvictable(new FrameId(1), true);
        replacer.setEvictable(new FrameId(2), true);
        replacer.setEvictable(new FrameId(3), true);
        replacer.setEvictable(new FrameId(4), true);
        replacer.setEvictable(new FrameId(5), true);
        replacer.setEvictable(new FrameId(6), false);

        assertThat(replacer.size()).isEqualTo(5);

        replacer.recordAccess(new FrameId(1), new PageId(containerId, 1));
        assertThat(replacer.evict()).isEqualTo(new FrameId(2));
        assertThat(replacer.evict()).isEqualTo(new FrameId(3));
        assertThat(replacer.evict()).isEqualTo(new FrameId(4));
        assertThat(replacer.size()).isEqualTo(2);

        replacer.recordAccess(new FrameId(2), new PageId(containerId, 7));
        replacer.setEvictable(new FrameId(2), true);

        replacer.recordAccess(new FrameId(3), new PageId(containerId, 2));
        replacer.setEvictable(new FrameId(3), true);
        assertThat(replacer.size()).isEqualTo(4);

        replacer.recordAccess(new FrameId(4), new PageId(containerId, 3));
        replacer.setEvictable(new FrameId(4), true);
        replacer.recordAccess(new FrameId(7), new PageId(containerId, 4));
        replacer.setEvictable(new FrameId(7), true);
        assertThat(replacer.size()).isEqualTo(6);

        assertThat(replacer.evict()).isEqualTo(new FrameId(5));
        assertThat(replacer.evict()).isEqualTo(new FrameId(1));

        replacer.recordAccess(new FrameId(5), new PageId(containerId, 1));
        replacer.setEvictable(new FrameId(5), true);
        assertThat(replacer.size()).isEqualTo(5);

        assertThat(replacer.evict()).isEqualTo(new FrameId(2));
    }

    @Test
    void sampleWorkflowOnArcReplacerOfSizeThree() {
        // This test was adapted from bustub:
        // https://github.com/cmu-db/bustub/blob/master/test/buffer/arc_replacer_test.cpp#L105-L215
        Replacer replacer = ArcReplacer.create(3);
        ContainerId containerId = containerId(1);

        replacer.recordAccess(new FrameId(1), new PageId(containerId, 1));
        replacer.setEvictable(new FrameId(1), true);
        replacer.recordAccess(new FrameId(2), new PageId(containerId, 2));
        replacer.setEvictable(new FrameId(2), true);
        replacer.recordAccess(new FrameId(3), new PageId(containerId, 3));
        replacer.setEvictable(new FrameId(3), true);
        assertThat(replacer.size()).isEqualTo(3);

        assertThat(replacer.evict()).isEqualTo(new FrameId(1));
        assertThat(replacer.evict()).isEqualTo(new FrameId(2));
        assertThat(replacer.evict()).isEqualTo(new FrameId(3));
        assertThat(replacer.size()).isEqualTo(0);

        replacer.recordAccess(new FrameId(3), new PageId(containerId, 4));
        replacer.setEvictable(new FrameId(3), true);

        replacer.recordAccess(new FrameId(2), new PageId(containerId, 1));
        replacer.setEvictable(new FrameId(2), true);
        assertThat(replacer.size()).isEqualTo(2);

        replacer.recordAccess(new FrameId(1), new PageId(containerId, 3));
        replacer.setEvictable(new FrameId(1), true);

        assertThat(replacer.evict()).isEqualTo(new FrameId(3));
        assertThat(replacer.evict()).isEqualTo(new FrameId(2));
        assertThat(replacer.evict()).isEqualTo(new FrameId(1));

        replacer.recordAccess(new FrameId(1), new PageId(containerId, 1));
        replacer.setEvictable(new FrameId(1), true);

        replacer.recordAccess(new FrameId(2), new PageId(containerId, 4));
        replacer.setEvictable(new FrameId(2), true);

        replacer.recordAccess(new FrameId(3), new PageId(containerId, 5));
        replacer.setEvictable(new FrameId(3), true);
        assertThat(replacer.evict()).isEqualTo(new FrameId(1));

        replacer.recordAccess(new FrameId(1), new PageId(containerId, 6));
        replacer.setEvictable(new FrameId(1), true);
        assertThat(replacer.evict()).isEqualTo(new FrameId(2));

        replacer.recordAccess(new FrameId(2), new PageId(containerId, 7));
        replacer.setEvictable(new FrameId(2), true);
        assertThat(replacer.evict()).isEqualTo(new FrameId(3));

        replacer.recordAccess(new FrameId(3), new PageId(containerId, 5));
        replacer.setEvictable(new FrameId(3), true);

        assertThat(replacer.evict()).isEqualTo(new FrameId(3));

        replacer.recordAccess(new FrameId(3), new PageId(containerId, 2));
        replacer.setEvictable(new FrameId(3), true);

        assertThat(replacer.evict()).isEqualTo(new FrameId(1));

        replacer.recordAccess(new FrameId(1), new PageId(containerId, 3));
        replacer.setEvictable(new FrameId(1), true);

        assertThat(replacer.evict()).isEqualTo(new FrameId(2));
        assertThat(replacer.evict()).isEqualTo(new FrameId(3));
        assertThat(replacer.evict()).isEqualTo(new FrameId(1));
    }

    private static ContainerId containerId(long value) {
        return ContainerId.fromUUID(new UUID(0, value));
    }
}
