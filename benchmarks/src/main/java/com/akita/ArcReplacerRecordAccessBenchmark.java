/*
 * Copyright (c) 2005, 2014, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

package com.akita;

import com.akita.buffer.FrameId;
import com.akita.buffer.PageId;
import com.akita.buffer.replacers.Replacer;
import com.akita.buffer.replacers.arc.ArcReplacer;
import com.akita.storage.ContainerId;
import org.openjdk.jmh.annotations.*;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
public class ArcReplacerRecordAccessBenchmark {

    static final int BUFFER_POOL_SIZE = 256 << 10; // 262,144

    Replacer replacer;
    ContainerId containerId;

    FrameId[] frameIds;
    PageId[] pageIds;

    int accessFrameId;

    @Setup(Level.Trial)
    public void setup() {
        replacer = ArcReplacer.create(BUFFER_POOL_SIZE);
        containerId = ContainerId.fromUUID(new UUID(0, 1));

        frameIds = new FrameId[BUFFER_POOL_SIZE];
        pageIds = new PageId[BUFFER_POOL_SIZE];

        for (int i = 0; i < BUFFER_POOL_SIZE; i++) {
            frameIds[i] = new FrameId(i);
            pageIds[i] = new PageId(containerId, i);

            replacer.recordAccess(frameIds[i], pageIds[i]);
            replacer.setEvictable(frameIds[i], true);
        }

        accessFrameId = BUFFER_POOL_SIZE / 2;
    }

    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Warmup(iterations = 3)
    @Measurement(iterations = 10)
    @Fork(1)
    @Benchmark
    public void recordAccessFullScan() {
        for (int i = 0; i < BUFFER_POOL_SIZE; i++) {
            replacer.recordAccess(
                    frameIds[accessFrameId],
                    pageIds[accessFrameId]
            );
            accessFrameId = (accessFrameId + 1) % BUFFER_POOL_SIZE;
        }
    }

}
