/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.join.deltajoin.refresh;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests when {@link GroupOutputMemory.PinnedCacheMemory} lets groups go. */
class PinnedCacheMemoryTest {

    private static final RowType ROW_TYPE = RowType.of(new IntType(), new IntType());

    /** Keeps no group once it may be read back, and never reads back in these tests. */
    private static GroupOutputMemory.PinnedCacheMemory memory() {
        return memory(0, false);
    }

    private static GroupOutputMemory.PinnedCacheMemory memory(
            int maxUnpinned, boolean readYourWrites) {
        return new GroupOutputMemory.PinnedCacheMemory(
                null,
                new RowDataSerializer(ROW_TYPE),
                HandwrittenSelectorUtil.getRowDataSelector(
                        new int[] {0}, ROW_TYPE.getChildren().toArray(new LogicalType[0])),
                maxUnpinned,
                0L,
                readYourWrites,
                null);
    }

    private static void write(GroupOutputMemory.PinnedCacheMemory memory, int key) {
        RowData group = GenericRowData.of(key);
        memory.written(group, Collections.emptyMap());
    }

    @Test
    void testAlignedCheckpointReleasesGroupsWrittenBeforeItsBarrier() {
        GroupOutputMemory.PinnedCacheMemory memory = memory();
        write(memory, 1);
        memory.snapshotState(1, true);
        write(memory, 2);
        memory.notifyCheckpointComplete(1);
        assertThat(memory.size()).isOne();

        memory.snapshotState(2, true);
        memory.notifyCheckpointComplete(2);
        assertThat(memory.size()).isZero();
    }

    @Test
    void testUnalignedCheckpointFails() {
        GroupOutputMemory.PinnedCacheMemory memory = memory();
        write(memory, 1);
        assertThatThrownBy(() -> memory.snapshotState(1, false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Checkpoint 1 is unaligned, so its completion does not prove that the "
                                + "sink received the rows emitted before it. A delta join that "
                                + "reads its output back after checkpoints needs aligned "
                                + "checkpoints.");
    }

    /**
     * With read-your-writes, a group can be read back as soon as it was written: the memory is a
     * cache of the most recently written groups, whatever the checkpoints do.
     */
    @Test
    void testReadYourWritesKeepsOnlyRecentGroups() {
        GroupOutputMemory.PinnedCacheMemory memory = memory(2, true);
        write(memory, 1);
        write(memory, 2);
        write(memory, 3);
        assertThat(memory.size()).isEqualTo(2);

        memory.snapshotState(1, false);
        write(memory, 4);
        assertThat(memory.size()).isEqualTo(2);
        memory.notifyCheckpointComplete(1);
        assertThat(memory.size()).isEqualTo(2);
    }

    @Test
    void testRowsOfAGroupWithTheSameOutputKeyFail() {
        assertThatThrownBy(
                        () ->
                                GroupOutputMemory.index(
                                        Arrays.asList(
                                                GenericRowData.of(1, 1), GenericRowData.of(1, 2)),
                                        new RowDataSerializer(ROW_TYPE),
                                        HandwrittenSelectorUtil.getRowDataSelector(
                                                new int[] {0},
                                                ROW_TYPE.getChildren()
                                                        .toArray(new LogicalType[0]))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Two rows of one group have the same output key (1). A constant that "
                                + "replaces NULL in a key column of the output must not be a "
                                + "value of that column.");
    }
}
