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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests when {@link GroupOutputMemory.PinnedCacheMemory} lets groups go. */
class PinnedCacheMemoryTest {

    private static final RowType ROW_TYPE = RowType.of(new IntType(), new IntType());

    /** Keeps no group once it may be read back, and never reads back in these tests. */
    private static GroupOutputMemory.PinnedCacheMemory memory() {
        return memory(0, Long.MAX_VALUE, false);
    }

    private static GroupOutputMemory.PinnedCacheMemory memory(
            long maxCachedRows, long maxPinnedRows, boolean readYourWrites) {
        return new GroupOutputMemory.PinnedCacheMemory(
                null,
                new RowDataSerializer(ROW_TYPE),
                HandwrittenSelectorUtil.getRowDataSelector(
                        new int[] {1}, ROW_TYPE.getChildren().toArray(new LogicalType[0])),
                maxCachedRows,
                maxPinnedRows,
                0L,
                readYourWrites,
                null);
    }

    /** Writes a group of one row. */
    private static void write(GroupOutputMemory.PinnedCacheMemory memory, int key)
            throws Exception {
        write(memory, key, 1);
    }

    private static void write(GroupOutputMemory.PinnedCacheMemory memory, int key, int rows)
            throws Exception {
        List<RowData> list = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            list.add(GenericRowData.of(key, i));
        }
        memory.written(
                GenericRowData.of(key),
                GroupOutputMemory.index(
                        list,
                        new RowDataSerializer(ROW_TYPE),
                        HandwrittenSelectorUtil.getRowDataSelector(
                                new int[] {1},
                                ROW_TYPE.getChildren().toArray(new LogicalType[0]))));
    }

    @Test
    void testAlignedCheckpointReleasesGroupsWrittenBeforeItsBarrier() throws Exception {
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
    void testUnalignedCheckpointFails() throws Exception {
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
    void testReadYourWritesKeepsOnlyRecentGroups() throws Exception {
        GroupOutputMemory.PinnedCacheMemory memory = memory(2, Long.MAX_VALUE, true);
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

    /** The cache limit counts rows: one large group pushes out several small ones. */
    @Test
    void testCacheCountsRows() throws Exception {
        GroupOutputMemory.PinnedCacheMemory memory = memory(3, Long.MAX_VALUE, true);
        write(memory, 1);
        write(memory, 2);
        write(memory, 3);
        assertThat(memory.size()).isEqualTo(3);
        write(memory, 4, 2);
        assertThat(memory.size()).isEqualTo(2);
        // larger than the whole cache: read back when needed
        write(memory, 5, 4);
        assertThat(memory.size()).isEqualTo(2);
    }

    /** A pinned group larger than the cache is dropped as soon as it may be read back. */
    @Test
    void testLargeGroupIsDroppedOnceReadable() throws Exception {
        GroupOutputMemory.PinnedCacheMemory memory = memory(2, Long.MAX_VALUE, false);
        write(memory, 1, 5);
        assertThat(memory.pinnedRows()).isEqualTo(5);
        memory.snapshotState(1, true);
        memory.notifyCheckpointComplete(1);
        assertThat(memory.size()).isZero();
        assertThat(memory.pinnedRows()).isZero();
    }

    /** Pinned rows beyond the budget make the memory full until a checkpoint releases them. */
    @Test
    void testFullUntilCheckpointReleasesRows() throws Exception {
        GroupOutputMemory.PinnedCacheMemory memory = memory(10, 3, false);
        write(memory, 1, 2);
        write(memory, 2);
        assertThat(memory.full()).isFalse();
        write(memory, 3);
        assertThat(memory.full()).isTrue();

        // rewriting a group replaces its rows
        write(memory, 3, 0);
        assertThat(memory.full()).isFalse();
        write(memory, 3, 2);
        assertThat(memory.full()).isTrue();

        memory.snapshotState(1, true);
        memory.notifyCheckpointComplete(1);
        assertThat(memory.full()).isFalse();
        assertThat(memory.pinnedRows()).isZero();
        assertThat(memory.size()).isEqualTo(3);
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
